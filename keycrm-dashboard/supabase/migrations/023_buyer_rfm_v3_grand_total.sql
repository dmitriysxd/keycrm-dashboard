-- Перебудова buyer_rfm з урахуванням знижок на замовлення.
--
-- ПРОБЛЕМА: попередня версія сумувала revenue по позиціях замовлення
-- (quantity × price), що ігнорує знижки KeyCRM, які накладаються на
-- весь заказ. Тепер беремо grand_total (зберігається в sales.order_grand_total
-- однаковим для всіх рядків заказу) — це фактична сума, яку заплатив клієнт.
--
-- Для історичних замовлень, де ingest ще не записав grand_total, ставимо
-- fallback: суму line-revenue (так було раніше). При наступному ре-інгесті
-- через backfill.js ці значення оновляться на справжній grand_total.

------------------------------------------------------------
-- 1. Backfill order_grand_total для історичних рядків.
------------------------------------------------------------
UPDATE sales s
SET order_grand_total = sub.tot
FROM (
  SELECT order_id, SUM(revenue)::numeric(14,2) AS tot
  FROM sales
  GROUP BY order_id
) sub
WHERE s.order_id = sub.order_id
  AND s.order_grand_total IS NULL;

CREATE INDEX IF NOT EXISTS sales_order_id_idx ON sales(order_id);

------------------------------------------------------------
-- 2. Перебудова buyer_rfm з grand_total.
------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS buyer_rfm;

CREATE MATERIALIZED VIEW buyer_rfm AS
WITH excl AS (
  SELECT s.buyer_id, s.order_id, s.offer_id, s.revenue, s.order_grand_total,
         s.ordered_at::date AS order_date
  FROM sales s
  WHERE s.buyer_id IS NOT NULL
    AND COALESCE(s.order_status, '') NOT IN (
      'cancelled','rejected','canceled',
      'Повернули','Відмовились','incorrect_data','underbid'
    )
    AND COALESCE(s.order_status, '') NOT LIKE 'Об%єднання замовлень'
),
orders AS (
  -- Одна строка на замовлення. order_revenue = grand_total (зі знижкою),
  -- fallback на суму line-revenue, якщо grand_total ще не записано.
  SELECT
    buyer_id, order_id,
    MIN(order_date) AS order_date,
    COALESCE(MAX(order_grand_total), SUM(revenue))::numeric(14,2) AS order_revenue
  FROM excl
  GROUP BY buyer_id, order_id
),
orders_ranked AS (
  SELECT
    buyer_id, order_id, order_date, order_revenue,
    ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY order_date, order_id) AS rn_asc,
    COUNT(*)   OVER (PARTITION BY buyer_id) AS total_orders,
    LAG(order_date) OVER (PARTITION BY buyer_id ORDER BY order_date, order_id) AS prev_order_date
  FROM orders
),
intervals AS (
  SELECT
    buyer_id,
    AVG((order_date - prev_order_date)::numeric)::numeric(10,2) AS avg_interval_days,
    AVG(CASE WHEN (total_orders - rn_asc + 1) <= 3
             THEN (order_date - prev_order_date)::numeric END)::numeric(10,2) AS recent_interval_days,
    AVG(CASE WHEN (total_orders - rn_asc + 1) > 3
             THEN (order_date - prev_order_date)::numeric END)::numeric(10,2) AS prior_interval_days
  FROM orders_ranked
  WHERE prev_order_date IS NOT NULL
  GROUP BY buyer_id
),
base AS (
  SELECT
    o.buyer_id,
    MIN(o.order_date)                                              AS first_order_date,
    MAX(o.order_date)                                              AS last_order_date,
    COUNT(*)::int                                                  AS frequency,
    COALESCE(SUM(o.order_revenue), 0)::numeric(14,2)               AS monetary,
    (CURRENT_DATE - MAX(o.order_date))                             AS recency_days,
    AVG(o.order_revenue)::numeric(14,2)                            AS aov,
    SUM(CASE WHEN o.order_date >= CURRENT_DATE - 90 THEN 1 ELSE 0 END)::int AS freq_last_90d,
    SUM(CASE WHEN o.order_date >= CURRENT_DATE - 180
                  AND o.order_date <  CURRENT_DATE - 90 THEN 1 ELSE 0 END)::int AS freq_prior_90d,
    AVG(CASE WHEN o.order_date >= CURRENT_DATE - 90
             THEN o.order_revenue END)::numeric(14,2)              AS aov_last_90d
  FROM orders o
  GROUP BY o.buyer_id
),
cats AS (
  SELECT
    e.buyer_id,
    COUNT(DISTINCT k.category_id) FILTER (WHERE k.category_id IS NOT NULL)::int AS categories_lifetime,
    COUNT(DISTINCT CASE WHEN e.order_date >= CURRENT_DATE - 90 THEN k.category_id END)::int AS categories_90d
  FROM excl e
  LEFT JOIN skus k ON k.offer_id = e.offer_id
  GROUP BY e.buyer_id
)
SELECT
  b.buyer_id,
  b.first_order_date,
  b.last_order_date,
  b.recency_days,
  b.frequency,
  b.monetary,
  b.aov,
  b.aov_last_90d,
  b.freq_last_90d,
  b.freq_prior_90d,
  i.avg_interval_days,
  i.recent_interval_days,
  i.prior_interval_days,
  COALESCE(c.categories_lifetime, 0) AS categories_lifetime,
  COALESCE(c.categories_90d, 0)      AS categories_90d,
  (6 - NTILE(5) OVER (ORDER BY b.recency_days))::int AS r_score,
  NTILE(5) OVER (ORDER BY b.frequency)::int          AS f_score,
  NTILE(5) OVER (ORDER BY b.monetary)::int           AS m_score
FROM base b
LEFT JOIN intervals i ON i.buyer_id = b.buyer_id
LEFT JOIN cats      c ON c.buyer_id = b.buyer_id;

CREATE UNIQUE INDEX IF NOT EXISTS buyer_rfm_pk          ON buyer_rfm(buyer_id);
CREATE INDEX        IF NOT EXISTS buyer_rfm_monetary_idx ON buyer_rfm(monetary DESC);
CREATE INDEX        IF NOT EXISTS buyer_rfm_recency_idx  ON buyer_rfm(recency_days);
