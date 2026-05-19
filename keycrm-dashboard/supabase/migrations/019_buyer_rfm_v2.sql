-- Расширение RFM-аналитики:
--   - средний интервал между заказами (цикл закупки)
--   - "недавний" интервал vs "ранний" → velocity-trend (ускоряется/замедляется)
--   - частота за последние 90 дней vs предыдущие 90
--   - AOV (средний чек) исторический и за 90 дней
--   - количество категорий по всему времени и за 90 дней
--
-- Затем в API/UI считаются:
--   - overdue (recency > 1.5 × avg_interval) — выделяем строку
--   - churn_pct — суммарная вероятность оттока (взвешенная)
--   - velocity_trend — accelerating / stable / decelerating
--   - ltv = historical monetary (с возможностью расширения позже)

DROP MATERIALIZED VIEW IF EXISTS buyer_rfm;

CREATE MATERIALIZED VIEW buyer_rfm AS
WITH excl AS (
  -- Чистка статусов один раз, дальше работаем уже с очищенным sales.
  SELECT s.buyer_id, s.order_id, s.offer_id, s.revenue, s.ordered_at::date AS order_date
  FROM sales s
  WHERE s.buyer_id IS NOT NULL
    AND COALESCE(s.order_status, '') NOT IN (
      'cancelled','rejected','canceled',
      'Повернули','Відмовились','incorrect_data','underbid'
    )
    AND COALESCE(s.order_status, '') NOT LIKE 'Об%єднання замовлень'
),
orders AS (
  -- Один заказ = одна строка: дата + суммарная выручка.
  SELECT buyer_id, order_id, MIN(order_date) AS order_date, SUM(revenue)::numeric AS order_revenue
  FROM excl
  GROUP BY buyer_id, order_id
),
orders_ranked AS (
  SELECT
    buyer_id, order_id, order_date, order_revenue,
    ROW_NUMBER() OVER (PARTITION BY buyer_id ORDER BY order_date, order_id) AS rn_asc,
    COUNT(*)   OVER (PARTITION BY buyer_id)                                  AS total_orders,
    LAG(order_date) OVER (PARTITION BY buyer_id ORDER BY order_date, order_id) AS prev_order_date
  FROM orders
),
intervals AS (
  -- Средние интервалы: общий, последние ≤3, предыдущие.
  -- "Последние" — заказы с rn_desc <= 3 (rn_desc = total_orders - rn_asc + 1).
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
  -- Уникальные категории за всё время и за последние 90 дней.
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
  -- меньший recency_days → выше скор (5 — недавний)
  (6 - NTILE(5) OVER (ORDER BY b.recency_days))::int AS r_score,
  NTILE(5) OVER (ORDER BY b.frequency)::int          AS f_score,
  NTILE(5) OVER (ORDER BY b.monetary)::int           AS m_score
FROM base b
LEFT JOIN intervals i ON i.buyer_id = b.buyer_id
LEFT JOIN cats      c ON c.buyer_id = b.buyer_id;

CREATE UNIQUE INDEX IF NOT EXISTS buyer_rfm_pk ON buyer_rfm(buyer_id);
CREATE INDEX IF NOT EXISTS buyer_rfm_monetary_idx ON buyer_rfm(monetary DESC);
CREATE INDEX IF NOT EXISTS buyer_rfm_recency_idx ON buyer_rfm(recency_days);
