-- Migration 045: додати статус "Перенос (дублікат)" до виключених.
--
-- ПРИЧИНА (юзер, 2026-07-06): наприкінці місяця частину незабраних посилок
-- юзер КОПІЮЄ в новий місяць (нове замовлення, новий order_id, нова дата),
-- а старе ВИДАЛЯЄ — щоб швидше закрити місяць. Але наш ingest видалення
-- замовлень не обробляє: старий заказ лишається в sales назавжди → та сама
-- посилка рахується ДВІЧІ (старий місяць + новий) → задвоєння обороту/LTV.
--
-- РІШЕННЯ: юзер завів у KeyCRM статус "Перенос (дублікат)", який ставиться
-- на старий (продубльований) заказ. Додаємо його до списку виключених
-- статусів — так само як cancelled/Відмовились. Тоді задвоєна копія не
-- потрапляє в жодну метрику.
--
-- Matviews у Postgres не мають CREATE OR REPLACE, тож DROP + CREATE. Тіла
-- sku_metrics (з міграції 037) та buyer_rfm (з 023) відтворені 1-в-1, змінено
-- ЛИШЕ список статусів NOT IN (додано 'Перенос (дублікат)'). JS-фільтри
-- (EXCLUDED_STATUSES) оновлені окремо в api/*.js.

------------------------------------------------------------
-- 1. sku_metrics (тіло з 037 + новий статус)
------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS sku_metrics;

CREATE MATERIALIZED VIEW sku_metrics AS
WITH latest_stock AS (
  SELECT DISTINCT ON (offer_id) offer_id, snapshot_date AS stock_date, quantity AS current_stock
  FROM stock_snapshots ORDER BY offer_id, snapshot_date DESC
),
peak_stock_30d AS (
  SELECT offer_id, MAX(quantity)::numeric AS qty FROM stock_snapshots
  WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY offer_id
),
selling_window AS (
  SELECT offer_id,
    MIN(snapshot_date) FILTER (WHERE quantity > 0) AS first_positive,
    MAX(snapshot_date) FILTER (WHERE quantity > 0) AS last_positive,
    COUNT(DISTINCT snapshot_date) FILTER (WHERE quantity > 0) AS days_in_stock
  FROM stock_snapshots GROUP BY offer_id
),
sales_dates AS (
  SELECT product_id, MIN(ordered_at)::date AS first_sold, MAX(ordered_at)::date AS last_sold FROM sales
  WHERE COALESCE(order_status,'') NOT IN ('cancelled','rejected','canceled','Повернули','Відмовились','incorrect_data','underbid','Перенос (дублікат)')
    AND COALESCE(order_status,'') NOT LIKE 'Об%єднання замовлень' AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_7 AS (
  SELECT product_id, SUM(quantity) AS qty FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '7 days'
    AND COALESCE(order_status,'') NOT IN ('cancelled','rejected','canceled','Повернули','Відмовились','incorrect_data','underbid','Перенос (дублікат)')
    AND COALESCE(order_status,'') NOT LIKE 'Об%єднання замовлень' AND product_id IS NOT NULL GROUP BY product_id
),
sales_30 AS (
  SELECT product_id, SUM(quantity) AS qty, SUM(revenue) AS revenue FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '30 days'
    AND COALESCE(order_status,'') NOT IN ('cancelled','rejected','canceled','Повернули','Відмовились','incorrect_data','underbid','Перенос (дублікат)')
    AND COALESCE(order_status,'') NOT LIKE 'Об%єднання замовлень' AND product_id IS NOT NULL GROUP BY product_id
),
sales_90 AS (
  SELECT product_id, SUM(quantity) AS qty FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '90 days'
    AND COALESCE(order_status,'') NOT IN ('cancelled','rejected','canceled','Повернули','Відмовились','incorrect_data','underbid','Перенос (дублікат)')
    AND COALESCE(order_status,'') NOT LIKE 'Об%єднання замовлень' AND product_id IS NOT NULL GROUP BY product_id
),
sales_total AS (
  SELECT product_id, SUM(quantity) AS qty_all, MAX(ordered_at) AS last_sold_at FROM sales
  WHERE COALESCE(order_status,'') NOT IN ('cancelled','rejected','canceled','Повернули','Відмовились','incorrect_data','underbid','Перенос (дублікат)')
    AND COALESCE(order_status,'') NOT LIKE 'Об%єднання замовлень' AND product_id IS NOT NULL GROUP BY product_id
),
buyers_per_product AS (
  SELECT product_id, COUNT(DISTINCT buyer_id)::int AS buyers FROM sales
  WHERE buyer_id IS NOT NULL
    AND COALESCE(order_status,'') NOT IN ('cancelled','rejected','canceled','Повернули','Відмовились','incorrect_data','underbid','Перенос (дублікат)')
    AND COALESCE(order_status,'') NOT LIKE 'Об%єднання замовлень' AND product_id IS NOT NULL GROUP BY product_id
),
distinct_sale_dates AS (
  SELECT DISTINCT product_id, ordered_at::date AS day FROM sales
  WHERE COALESCE(order_status,'') NOT IN ('cancelled','rejected','canceled','Повернули','Відмовились','incorrect_data','underbid','Перенос (дублікат)')
    AND COALESCE(order_status,'') NOT LIKE 'Об%єднання замовлень' AND product_id IS NOT NULL
),
creation_anchors AS (
  SELECT s.product_id, s.keycrm_created_at::date AS day FROM skus s
  JOIN sales_dates sd ON sd.product_id = s.product_id
  WHERE s.keycrm_created_at IS NOT NULL AND s.keycrm_created_at::date < sd.first_sold
    AND sd.first_sold - s.keycrm_created_at::date <= 7
),
all_active_days AS (
  SELECT product_id, day FROM distinct_sale_dates
  UNION SELECT product_id, day FROM creation_anchors
),
active_lagged AS (
  SELECT product_id, day, LAG(day) OVER (PARTITION BY product_id ORDER BY day) AS prev_day FROM all_active_days
),
cycle_marks AS (
  SELECT product_id, day,
    CASE WHEN prev_day IS NULL THEN 1 WHEN (day - prev_day) > 60 THEN 1 ELSE 0 END AS new_cycle FROM active_lagged
),
cycle_assigned AS (
  SELECT product_id, day, SUM(new_cycle) OVER (PARTITION BY product_id ORDER BY day) AS cycle_id FROM cycle_marks
),
cycle_summary AS (
  SELECT product_id, cycle_id, MIN(day) AS cycle_start, (MAX(day) - MIN(day) + 1) AS cycle_days
  FROM cycle_assigned GROUP BY product_id, cycle_id
),
last_cycle_starts AS (
  SELECT product_id, MAX(cycle_start) AS cycle_start FROM cycle_summary GROUP BY product_id
),
sales_active_days AS (
  SELECT product_id, SUM(cycle_days)::int AS days FROM cycle_summary GROUP BY product_id
),
base AS (
  SELECT
    s.offer_id, s.product_id, s.sku, s.name, s.category_id, s.category_name,
    s.price, s.cost, s.supplier_lot, s.notes, s.is_active, s.first_seen_at,
    s.first_stock_at, s.keycrm_created_at, s.last_restock_at, s.manual_status,
    COALESCE(ls.current_stock, 0)::numeric AS current_stock,
    ls.stock_date,
    COALESCE(p30.qty, 0)::numeric AS peak_stock_30d,
    sw.first_positive, sw.last_positive, sw.days_in_stock,
    sd.first_sold, sd.last_sold,
    sad.days AS sales_active_days,
    lcs.cycle_start AS last_cycle_start,
    COALESCE(bpp.buyers, 0)::int AS buyers_count,
    COALESCE(s7.qty,  0)::numeric AS sold_7d,
    COALESCE(s30.qty, 0)::numeric AS sold_30d,
    COALESCE(s90.qty, 0)::numeric AS sold_90d,
    COALESCE(s30.revenue, 0)::numeric AS revenue_30d,
    COALESCE(st.qty_all, 0)::numeric AS sold_total,
    st.last_sold_at,
    CASE
      WHEN COALESCE(p30.qty, 0) <= 0 AND COALESCE(s30.qty, 0) + COALESCE(ls.current_stock, 0) = 0 THEN NULL
      ELSE COALESCE(s30.qty, 0)::numeric
           / NULLIF(GREATEST(COALESCE(p30.qty, 0), COALESCE(s30.qty, 0) + COALESCE(ls.current_stock, 0)), 0)::numeric
    END AS st30_calc,
    LEAST(COALESCE(sw.first_positive, sd.first_sold), COALESCE(sd.first_sold, sw.first_positive)) AS window_start,
    CASE WHEN COALESCE(ls.current_stock, 0) > 0 THEN CURRENT_DATE
         ELSE GREATEST(COALESCE(sw.last_positive, sd.last_sold), COALESCE(sd.last_sold, sw.last_positive))
    END AS window_end,
    GREATEST(COALESCE(sw.days_in_stock, 0), COALESCE(sad.days, 0), 1) AS vlife_days
  FROM skus s
  LEFT JOIN latest_stock     ls  ON ls.offer_id    = s.offer_id
  LEFT JOIN peak_stock_30d   p30 ON p30.offer_id   = s.offer_id
  LEFT JOIN selling_window   sw  ON sw.offer_id    = s.offer_id
  LEFT JOIN sales_dates      sd  ON sd.product_id  = s.product_id
  LEFT JOIN sales_active_days sad ON sad.product_id = s.product_id
  LEFT JOIN last_cycle_starts  lcs ON lcs.product_id = s.product_id
  LEFT JOIN buyers_per_product bpp ON bpp.product_id = s.product_id
  LEFT JOIN sales_7          s7  ON s7.product_id  = s.product_id
  LEFT JOIN sales_30         s30 ON s30.product_id = s.product_id
  LEFT JOIN sales_90         s90 ON s90.product_id = s.product_id
  LEFT JOIN sales_total      st  ON st.product_id  = s.product_id
),
with_age AS (
  SELECT *,
    CASE
      WHEN current_stock <= 0 THEN NULL
      WHEN last_restock_at IS NOT NULL THEN (CURRENT_DATE - last_restock_at)::int
      WHEN last_cycle_start IS NOT NULL THEN (CURRENT_DATE - last_cycle_start)::int
      WHEN first_stock_at IS NOT NULL THEN EXTRACT(DAY FROM (NOW() - first_stock_at))::int
      WHEN keycrm_created_at IS NOT NULL THEN EXTRACT(DAY FROM (NOW() - keycrm_created_at))::int
      ELSE NULL
    END AS age_days,
    CASE
      WHEN keycrm_created_at IS NOT NULL THEN EXTRACT(DAY FROM (NOW() - keycrm_created_at))::int
      WHEN first_stock_at IS NOT NULL THEN EXTRACT(DAY FROM (NOW() - first_stock_at))::int
      WHEN first_sold IS NOT NULL THEN (CURRENT_DATE - first_sold)::int
      ELSE NULL
    END AS lifetime_days,
    public.current_season_multiplier(category_id) AS season_mult,
    ROUND(sold_7d / 7.0, 3) AS v7_raw,
    ROUND(sold_30d / 30.0, 3) AS v30_raw,
    CASE WHEN sold_total > 0 THEN ROUND(sold_total / vlife_days::numeric, 3) ELSE NULL END AS lifetime_v_raw
  FROM base
),
ranked AS (
  SELECT *,
    CASE WHEN is_active AND current_stock > 0 AND sold_30d > 0 THEN
      NTILE(5) OVER (
        PARTITION BY category_id
        ORDER BY CASE WHEN is_active AND current_stock > 0 AND sold_30d > 0 THEN sold_30d ELSE NULL END
                 ASC NULLS FIRST
      )
    END AS category_quintile,
    CASE WHEN revenue_30d > 0 THEN
      NTILE(20) OVER (ORDER BY revenue_30d DESC NULLS LAST)
    END AS revenue_rank,
    CASE WHEN lifetime_v_raw IS NOT NULL AND lifetime_v_raw > 0 AND sold_30d > 0
      THEN ROUND(v30_raw / lifetime_v_raw, 2)
    END AS relative_performance
  FROM with_age
)
SELECT
  offer_id, product_id, sku, name, category_id, category_name, price, cost,
  supplier_lot, notes, is_active, first_seen_at, first_stock_at,
  keycrm_created_at, last_restock_at, manual_status,
  current_stock, stock_date, peak_stock_30d,
  window_start, window_end, vlife_days, last_cycle_start, buyers_count,
  sold_7d, sold_30d, sold_90d, revenue_30d, sold_total, last_sold_at,
  v7_raw AS velocity_7d,
  CASE WHEN lifetime_days IS NULL OR lifetime_days >= 30 THEN v30_raw ELSE NULL END AS velocity_30d,
  CASE WHEN lifetime_days IS NULL OR lifetime_days >= 90
    THEN ROUND(sold_90d / 90.0, 3) ELSE NULL END AS velocity_90d,
  lifetime_v_raw AS lifetime_velocity,
  CASE WHEN st30_calc IS NULL THEN NULL ELSE ROUND(st30_calc, 4) END AS sell_through_30d,
  CASE WHEN sold_7d = 0 THEN NULL
       ELSE ROUND(current_stock / NULLIF(sold_7d / 7.0, 0), 1) END AS days_of_supply,
  age_days,
  lifetime_days,
  season_mult AS current_season_multiplier,
  category_quintile,
  revenue_rank,
  relative_performance,
  -- НОВЕ (037): "списати" перед "dead" для товарів які не продавались 60+ днів.
  COALESCE(manual_status, CASE
    WHEN keycrm_created_at IS NOT NULL AND keycrm_created_at >= NOW() - INTERVAL '30 days' THEN 'new'
    WHEN current_stock <= 0 THEN 'archive'
    WHEN sold_30d = 0 AND (last_sold_at IS NULL OR last_sold_at < NOW() - INTERVAL '60 days') THEN 'списати'
    WHEN sold_30d = 0 THEN 'dead'
    WHEN sold_30d >= 30 * season_mult THEN 'hit'
    WHEN sold_30d >= 15 * season_mult THEN 'good'
    WHEN sold_30d >= 6  * season_mult THEN 'slow'
    ELSE 'weak'
  END) AS status,
  CASE
    WHEN current_stock <= 0 THEN NULL
    WHEN keycrm_created_at IS NOT NULL AND keycrm_created_at >= NOW() - INTERVAL '30 days' THEN NULL
    WHEN sold_30d = 0 THEN NULL
    WHEN revenue_rank IS NOT NULL AND revenue_rank <= 4 AND v7_raw > 0 AND v30_raw > 0 AND v7_raw / v30_raw > 1.2
      THEN 'star'
    WHEN revenue_rank IS NOT NULL AND revenue_rank <= 4
      THEN 'cash_cow'
    WHEN v7_raw > 0 AND v30_raw > 0 AND v7_raw / v30_raw > 1.2 THEN 'question'
    WHEN v7_raw > 0 AND v30_raw > 0 AND v7_raw / v30_raw < 0.7 THEN 'dog'
    ELSE NULL
  END AS bcg_role,
  ARRAY_REMOVE(ARRAY[
    CASE WHEN v7_raw > 0 AND v30_raw > 0 AND v7_raw / v30_raw > 1.5 THEN 'trend_up' END,
    CASE WHEN v7_raw > 0 AND v30_raw > 0 AND v7_raw / v30_raw < 0.5 THEN 'trend_down' END,
    CASE WHEN sold_7d > 0
              AND ROUND(current_stock / NULLIF(sold_7d / 7.0, 0), 1) < 7
              AND current_stock > 0 THEN 'stockout_risk' END,
    CASE WHEN sold_7d > 0
              AND ROUND(current_stock / NULLIF(sold_7d / 7.0, 0), 1) > 180 THEN 'overstocked' END,
    CASE WHEN revenue_rank IS NOT NULL AND revenue_rank <= 4 THEN 'hero' END,
    CASE WHEN sold_7d > 0 AND last_sold_at IS NOT NULL
              AND last_sold_at < NOW() - INTERVAL '30 days'
              AND last_sold_at >= NOW() - INTERVAL '7 days' THEN 'comeback' END,
    CASE WHEN relative_performance IS NOT NULL AND relative_performance >= 1.2 THEN 'above_baseline' END,
    CASE WHEN relative_performance IS NOT NULL AND relative_performance < 0.8
              AND relative_performance > 0 THEN 'below_baseline' END
  ], NULL) AS tags,
  CASE WHEN sold_total = 0 THEN 'dead' ELSE CASE
    WHEN sold_total / vlife_days::numeric >= 0.5  THEN 'hit'
    WHEN sold_total / vlife_days::numeric >= 0.2  THEN 'good'
    WHEN sold_total / vlife_days::numeric >= 0.05 THEN 'slow'
    ELSE 'dead' END
  END AS archive_tier
FROM ranked;

CREATE UNIQUE INDEX IF NOT EXISTS sku_metrics_offer_idx    ON sku_metrics(offer_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_status_idx   ON sku_metrics(status);
CREATE INDEX        IF NOT EXISTS sku_metrics_category_idx ON sku_metrics(category_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_velocity_idx ON sku_metrics(velocity_30d DESC);
CREATE INDEX        IF NOT EXISTS sku_metrics_lifetime_idx ON sku_metrics(lifetime_velocity DESC);
CREATE INDEX        IF NOT EXISTS sku_metrics_revenue_idx  ON sku_metrics(revenue_30d DESC NULLS LAST);
CREATE INDEX        IF NOT EXISTS sku_metrics_bcg_idx      ON sku_metrics(bcg_role) WHERE bcg_role IS NOT NULL;

REFRESH MATERIALIZED VIEW sku_metrics;

------------------------------------------------------------
-- 2. buyer_rfm (тіло з 023 + новий статус)
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
      'Повернули','Відмовились','incorrect_data','underbid','Перенос (дублікат)'
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

REFRESH MATERIALIZED VIEW buyer_rfm;
