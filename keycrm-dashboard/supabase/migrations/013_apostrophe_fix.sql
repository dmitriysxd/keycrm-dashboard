-- v10 of sku_metrics: handle typographic apostrophe in status values.
--
-- Bug discovered after migration 012 was applied: SKU 03285 showed
-- sold_30d = 216 instead of expected 214. The diff = 2 units in status
-- "Об'єднання замовлень" which the migration was supposed to exclude.
--
-- Root cause: KeyCRM stores the status with a TYPOGRAPHIC apostrophe
-- (U+2019, 3 bytes in UTF-8: E2 80 99). My filter used the ASCII
-- apostrophe (U+0027, 1 byte: 27). Postgres compares strings byte-by-
-- byte, so 'Об''єднання замовлень' (ASCII) never equals 'Об'єднання
-- замовлень' (typographic) and the row passes the NOT IN check.
--
-- Fix: use LIKE 'Об%єднання замовлень' for this status — % matches any
-- single character at the apostrophe position, so all Unicode variants
-- of apostrophe are caught. Other excluded statuses don't have
-- apostrophes so plain = comparison still works.
--
-- Apply via Supabase SQL Editor.

DROP MATERIALIZED VIEW IF EXISTS sku_metrics;

CREATE MATERIALIZED VIEW sku_metrics AS
WITH latest_stock AS (
  SELECT DISTINCT ON (offer_id)
    offer_id,
    snapshot_date AS stock_date,
    quantity      AS current_stock
  FROM stock_snapshots
  ORDER BY offer_id, snapshot_date DESC
),
peak_stock_30d AS (
  SELECT offer_id, MAX(quantity)::numeric AS qty
  FROM stock_snapshots
  WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY offer_id
),
selling_window AS (
  SELECT
    offer_id,
    MIN(snapshot_date) FILTER (WHERE quantity > 0) AS first_positive,
    MAX(snapshot_date) FILTER (WHERE quantity > 0) AS last_positive,
    COUNT(DISTINCT snapshot_date) FILTER (WHERE quantity > 0) AS days_in_stock
  FROM stock_snapshots
  GROUP BY offer_id
),
sales_dates AS (
  SELECT
    product_id,
    MIN(ordered_at)::date AS first_sold,
    MAX(ordered_at)::date AS last_sold
  FROM sales
  WHERE COALESCE(order_status, '') NOT IN (
          'cancelled', 'rejected', 'canceled',
          'Повернули', 'Відмовились',
          'incorrect_data', 'underbid'
        )
    AND COALESCE(order_status, '') NOT LIKE 'Об%єднання замовлень'
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_7 AS (
  SELECT product_id, SUM(quantity) AS qty
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '7 days'
    AND COALESCE(order_status, '') NOT IN (
          'cancelled', 'rejected', 'canceled',
          'Повернули', 'Відмовились',
          'incorrect_data', 'underbid'
        )
    AND COALESCE(order_status, '') NOT LIKE 'Об%єднання замовлень'
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_30 AS (
  SELECT product_id,
         SUM(quantity) AS qty,
         SUM(revenue)  AS revenue
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '30 days'
    AND COALESCE(order_status, '') NOT IN (
          'cancelled', 'rejected', 'canceled',
          'Повернули', 'Відмовились',
          'incorrect_data', 'underbid'
        )
    AND COALESCE(order_status, '') NOT LIKE 'Об%єднання замовлень'
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_90 AS (
  SELECT product_id, SUM(quantity) AS qty
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '90 days'
    AND COALESCE(order_status, '') NOT IN (
          'cancelled', 'rejected', 'canceled',
          'Повернули', 'Відмовились',
          'incorrect_data', 'underbid'
        )
    AND COALESCE(order_status, '') NOT LIKE 'Об%єднання замовлень'
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_total AS (
  SELECT product_id,
         SUM(quantity)   AS qty_all,
         MAX(ordered_at) AS last_sold_at
  FROM sales
  WHERE COALESCE(order_status, '') NOT IN (
          'cancelled', 'rejected', 'canceled',
          'Повернули', 'Відмовились',
          'incorrect_data', 'underbid'
        )
    AND COALESCE(order_status, '') NOT LIKE 'Об%єднання замовлень'
    AND product_id IS NOT NULL
  GROUP BY product_id
),
-- Build per-product set of distinct active dates from sales,
-- plus keycrm_created_at as synthetic floor when it's within
-- 7 days BEFORE the first real sale.
distinct_sale_dates AS (
  SELECT DISTINCT product_id, ordered_at::date AS day
  FROM sales
  WHERE COALESCE(order_status, '') NOT IN (
          'cancelled', 'rejected', 'canceled',
          'Повернули', 'Відмовились',
          'incorrect_data', 'underbid'
        )
    AND COALESCE(order_status, '') NOT LIKE 'Об%єднання замовлень'
    AND product_id IS NOT NULL
),
creation_anchors AS (
  SELECT s.product_id, s.keycrm_created_at::date AS day
  FROM skus s
  JOIN sales_dates sd ON sd.product_id = s.product_id
  WHERE s.keycrm_created_at IS NOT NULL
    AND s.keycrm_created_at::date < sd.first_sold
    AND sd.first_sold - s.keycrm_created_at::date <= 7
),
all_active_days AS (
  SELECT product_id, day FROM distinct_sale_dates
  UNION
  SELECT product_id, day FROM creation_anchors
),
active_lagged AS (
  SELECT product_id, day,
         LAG(day) OVER (PARTITION BY product_id ORDER BY day) AS prev_day
  FROM all_active_days
),
cycle_marks AS (
  SELECT product_id, day,
         CASE
           WHEN prev_day IS NULL THEN 1
           WHEN (day - prev_day) > 30 THEN 1
           ELSE 0
         END AS new_cycle
  FROM active_lagged
),
cycle_assigned AS (
  SELECT product_id, day,
         SUM(new_cycle) OVER (PARTITION BY product_id ORDER BY day) AS cycle_id
  FROM cycle_marks
),
cycle_summary AS (
  SELECT product_id, cycle_id,
         (MAX(day) - MIN(day) + 1) AS cycle_days
  FROM cycle_assigned
  GROUP BY product_id, cycle_id
),
sales_active_days AS (
  SELECT product_id, SUM(cycle_days)::int AS days
  FROM cycle_summary
  GROUP BY product_id
),
base AS (
  SELECT
    s.offer_id,
    s.product_id,
    s.sku,
    s.name,
    s.category_id,
    s.category_name,
    s.price,
    s.cost,
    s.supplier_lot,
    s.notes,
    s.is_active,
    s.first_seen_at,
    s.first_stock_at,
    s.keycrm_created_at,
    s.last_restock_at,
    s.manual_status,
    COALESCE(ls.current_stock, 0)::numeric AS current_stock,
    ls.stock_date,
    COALESCE(p30.qty, 0)::numeric AS peak_stock_30d,
    sw.first_positive,
    sw.last_positive,
    sw.days_in_stock,
    sd.first_sold,
    sd.last_sold,
    sad.days AS sales_active_days,
    COALESCE(s7.qty,  0)::numeric AS sold_7d,
    COALESCE(s30.qty, 0)::numeric AS sold_30d,
    COALESCE(s90.qty, 0)::numeric AS sold_90d,
    COALESCE(s30.revenue, 0)::numeric AS revenue_30d,
    COALESCE(st.qty_all, 0)::numeric AS sold_total,
    st.last_sold_at,
    CASE
      WHEN COALESCE(p30.qty, 0) <= 0 AND COALESCE(s30.qty, 0) + COALESCE(ls.current_stock, 0) = 0 THEN NULL
      ELSE COALESCE(s30.qty, 0)::numeric
           / NULLIF(GREATEST(
               COALESCE(p30.qty, 0),
               COALESCE(s30.qty, 0) + COALESCE(ls.current_stock, 0)
             ), 0)::numeric
    END AS st30_calc,
    LEAST(
      COALESCE(sw.first_positive, sd.first_sold),
      COALESCE(sd.first_sold,     sw.first_positive)
    ) AS window_start,
    CASE
      WHEN COALESCE(ls.current_stock, 0) > 0 THEN CURRENT_DATE
      ELSE GREATEST(
        COALESCE(sw.last_positive, sd.last_sold),
        COALESCE(sd.last_sold,     sw.last_positive)
      )
    END AS window_end,
    -- Vlife denominator: GREATEST of (snapshot days with stock, sales-based
    -- multi-cycle active days, 1). Uses whichever signal is stronger.
    GREATEST(
      COALESCE(sw.days_in_stock, 0),
      COALESCE(sad.days, 0),
      1
    ) AS vlife_days
  FROM skus s
  LEFT JOIN latest_stock     ls  ON ls.offer_id    = s.offer_id
  LEFT JOIN peak_stock_30d   p30 ON p30.offer_id   = s.offer_id
  LEFT JOIN selling_window   sw  ON sw.offer_id    = s.offer_id
  LEFT JOIN sales_dates      sd  ON sd.product_id  = s.product_id
  LEFT JOIN sales_active_days sad ON sad.product_id = s.product_id
  LEFT JOIN sales_7          s7  ON s7.product_id  = s.product_id
  LEFT JOIN sales_30         s30 ON s30.product_id = s.product_id
  LEFT JOIN sales_90         s90 ON s90.product_id = s.product_id
  LEFT JOIN sales_total      st  ON st.product_id  = s.product_id
)
SELECT
  offer_id,
  product_id,
  sku,
  name,
  category_id,
  category_name,
  price,
  cost,
  supplier_lot,
  notes,
  is_active,
  first_seen_at,
  first_stock_at,
  keycrm_created_at,
  last_restock_at,
  current_stock,
  stock_date,
  peak_stock_30d,
  window_start,
  window_end,
  vlife_days,
  sold_7d,
  sold_30d,
  sold_90d,
  revenue_30d,
  sold_total,
  last_sold_at,
  ROUND(sold_7d  / 7.0,  3) AS velocity_7d,
  ROUND(sold_30d / 30.0, 3) AS velocity_30d,
  ROUND(sold_90d / 90.0, 3) AS velocity_90d,
  CASE
    WHEN sold_total = 0 THEN NULL
    ELSE ROUND(sold_total / vlife_days::numeric, 3)
  END AS lifetime_velocity,
  CASE WHEN st30_calc IS NULL THEN NULL ELSE ROUND(st30_calc, 4) END AS sell_through_30d,
  CASE
    WHEN sold_7d = 0 THEN NULL
    ELSE ROUND(current_stock / NULLIF(sold_7d / 7.0, 0), 1)
  END AS days_of_supply,
  -- age_days: days since the SKU first existed in the catalog. We prefer
  -- KeyCRM's product creation date, then our first observed positive stock,
  -- then last_restock_at as last resort. Sold-out SKUs return NULL →
  -- frontend renders "—".
  CASE
    WHEN current_stock <= 0 THEN NULL
    WHEN keycrm_created_at IS NOT NULL THEN EXTRACT(DAY FROM (NOW() - keycrm_created_at))::int
    WHEN first_stock_at IS NOT NULL THEN EXTRACT(DAY FROM (NOW() - first_stock_at))::int
    WHEN last_restock_at IS NOT NULL THEN (CURRENT_DATE - last_restock_at)::int
    ELSE NULL
  END AS age_days,
  COALESCE(
    manual_status,
    CASE
      -- "Новий" — recently created OR restocked (within 30 days) AND has
      -- stock right now AND has zero sales history. The sold_total = 0
      -- gate prevents old SKUs from being mislabeled "Новий" after a
      -- single-unit return that bumps stock 0→1 (which detect_restocks
      -- can't distinguish from a real new batch).
      WHEN current_stock > 0
           AND sold_total = 0
           AND keycrm_created_at IS NOT NULL
           AND keycrm_created_at >= NOW() - INTERVAL '30 days' THEN 'new'
      WHEN current_stock > 0
           AND sold_total = 0
           AND last_restock_at IS NOT NULL
           AND last_restock_at >= (CURRENT_DATE - INTERVAL '30 days')::date THEN 'new'
      WHEN sold_30d >= 30 THEN 'hit'
      WHEN sold_30d >= 10
           AND st30_calc IS NOT NULL
           AND st30_calc >= 0.7 THEN 'hit'
      WHEN sold_30d >= 15 THEN 'good'
      WHEN sold_30d BETWEEN 6 AND 14 THEN 'slow'
      WHEN sold_30d BETWEEN 1 AND 5 THEN 'weak'
      WHEN current_stock > 0 THEN 'dead'
      ELSE 'archive'
    END
  ) AS status,
  CASE
    WHEN sold_total = 0 THEN 'dead'
    ELSE
      CASE
        WHEN sold_total / vlife_days::numeric >= 0.5  THEN 'hit'
        WHEN sold_total / vlife_days::numeric >= 0.2  THEN 'good'
        WHEN sold_total / vlife_days::numeric >= 0.05 THEN 'slow'
        ELSE 'dead'
      END
  END AS archive_tier
FROM base;

CREATE UNIQUE INDEX IF NOT EXISTS sku_metrics_offer_idx    ON sku_metrics(offer_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_status_idx   ON sku_metrics(status);
CREATE INDEX        IF NOT EXISTS sku_metrics_category_idx ON sku_metrics(category_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_velocity_idx ON sku_metrics(velocity_30d DESC);
CREATE INDEX        IF NOT EXISTS sku_metrics_lifetime_idx ON sku_metrics(lifetime_velocity DESC);
