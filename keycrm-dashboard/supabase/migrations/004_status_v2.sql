-- v2 of sku_metrics:
--   * adds velocity_90d (sold last 90 days / 90)
--   * exposes sold_30d (was already computed, now plain column)
--   * status thresholds switched to monthly-quantity boundaries
--     (hit ≥ 30, good 15..29, slow 6..14, weak 1..5, dead, new)
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
sales_7 AS (
  SELECT product_id, SUM(quantity) AS qty
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '7 days'
    AND COALESCE(order_status, '') NOT IN ('cancelled', 'rejected', 'canceled')
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_30 AS (
  SELECT product_id,
         SUM(quantity) AS qty,
         SUM(revenue)  AS revenue
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '30 days'
    AND COALESCE(order_status, '') NOT IN ('cancelled', 'rejected', 'canceled')
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_90 AS (
  SELECT product_id, SUM(quantity) AS qty
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '90 days'
    AND COALESCE(order_status, '') NOT IN ('cancelled', 'rejected', 'canceled')
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_total AS (
  SELECT product_id,
         SUM(quantity) AS qty_all,
         MAX(ordered_at) AS last_sold_at
  FROM sales
  WHERE COALESCE(order_status, '') NOT IN ('cancelled', 'rejected', 'canceled')
    AND product_id IS NOT NULL
  GROUP BY product_id
)
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
  COALESCE(ls.current_stock, 0)::numeric AS current_stock,
  ls.stock_date,
  COALESCE(s7.qty,  0)::numeric AS sold_7d,
  COALESCE(s30.qty, 0)::numeric AS sold_30d,
  COALESCE(s90.qty, 0)::numeric AS sold_90d,
  COALESCE(s30.revenue, 0)::numeric AS revenue_30d,
  COALESCE(st.qty_all, 0)::numeric AS sold_total,
  st.last_sold_at,
  ROUND(COALESCE(s7.qty,  0)::numeric / 7.0,  3) AS velocity_7d,
  ROUND(COALESCE(s30.qty, 0)::numeric / 30.0, 3) AS velocity_30d,
  ROUND(COALESCE(s90.qty, 0)::numeric / 90.0, 3) AS velocity_90d,
  CASE
    WHEN COALESCE(s30.qty, 0) + COALESCE(ls.current_stock, 0) = 0 THEN NULL
    ELSE ROUND(
      COALESCE(s30.qty, 0)::numeric
      / NULLIF(COALESCE(s30.qty, 0) + COALESCE(ls.current_stock, 0), 0)::numeric,
      4
    )
  END AS sell_through_30d,
  CASE
    WHEN COALESCE(s7.qty, 0) = 0 THEN NULL
    ELSE ROUND(
      COALESCE(ls.current_stock, 0)::numeric
      / NULLIF(COALESCE(s7.qty, 0)::numeric / 7.0, 0),
      1
    )
  END AS days_of_supply,
  CASE
    WHEN s.first_stock_at IS NULL THEN NULL
    ELSE EXTRACT(DAY FROM (NOW() - s.first_stock_at))::int
  END AS age_days,
  COALESCE(
    s.manual_status,
    CASE
      -- new: nothing ever sold AND nothing in stock
      WHEN COALESCE(st.qty_all, 0) = 0 AND COALESCE(ls.current_stock, 0) = 0 THEN 'new'
      -- dead: has stock but zero sales in last 30 days
      WHEN COALESCE(ls.current_stock, 0) > 0 AND COALESCE(s30.qty, 0) = 0 THEN 'dead'
      -- hit: V30 >= 1 (>= 30 / month)
      WHEN COALESCE(s30.qty, 0) >= 30 THEN 'hit'
      -- good: V30 in [0.5, 1) (15..29 / month)
      WHEN COALESCE(s30.qty, 0) >= 15 THEN 'good'
      -- weak: 1..5 / month
      WHEN COALESCE(s30.qty, 0) BETWEEN 1 AND 5 THEN 'weak'
      -- slow: everything else (6..14 / month, or zero sales but no stock)
      ELSE 'slow'
    END
  ) AS status
FROM skus s
LEFT JOIN latest_stock ls  ON ls.offer_id   = s.offer_id
LEFT JOIN sales_7      s7  ON s7.product_id = s.product_id
LEFT JOIN sales_30     s30 ON s30.product_id = s.product_id
LEFT JOIN sales_90     s90 ON s90.product_id = s.product_id
LEFT JOIN sales_total  st  ON st.product_id  = s.product_id;

CREATE UNIQUE INDEX IF NOT EXISTS sku_metrics_offer_idx    ON sku_metrics(offer_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_status_idx   ON sku_metrics(status);
CREATE INDEX        IF NOT EXISTS sku_metrics_category_idx ON sku_metrics(category_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_velocity_idx ON sku_metrics(velocity_30d DESC);
