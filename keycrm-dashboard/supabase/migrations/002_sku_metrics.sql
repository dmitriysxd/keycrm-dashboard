-- SKU metrics materialized view + refresh helper
-- Apply AFTER 001_init.sql

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
  SELECT offer_id, product_id, SUM(quantity) AS qty
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '7 days'
    AND COALESCE(order_status, '') NOT IN ('cancelled', 'rejected', 'canceled')
  GROUP BY offer_id, product_id
),
sales_30 AS (
  SELECT offer_id, product_id,
         SUM(quantity) AS qty,
         SUM(revenue)  AS revenue
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '30 days'
    AND COALESCE(order_status, '') NOT IN ('cancelled', 'rejected', 'canceled')
  GROUP BY offer_id, product_id
),
sales_total AS (
  SELECT offer_id, product_id,
         SUM(quantity) AS qty_all,
         MAX(ordered_at) AS last_sold_at
  FROM sales
  WHERE COALESCE(order_status, '') NOT IN ('cancelled', 'rejected', 'canceled')
  GROUP BY offer_id, product_id
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
  COALESCE(s7.qty,  s7p.qty,  0)::numeric  AS sold_7d,
  COALESCE(s30.qty, s30p.qty, 0)::numeric  AS sold_30d,
  COALESCE(s30.revenue, s30p.revenue, 0)::numeric AS revenue_30d,
  COALESCE(st.qty_all, stp.qty_all, 0)::numeric   AS sold_total,
  COALESCE(st.last_sold_at, stp.last_sold_at)     AS last_sold_at,
  ROUND(COALESCE(s7.qty,  s7p.qty,  0)::numeric  / 7.0,  3) AS velocity_7d,
  ROUND(COALESCE(s30.qty, s30p.qty, 0)::numeric / 30.0, 3) AS velocity_30d,
  CASE
    WHEN COALESCE(s30.qty, s30p.qty, 0) + COALESCE(ls.current_stock, 0) = 0 THEN NULL
    ELSE ROUND(
      COALESCE(s30.qty, s30p.qty, 0)::numeric
      / NULLIF(COALESCE(s30.qty, s30p.qty, 0) + COALESCE(ls.current_stock, 0), 0)::numeric,
      4
    )
  END AS sell_through_30d,
  CASE
    WHEN COALESCE(s7.qty, s7p.qty, 0) = 0 THEN NULL
    ELSE ROUND(
      COALESCE(ls.current_stock, 0)::numeric
      / NULLIF(COALESCE(s7.qty, s7p.qty, 0)::numeric / 7.0, 0),
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
      WHEN s.first_stock_at IS NULL THEN 'new'
      WHEN EXTRACT(DAY FROM (NOW() - s.first_stock_at)) < 14 THEN 'new'
      WHEN EXTRACT(DAY FROM (NOW() - s.first_stock_at)) >= 60
           AND COALESCE(s30.qty, s30p.qty, 0) = 0
           AND COALESCE(ls.current_stock, 0) > 0 THEN 'dead'
      WHEN COALESCE(s30.qty, s30p.qty, 0) / 30.0 >= 0.3
           AND (
             COALESCE(s7.qty, s7p.qty, 0) > 0
             AND COALESCE(ls.current_stock, 0) / NULLIF(COALESCE(s7.qty, s7p.qty, 0)::numeric / 7.0, 0) <= 60
           ) THEN 'hit'
      ELSE 'slow'
    END
  ) AS status
FROM skus s
LEFT JOIN latest_stock ls  ON ls.offer_id  = s.offer_id
LEFT JOIN sales_7      s7  ON s7.offer_id  = s.offer_id
LEFT JOIN sales_30     s30 ON s30.offer_id = s.offer_id
LEFT JOIN sales_total  st  ON st.offer_id  = s.offer_id
LEFT JOIN sales_7      s7p  ON s7p.offer_id  IS NULL AND s7p.product_id  = s.product_id
LEFT JOIN sales_30     s30p ON s30p.offer_id IS NULL AND s30p.product_id = s.product_id
LEFT JOIN sales_total  stp  ON stp.offer_id  IS NULL AND stp.product_id  = s.product_id;

CREATE UNIQUE INDEX IF NOT EXISTS sku_metrics_offer_idx    ON sku_metrics(offer_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_status_idx   ON sku_metrics(status);
CREATE INDEX        IF NOT EXISTS sku_metrics_category_idx ON sku_metrics(category_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_velocity_idx ON sku_metrics(velocity_30d DESC);

CREATE OR REPLACE FUNCTION refresh_sku_metrics() RETURNS void AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY sku_metrics;
EXCEPTION WHEN OTHERS THEN
  REFRESH MATERIALIZED VIEW sku_metrics;
END;
$$ LANGUAGE plpgsql;
