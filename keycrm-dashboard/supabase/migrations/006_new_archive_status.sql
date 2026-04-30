-- v3 of sku_metrics:
--   * adds skus.keycrm_created_at  — date the product was created in KeyCRM
--   * adds skus.last_restock_at    — date stock most recently transitioned 0 → >0
--   * adds detect_restocks() helper (called by ingest after writing today's snapshots)
--   * status logic gains 'new' (14-day grace from creation OR latest restock) and
--     'archive' (no stock + not new + outside sales bands)
-- Apply via Supabase SQL Editor.

ALTER TABLE skus ADD COLUMN IF NOT EXISTS keycrm_created_at TIMESTAMPTZ;
ALTER TABLE skus ADD COLUMN IF NOT EXISTS last_restock_at   DATE;

CREATE INDEX IF NOT EXISTS skus_keycrm_created_at_idx ON skus(keycrm_created_at);
CREATE INDEX IF NOT EXISTS skus_last_restock_at_idx   ON skus(last_restock_at);

-- Detect 0 → >0 transitions on a given snapshot date and update skus.last_restock_at.
-- Uses INNER JOIN LATERAL so offers without any prior snapshot are skipped — this
-- prevents the very first ingest from flagging every active SKU as "just restocked".
CREATE OR REPLACE FUNCTION detect_restocks(target_date DATE)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
  updated_count INTEGER;
BEGIN
  WITH restocked AS (
    SELECT cur.offer_id
    FROM stock_snapshots cur
    JOIN LATERAL (
      SELECT quantity
      FROM stock_snapshots
      WHERE offer_id = cur.offer_id
        AND snapshot_date < cur.snapshot_date
      ORDER BY snapshot_date DESC
      LIMIT 1
    ) prev ON TRUE
    WHERE cur.snapshot_date = target_date
      AND cur.quantity > 0
      AND prev.quantity = 0
  ),
  upd AS (
    UPDATE skus s
    SET last_restock_at = target_date
    FROM restocked r
    WHERE s.offer_id = r.offer_id
    RETURNING 1
  )
  SELECT COUNT(*)::INTEGER INTO updated_count FROM upd;
  RETURN updated_count;
END;
$$;

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
  s.keycrm_created_at,
  s.last_restock_at,
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
      -- new: 14-day grace from creation OR latest restock
      WHEN s.keycrm_created_at IS NOT NULL
           AND s.keycrm_created_at >= NOW() - INTERVAL '14 days' THEN 'new'
      WHEN s.last_restock_at IS NOT NULL
           AND s.last_restock_at >= (CURRENT_DATE - INTERVAL '14 days')::date THEN 'new'
      -- sales bands (apply regardless of current stock)
      WHEN COALESCE(s30.qty, 0) >= 30 THEN 'hit'
      WHEN COALESCE(s30.qty, 0) >= 15 THEN 'good'
      WHEN COALESCE(s30.qty, 0) BETWEEN 6 AND 14 THEN 'slow'
      WHEN COALESCE(s30.qty, 0) BETWEEN 1 AND 5  THEN 'weak'
      -- s30 = 0 from here
      WHEN COALESCE(ls.current_stock, 0) > 0 THEN 'dead'      -- has stock, no recent sales
      ELSE 'archive'                                          -- no stock, no recent sales, not new
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
