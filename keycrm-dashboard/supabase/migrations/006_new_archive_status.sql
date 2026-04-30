-- v3 of sku_metrics:
--   * adds skus.keycrm_created_at  — date the product was created in KeyCRM
--   * adds skus.last_restock_at    — date stock most recently transitioned 0 → >0
--   * adds detect_restocks() helper (called by ingest after writing today's snapshots)
--   * status logic gains:
--       'new'      — 14-day grace from creation OR latest restock
--       'archive'  — sold-out catalog leftover, outside sales bands and grace
--   * 'hit' now also catches small fast-turnover batches:
--       (V30 >= 1) OR (sold_30d >= 5 AND sell_through_30d >= 0.7)
--   * exposes lifetime_velocity (sold_total / age_days, capped at >=1 day)
--     plus archive_tier (hit/good/slow/dead, computed from lifetime_velocity)
--     so we can see what an archived SKU's lifetime performance was without
--     losing that signal when stock hits zero.
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
    COALESCE(s7.qty,  0)::numeric AS sold_7d,
    COALESCE(s30.qty, 0)::numeric AS sold_30d,
    COALESCE(s90.qty, 0)::numeric AS sold_90d,
    COALESCE(s30.revenue, 0)::numeric AS revenue_30d,
    COALESCE(st.qty_all, 0)::numeric AS sold_total,
    st.last_sold_at,
    -- sell-through over last 30 days
    CASE
      WHEN COALESCE(s30.qty, 0) + COALESCE(ls.current_stock, 0) = 0 THEN NULL
      ELSE COALESCE(s30.qty, 0)::numeric
           / NULLIF(COALESCE(s30.qty, 0) + COALESCE(ls.current_stock, 0), 0)::numeric
    END AS st30_calc,
    -- lifetime velocity = sold_total / age_in_days (capped at >= 1 day)
    CASE
      WHEN COALESCE(st.qty_all, 0) = 0 THEN 0
      ELSE COALESCE(st.qty_all, 0)::numeric / GREATEST(
        EXTRACT(EPOCH FROM (NOW() - COALESCE(
          s.keycrm_created_at,
          s.first_stock_at,
          s.first_seen_at,
          NOW()
        )))::numeric / 86400.0,
        1.0
      )
    END AS lifetime_velocity_calc
  FROM skus s
  LEFT JOIN latest_stock ls  ON ls.offer_id   = s.offer_id
  LEFT JOIN sales_7      s7  ON s7.product_id = s.product_id
  LEFT JOIN sales_30     s30 ON s30.product_id = s.product_id
  LEFT JOIN sales_90     s90 ON s90.product_id = s.product_id
  LEFT JOIN sales_total  st  ON st.product_id  = s.product_id
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
  sold_7d,
  sold_30d,
  sold_90d,
  revenue_30d,
  sold_total,
  last_sold_at,
  ROUND(sold_7d  / 7.0,  3) AS velocity_7d,
  ROUND(sold_30d / 30.0, 3) AS velocity_30d,
  ROUND(sold_90d / 90.0, 3) AS velocity_90d,
  ROUND(lifetime_velocity_calc, 3) AS lifetime_velocity,
  CASE WHEN st30_calc IS NULL THEN NULL ELSE ROUND(st30_calc, 4) END AS sell_through_30d,
  CASE
    WHEN sold_7d = 0 THEN NULL
    ELSE ROUND(current_stock / NULLIF(sold_7d / 7.0, 0), 1)
  END AS days_of_supply,
  CASE
    WHEN first_stock_at IS NULL THEN NULL
    ELSE EXTRACT(DAY FROM (NOW() - first_stock_at))::int
  END AS age_days,
  COALESCE(
    manual_status,
    CASE
      -- new: 14-day grace from creation OR latest restock
      WHEN keycrm_created_at IS NOT NULL
           AND keycrm_created_at >= NOW() - INTERVAL '14 days' THEN 'new'
      WHEN last_restock_at IS NOT NULL
           AND last_restock_at >= (CURRENT_DATE - INTERVAL '14 days')::date THEN 'new'
      -- hit: V30 >= 1 (>= 30/міс)
      WHEN sold_30d >= 30 THEN 'hit'
      -- hit: small fast-turnover batch (sold ≥ 5 AND ≥ 70% of available stock cleared)
      WHEN sold_30d >= 5 AND st30_calc IS NOT NULL AND st30_calc >= 0.7 THEN 'hit'
      -- good: 15..29/міс
      WHEN sold_30d >= 15 THEN 'good'
      -- slow: 6..14/міс
      WHEN sold_30d BETWEEN 6 AND 14 THEN 'slow'
      -- weak: 1..5/міс (and didn't qualify as fast-turnover hit)
      WHEN sold_30d BETWEEN 1 AND 5 THEN 'weak'
      -- s30 = 0 from here
      WHEN current_stock > 0 THEN 'dead'   -- has stock, no recent sales, not new
      ELSE 'archive'                       -- no stock, no recent sales, not new
    END
  ) AS status,
  -- archive tier: lifetime-velocity classification, meaningful only when status='archive'
  CASE
    WHEN lifetime_velocity_calc >= 0.5  THEN 'hit'
    WHEN lifetime_velocity_calc >= 0.2  THEN 'good'
    WHEN lifetime_velocity_calc >= 0.05 THEN 'slow'
    ELSE 'dead'
  END AS archive_tier
FROM base;

CREATE UNIQUE INDEX IF NOT EXISTS sku_metrics_offer_idx    ON sku_metrics(offer_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_status_idx   ON sku_metrics(status);
CREATE INDEX        IF NOT EXISTS sku_metrics_category_idx ON sku_metrics(category_id);
CREATE INDEX        IF NOT EXISTS sku_metrics_velocity_idx ON sku_metrics(velocity_30d DESC);
CREATE INDEX        IF NOT EXISTS sku_metrics_lifetime_idx ON sku_metrics(lifetime_velocity DESC);
