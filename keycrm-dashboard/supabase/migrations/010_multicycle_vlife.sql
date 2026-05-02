-- v7 of sku_metrics: multi-cycle lifetime_velocity + finalized status filter.
--
-- Two changes vs v6 (migration 009):
--
-- 1) MULTI-CYCLE Vlife denominator.
--    Old formula: sold_total / (last_active_day - first_active_day + 1).
--    For SKUs that sold out, sat at 0 for weeks, then were re-stocked,
--    that "single window" inflates the denominator (counts gap days as
--    selling days) → Vlife under-reports.
--    New formula: sold_total / COUNT(DISTINCT snapshot_date WHERE qty > 0).
--    Each calendar day where we recorded any positive stock counts;
--    multi-cycle products correctly get the SUM of their active days.
--    For SKUs that depleted before our snapshot history began (no day
--    in stock_snapshots ever had qty > 0), we still fall back to the
--    sales-date window so they get a reasonable approximation.
--
-- 2) Status filter aligned with user's CRM mapping (carries over 009):
--    EXCLUDED (not real sales):
--      cancelled / canceled / rejected,
--      Повернули, Відмовились, "Об'єднання замовлень",
--      incorrect_data, underbid (НЕДОЗВОН).
--    INCLUDED:
--      completed (ВИКОНАНО), delivered (ДОСТАВЛЯЄТЬСЯ),
--      delivered_to_delivery (ПЕРЕДАНО В ДОСТАВКУ),
--      in_transit (В ДОРОЗІ), Очікує оплату, new (НОВИЙ),
--      Зв'язатись повторно, anything else not in the exclude list.
--
-- This migration supersedes 009. Applying just 010 is sufficient.

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
          'Повернули', 'Відмовились', 'Об''єднання замовлень',
          'incorrect_data', 'underbid'
        )
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_7 AS (
  SELECT product_id, SUM(quantity) AS qty
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '7 days'
    AND COALESCE(order_status, '') NOT IN (
          'cancelled', 'rejected', 'canceled',
          'Повернули', 'Відмовились', 'Об''єднання замовлень',
          'incorrect_data', 'underbid'
        )
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
          'Повернули', 'Відмовились', 'Об''єднання замовлень',
          'incorrect_data', 'underbid'
        )
    AND product_id IS NOT NULL
  GROUP BY product_id
),
sales_90 AS (
  SELECT product_id, SUM(quantity) AS qty
  FROM sales
  WHERE ordered_at >= NOW() - INTERVAL '90 days'
    AND COALESCE(order_status, '') NOT IN (
          'cancelled', 'rejected', 'canceled',
          'Повернули', 'Відмовились', 'Об''єднання замовлень',
          'incorrect_data', 'underbid'
        )
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
          'Повернули', 'Відмовились', 'Об''єднання замовлень',
          'incorrect_data', 'underbid'
        )
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
    COALESCE(p30.qty, 0)::numeric AS peak_stock_30d,
    sw.first_positive,
    sw.last_positive,
    sw.days_in_stock,
    sd.first_sold,
    sd.last_sold,
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
    -- Vlife denominator: prefer days actually in stock (multi-cycle aware);
    -- fall back to sales-date window for SKUs whose stock history predates
    -- our ingest (snapshots never showed qty > 0).
    CASE
      WHEN COALESCE(sw.days_in_stock, 0) > 0 THEN sw.days_in_stock
      WHEN sd.first_sold IS NOT NULL AND sd.last_sold IS NOT NULL
        THEN GREATEST((sd.last_sold - sd.first_sold) + 1, 1)
      ELSE NULL
    END AS vlife_days
  FROM skus s
  LEFT JOIN latest_stock     ls  ON ls.offer_id    = s.offer_id
  LEFT JOIN peak_stock_30d   p30 ON p30.offer_id   = s.offer_id
  LEFT JOIN selling_window   sw  ON sw.offer_id    = s.offer_id
  LEFT JOIN sales_dates      sd  ON sd.product_id  = s.product_id
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
    WHEN sold_total = 0 OR vlife_days IS NULL OR vlife_days <= 0 THEN NULL
    ELSE ROUND(sold_total / vlife_days::numeric, 3)
  END AS lifetime_velocity,
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
      WHEN keycrm_created_at IS NOT NULL
           AND keycrm_created_at >= NOW() - INTERVAL '14 days'
           AND sold_total = 0 THEN 'new'
      WHEN last_restock_at IS NOT NULL
           AND last_restock_at >= (CURRENT_DATE - INTERVAL '14 days')::date
           AND sold_total = 0 THEN 'new'
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
    WHEN sold_total = 0 OR vlife_days IS NULL OR vlife_days <= 0 THEN 'dead'
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
