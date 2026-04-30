-- SKU analytics base schema
-- Apply via Supabase Studio → SQL Editor → New query → paste → Run

-- skus: master catalog with manual fields preserved across ingests
CREATE TABLE IF NOT EXISTS skus (
  offer_id        BIGINT      PRIMARY KEY,
  product_id      BIGINT      NOT NULL,
  sku             TEXT,
  name            TEXT        NOT NULL,
  category_id     BIGINT,
  category_name   TEXT,
  price           NUMERIC(12,2),
  first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  first_stock_at  TIMESTAMPTZ,
  last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
  cost            NUMERIC(12,2),
  supplier_lot    TEXT,
  notes           TEXT,
  manual_status   TEXT,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS skus_product_id_idx     ON skus(product_id);
CREATE INDEX IF NOT EXISTS skus_category_id_idx    ON skus(category_id);
CREATE INDEX IF NOT EXISTS skus_first_stock_at_idx ON skus(first_stock_at);
CREATE INDEX IF NOT EXISTS skus_active_idx         ON skus(is_active) WHERE is_active = TRUE;

-- stock_snapshots: one row per SKU per day
CREATE TABLE IF NOT EXISTS stock_snapshots (
  snapshot_date  DATE          NOT NULL,
  offer_id       BIGINT        NOT NULL REFERENCES skus(offer_id) ON DELETE CASCADE,
  quantity       NUMERIC(10,2) NOT NULL,
  price          NUMERIC(12,2),
  PRIMARY KEY (snapshot_date, offer_id)
);
CREATE INDEX IF NOT EXISTS stock_snapshots_offer_idx ON stock_snapshots(offer_id, snapshot_date DESC);

-- sales: one row per order line item, idempotent on (order_id, line_idx)
CREATE TABLE IF NOT EXISTS sales (
  order_id        BIGINT        NOT NULL,
  line_idx        INT           NOT NULL,
  offer_id        BIGINT,
  product_id      BIGINT,
  name_snapshot   TEXT,
  quantity        NUMERIC(10,2) NOT NULL,
  unit_price      NUMERIC(12,2) NOT NULL,
  revenue         NUMERIC(14,2) NOT NULL,
  order_status    TEXT,
  ordered_at      TIMESTAMPTZ   NOT NULL,
  PRIMARY KEY (order_id, line_idx)
);
CREATE INDEX IF NOT EXISTS sales_offer_ordered_idx   ON sales(offer_id, ordered_at DESC);
CREATE INDEX IF NOT EXISTS sales_product_ordered_idx ON sales(product_id, ordered_at DESC);
CREATE INDEX IF NOT EXISTS sales_ordered_at_idx      ON sales(ordered_at DESC);

-- ingest_runs: log + watchdog
CREATE TABLE IF NOT EXISTS ingest_runs (
  id              BIGSERIAL   PRIMARY KEY,
  started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at     TIMESTAMPTZ,
  kind            TEXT        NOT NULL,
  status          TEXT        NOT NULL DEFAULT 'running',
  products_seen   INT,
  offers_seen     INT,
  orders_seen     INT,
  sales_upserted  INT,
  api_calls       INT,
  error_message   TEXT,
  meta            JSONB
);
CREATE INDEX IF NOT EXISTS ingest_runs_started_idx ON ingest_runs(started_at DESC);
