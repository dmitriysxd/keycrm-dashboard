-- RFM-аналитика клиентов + CRM-заметки.
--
-- sales.buyer_id уже есть с миграции 008, но имя/телефон/тег "опт"
-- нигде не хранятся. Этот файл добавляет:
--   - buyer_statuses: справочник статусов (редактируется из UI)
--   - buyers: карточки покупателей (full_name, phone, is_wholesale, status_id)
--   - buyer_notes: история звонков (timeline записей)
--   - buyer_rfm: materialized view с recency/frequency/monetary + квинтили 1..5
--   - pg_cron job: ежедневный REFRESH в 03:15 UTC (после sku_metrics в 03:10)

------------------------------------------------------------
-- Справочник статусов
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buyer_statuses (
  id          SERIAL PRIMARY KEY,
  name        TEXT NOT NULL UNIQUE,
  color       TEXT,
  sort_order  INT NOT NULL DEFAULT 0
);

INSERT INTO buyer_statuses (name, color, sort_order) VALUES
  ('Новий',     '#3b82f6', 10),
  ('Активний',  '#10b981', 20),
  ('Тёплий',    '#f59e0b', 30),
  ('Сплячий',   '#6b7280', 40),
  ('VIP',       '#a855f7', 50),
  ('Відмова',   '#ef4444', 60)
ON CONFLICT (name) DO NOTHING;

------------------------------------------------------------
-- Карточки покупателей
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buyers (
  buyer_id           BIGINT      PRIMARY KEY,
  full_name          TEXT,
  phone              TEXT,
  email              TEXT,
  is_wholesale       BOOLEAN     NOT NULL DEFAULT FALSE,
  status_id          INT         REFERENCES buyer_statuses(id) ON DELETE SET NULL,
  first_seen_at      TIMESTAMPTZ,
  last_synced_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  custom_fields_raw  JSONB
);
CREATE INDEX IF NOT EXISTS buyers_phone_idx ON buyers(phone) WHERE phone IS NOT NULL;
CREATE INDEX IF NOT EXISTS buyers_wholesale_idx ON buyers(is_wholesale) WHERE is_wholesale;
CREATE INDEX IF NOT EXISTS buyers_status_idx ON buyers(status_id) WHERE status_id IS NOT NULL;

------------------------------------------------------------
-- История звонков
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS buyer_notes (
  id          BIGSERIAL    PRIMARY KEY,
  buyer_id    BIGINT       NOT NULL REFERENCES buyers(buyer_id) ON DELETE CASCADE,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  outcome     TEXT,
  body        TEXT         NOT NULL
);
CREATE INDEX IF NOT EXISTS buyer_notes_buyer_idx ON buyer_notes(buyer_id, created_at DESC);

------------------------------------------------------------
-- RFM materialized view
------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS buyer_rfm;

CREATE MATERIALIZED VIEW buyer_rfm AS
WITH base AS (
  SELECT
    buyer_id,
    MAX(ordered_at)::date                                          AS last_order_date,
    COUNT(DISTINCT order_id)                                       AS frequency,
    COALESCE(SUM(revenue), 0)::numeric(14,2)                       AS monetary,
    (CURRENT_DATE - MAX(ordered_at)::date)                         AS recency_days
  FROM sales
  WHERE buyer_id IS NOT NULL
    AND COALESCE(order_status, '') NOT IN (
      'cancelled','rejected','canceled',
      'Повернули','Відмовились','incorrect_data','underbid'
    )
    AND COALESCE(order_status, '') NOT LIKE 'Об%єднання замовлень'
  GROUP BY buyer_id
)
SELECT
  b.buyer_id,
  b.last_order_date,
  b.recency_days,
  b.frequency,
  b.monetary,
  -- меньший recency_days → выше скор (5 — недавний)
  (6 - NTILE(5) OVER (ORDER BY b.recency_days))::int               AS r_score,
  NTILE(5) OVER (ORDER BY b.frequency)::int                        AS f_score,
  NTILE(5) OVER (ORDER BY b.monetary)::int                         AS m_score
FROM base b;

CREATE UNIQUE INDEX IF NOT EXISTS buyer_rfm_pk ON buyer_rfm(buyer_id);
CREATE INDEX IF NOT EXISTS buyer_rfm_monetary_idx ON buyer_rfm(monetary DESC);
CREATE INDEX IF NOT EXISTS buyer_rfm_recency_idx ON buyer_rfm(recency_days);

CREATE OR REPLACE FUNCTION refresh_buyer_rfm()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY buyer_rfm;
END;
$$;

------------------------------------------------------------
-- pg_cron: refresh buyer_rfm at 03:15 UTC (5 min after sku_metrics).
------------------------------------------------------------
DO $$
DECLARE
  jid BIGINT;
BEGIN
  SELECT jobid INTO jid FROM cron.job WHERE jobname = 'refresh-buyer-rfm';
  IF jid IS NOT NULL THEN
    PERFORM cron.unschedule(jid);
  END IF;
END $$;

SELECT cron.schedule(
  'refresh-buyer-rfm',
  '15 3 * * *',
  $$REFRESH MATERIALIZED VIEW CONCURRENTLY buyer_rfm;$$
);
