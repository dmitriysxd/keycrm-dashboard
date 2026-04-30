-- Singleton state row for chained auto-ingest.
-- The cron handler reads this row, processes one small chunk, updates the row,
-- and (if not finished) self-triggers via HTTP to keep the chain going.

CREATE TABLE IF NOT EXISTS ingest_state (
  id                SMALLINT     PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  cycle_date        DATE,
  cycle_started_at  TIMESTAMPTZ,
  current_step      TEXT         NOT NULL DEFAULT 'products',
  current_page      INT          NOT NULL DEFAULT 1,
  status            TEXT         NOT NULL DEFAULT 'idle',
  last_chunk_at     TIMESTAMPTZ,
  last_error        TEXT,
  updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

INSERT INTO ingest_state (id, status)
VALUES (1, 'idle')
ON CONFLICT (id) DO NOTHING;
