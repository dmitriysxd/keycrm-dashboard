-- Migration 062: час останнього рефрешу matview у ingest_state.
--
-- Для чесного індикатора свіжості: окремо знати, коли РЕАЛЬНО оновились
-- sku_metrics і buyer_rfm (крок metrics може впасти по таймауту, а цикл
-- піде далі — тоді matview застарілі, хоч цикл «пройшов»).

ALTER TABLE ingest_state
  ADD COLUMN IF NOT EXISTS sku_metrics_at timestamptz,
  ADD COLUMN IF NOT EXISTS buyer_rfm_at   timestamptz;
