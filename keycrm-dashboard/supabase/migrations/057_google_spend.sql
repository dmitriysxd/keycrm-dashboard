-- Migration 057: авто-розхід Google Ads (із Google Ads API).
--
-- Аналог meta_spend: крок google_spend у ingest тягне cost/impressions/clicks
-- по кампаніях і днях. spend — в валюті акаунта; spend_uah — конвертовано по
-- курсу НБУ на дату розходу + ПДВ. Дашборд бере суму за період як «Витрати»
-- каналу Google Ads → ROAS/CPM/CTR.

CREATE TABLE IF NOT EXISTS google_spend (
  date          date   NOT NULL,
  campaign_id   text   NOT NULL,
  campaign_name text,
  spend         numeric(14,2) NOT NULL DEFAULT 0,
  currency      text,
  fx_rate       numeric(12,4),
  spend_uah     numeric(14,2),
  impressions   bigint NOT NULL DEFAULT 0,
  clicks        bigint NOT NULL DEFAULT 0,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, campaign_id)
);

CREATE INDEX IF NOT EXISTS google_spend_date_idx ON google_spend(date);

GRANT SELECT, INSERT, UPDATE, DELETE ON google_spend TO authenticated, anon, service_role;
