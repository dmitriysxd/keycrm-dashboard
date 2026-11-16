-- Migration 060: розхід Meta по ОГОЛОШЕННЯХ (level=ad) для звіту «Реклама».
--
-- Аналог meta_spend, але зерно — оголошення (ad). Дає drill-down
-- кампанія→оголошення: spend/impressions/clicks по кожному оголошенню і дню.
-- spend — у валюті кабінету; spend_uah — по курсу НБУ на дату + ПДВ.

CREATE TABLE IF NOT EXISTS meta_ad_spend (
  date          date   NOT NULL,
  ad_id         text   NOT NULL,
  ad_name       text,
  adset_id      text,
  adset_name    text,
  campaign_id   text,
  campaign_name text,
  spend         numeric(14,2) NOT NULL DEFAULT 0,
  currency      text,
  fx_rate       numeric(12,4),
  spend_uah     numeric(14,2),
  impressions   bigint NOT NULL DEFAULT 0,
  clicks        bigint NOT NULL DEFAULT 0,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, ad_id)
);

CREATE INDEX IF NOT EXISTS meta_ad_spend_date_idx ON meta_ad_spend(date);
CREATE INDEX IF NOT EXISTS meta_ad_spend_campaign_idx ON meta_ad_spend(campaign_id);

GRANT SELECT, INSERT, UPDATE, DELETE ON meta_ad_spend TO authenticated, anon, service_role;
