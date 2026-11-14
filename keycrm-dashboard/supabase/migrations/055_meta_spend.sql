-- Migration 055: авто-розхід Meta Ads (із Meta Marketing API).
--
-- Крок meta_spend у ingest тягне insights по кампаніях (spend/impressions/
-- clicks) по днях і складає сюди. Маркетингова сторінка потім бере суму за
-- період як «Витрати» каналу Meta Ads → рахує ROAS/CPL/CPM/CTR автоматично.

CREATE TABLE IF NOT EXISTS meta_spend (
  date          date   NOT NULL,
  campaign_id   text   NOT NULL,
  campaign_name text,
  spend         numeric(14,2) NOT NULL DEFAULT 0,
  impressions   bigint NOT NULL DEFAULT 0,
  clicks        bigint NOT NULL DEFAULT 0,
  currency      text,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (date, campaign_id)
);

CREATE INDEX IF NOT EXISTS meta_spend_date_idx ON meta_spend(date);

GRANT SELECT, INSERT, UPDATE, DELETE ON meta_spend TO authenticated, anon, service_role;
