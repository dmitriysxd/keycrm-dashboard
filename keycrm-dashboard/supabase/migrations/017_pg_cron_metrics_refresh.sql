-- Move sku_metrics refresh out of the Vercel cron and into Postgres via
-- pg_cron, so it isn't constrained by Vercel's 60-second function limit.
--
-- Background: REFRESH MATERIALIZED VIEW sku_metrics takes 30-90 seconds
-- at our scale (3700 SKUs × 150k+ sales rows). The Vercel cron tries to
-- run it as the last step of the daily ingest, but at peak times the
-- function times out and Postgres cancels the refresh, leaving
-- ingest_state stuck at status='error', current_step='metrics' until
-- manual intervention.
--
-- Fix: pg_cron schedules the refresh server-side at 03:10 UTC daily
-- (10 minutes after the Vercel ingest starts at 03:00 UTC). Postgres
-- runs the refresh to completion regardless of how long it takes;
-- no function timeout to worry about.
--
-- CONCURRENTLY ensures dashboard reads aren't blocked during the
-- refresh — possible because sku_metrics has a unique index on offer_id.
--
-- Prerequisite: pg_cron extension must be enabled in
-- Supabase Dashboard → Database → Extensions → pg_cron (or this
-- migration enables it if your role has permission).

CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Define the refresh function explicitly so its behaviour is documented
-- in our migrations (we already call this RPC from ingest.js — this
-- migration takes over ownership).
CREATE OR REPLACE FUNCTION refresh_sku_metrics()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW CONCURRENTLY sku_metrics;
END;
$$;

-- Unschedule any previous job with this name (idempotent re-run).
DO $$
DECLARE
  jid BIGINT;
BEGIN
  SELECT jobid INTO jid FROM cron.job WHERE jobname = 'refresh-sku-metrics';
  IF jid IS NOT NULL THEN
    PERFORM cron.unschedule(jid);
  END IF;
END $$;

-- Schedule daily refresh at 03:10 UTC.
SELECT cron.schedule(
  'refresh-sku-metrics',
  '10 3 * * *',
  $$REFRESH MATERIALIZED VIEW CONCURRENTLY sku_metrics;$$
);

-- Clear any stale error state so today's cron starts fresh. The Vercel
-- ingest code added in this PR also recovers from this automatically
-- (>12h-old error → fresh cycle), but doing it once here unblocks the
-- next cron run immediately.
UPDATE ingest_state SET status = 'idle' WHERE status = 'error';
