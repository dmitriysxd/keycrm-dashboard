-- Migration 063: надійний рефреш matview на стороні БД (pg_cron), «розумно».
--
-- ПРОБЛЕМА: рефреш sku_metrics виріс і не влазить у serverless-ліміти
-- (Vercel 60с, supabase-клієнт 30с) → sku_metrics застрягало застарілим.
-- А фіксований pg_cron (міграція 036 його прибрала) рахував би matview на
-- ВЧОРАШНІХ даних, бо ingest часто запізнюється.
--
-- РІШЕННЯ: pg_cron щопівгодини ПЕРЕВІРЯЄ, чи сьогоднішній цикл ingest
-- завершено (ingest_state.status='done', cycle_date=сьогодні) і чи matview
-- ще НЕ оновлювали сьогодні. Якщо так — рефрешить ОБИДВІ matview на стороні
-- БД (до 300с, CONCURRENTLY — без блокування читань) і ставить час у
-- ingest_state. Так рефреш завжди на СВІЖИХ даних і поза лімітом serverless.

CREATE OR REPLACE FUNCTION refresh_matviews_if_ready()
RETURNS text
LANGUAGE plpgsql
SET statement_timeout = '300000'  -- 5 хв на всю операцію
AS $$
DECLARE
  st ingest_state%ROWTYPE;
BEGIN
  SELECT * INTO st FROM ingest_state WHERE id = 1;
  IF st IS NULL THEN RETURN 'no state'; END IF;
  -- рефрешимо лише коли цикл за СЬОГОДНІ завершено і ще не рефрешили сьогодні
  IF st.cycle_date = current_date AND st.status = 'done'
     AND (st.sku_metrics_at IS NULL OR st.sku_metrics_at::date < current_date) THEN
    PERFORM refresh_sku_metrics();
    PERFORM refresh_buyer_rfm();
    UPDATE ingest_state SET sku_metrics_at = now(), buyer_rfm_at = now() WHERE id = 1;
    RETURN 'refreshed';
  END IF;
  RETURN 'skip (not ready or already done today)';
END;
$$;

-- Прибираємо старий розклад, якщо лишився, і ставимо новий.
DO $$
DECLARE jid BIGINT;
BEGIN
  SELECT jobid INTO jid FROM cron.job WHERE jobname = 'refresh-matviews-smart';
  IF jid IS NOT NULL THEN PERFORM cron.unschedule(jid); END IF;
END $$;

-- Кожні 30 хв: перевірка + рефреш один раз на день, коли дані готові.
SELECT cron.schedule('refresh-matviews-smart', '*/30 * * * *', 'SELECT refresh_matviews_if_ready()');
