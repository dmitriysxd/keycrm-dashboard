-- Migration 036: прибрати pg_cron-задачі рефрешу matview, перенести в ingest.
--
-- ПРИЧИНА: pg_cron 'refresh-sku-metrics' (03:10 UTC) і 'refresh-buyer-rfm'
-- (03:15 UTC) рахували matview на ЗАСТАРІЛИХ даних, бо Vercel-cron
-- ingest'у на Hobby-плані часто пропускає 03:00 UTC і реальне залиття
-- даних відбувається о 03:30+ (через GitHub Actions backup-cron). Тобто
-- matview оновлювався, але по вчорашнім сирим даним — а реально свіжий
-- ingest приходив ПІСЛЯ refresh'у.
--
-- ФІКС: refresh тепер викликається з самого ingest'у (api/cron/ingest.js)
-- на кроці 'metrics' (фінальний крок state-machine). Це гарантує, що
-- matview оновляться рівно тоді, коли свіжі дані вже залилися —
-- незалежно від того о котрій реально стартував ingest.
--
-- RPC функції refresh_sku_metrics() / refresh_buyer_rfm() — лишаємо.
-- Видаляємо ТІЛЬКИ розклад.

DO $$
DECLARE
  jid BIGINT;
BEGIN
  SELECT jobid INTO jid FROM cron.job WHERE jobname = 'refresh-sku-metrics';
  IF jid IS NOT NULL THEN
    PERFORM cron.unschedule(jid);
    RAISE NOTICE 'Unscheduled refresh-sku-metrics (jobid=%)', jid;
  END IF;

  SELECT jobid INTO jid FROM cron.job WHERE jobname = 'refresh-buyer-rfm';
  IF jid IS NOT NULL THEN
    PERFORM cron.unschedule(jid);
    RAISE NOTICE 'Unscheduled refresh-buyer-rfm (jobid=%)', jid;
  END IF;
END $$;
