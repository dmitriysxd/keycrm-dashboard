-- Migration 047: атомарний лок проти паралельних auto-циклів.
--
-- ПРОБЛЕМА: self-chaining + зовнішні пінги (keepalive/cron) можуть стартувати
-- два auto-виклики майже одночасно. Обидва читають state, обидва вважають
-- цикл вільним, обидва пишуть у sales → блокування рядків → один падає з
-- "canceling statement due to statement timeout".
--
-- РІШЕННЯ: атомарний лок у ingest_state.lock_until. claim_ingest_lock робить
-- УМОВНИЙ UPDATE (SET lock_until=now+N WHERE lock_until IS NULL OR < now) —
-- row-lock Postgres серіалізує конкурентів, тож рівно ОДИН отримує ROW_COUNT=1
-- і право працювати; решта отримують 0 і відходять. release_ingest_lock
-- звільняє лок перед запуском наступної ланки ланцюга.

ALTER TABLE ingest_state ADD COLUMN IF NOT EXISTS lock_until timestamptz;

CREATE OR REPLACE FUNCTION claim_ingest_lock(_seconds int DEFAULT 70)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE _n int;
BEGIN
  UPDATE ingest_state
  SET lock_until = now() + make_interval(secs => _seconds)
  WHERE id = 1
    AND (lock_until IS NULL OR lock_until < now());
  GET DIAGNOSTICS _n = ROW_COUNT;
  RETURN _n > 0;
END;
$$;

GRANT EXECUTE ON FUNCTION claim_ingest_lock(int) TO authenticated, anon, service_role;

CREATE OR REPLACE FUNCTION release_ingest_lock()
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  UPDATE ingest_state SET lock_until = now() - interval '1 second' WHERE id = 1;
$$;

GRANT EXECUTE ON FUNCTION release_ingest_lock() TO authenticated, anon, service_role;
