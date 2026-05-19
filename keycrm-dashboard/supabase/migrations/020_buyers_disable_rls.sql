-- Fix: відключаємо RLS на таблицях клієнтів.
--
-- На цьому Supabase-проекті, схоже, увімкнено "Enable RLS by default for new
-- tables", тому міграція 018 створила buyers / buyer_statuses / buyer_notes з
-- RLS, але без жодної policy. Це блокує навіть service-role запис у buyers
-- (бачимо "new row violates row-level security policy for table buyers").
--
-- Доступ до цих таблиць іде ВИКЛЮЧНО з нашого бекенду через SERVICE_ROLE_KEY,
-- кінцеві користувачі не пишуть напряму — тому RLS тут не потрібна (як і на
-- skus / sales / ingest_state, де її ніколи не вмикали).

ALTER TABLE buyers          DISABLE ROW LEVEL SECURITY;
ALTER TABLE buyer_statuses  DISABLE ROW LEVEL SECURITY;
ALTER TABLE buyer_notes     DISABLE ROW LEVEL SECURITY;
