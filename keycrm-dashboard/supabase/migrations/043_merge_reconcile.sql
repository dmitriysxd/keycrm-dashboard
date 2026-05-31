-- Migration 043: автоматичне розпізнавання об'єднань покупців (merge) в KeyCRM
--
-- ПРОБЛЕМА (повідомлено юзером 2026-05-31, кейс "Лінецька Олена"):
-- У KeyCRM покупець робить новий заказ під коротким іменем + телефон.
-- Система бачить дубль (той самий телефон) і пропонує об'єднати. Після
-- об'єднання KeyCRM:
--   • лишає ОДИН профіль-survivor (напр. buyer_id=4299),
--   • ВИДАЛЯЄ старий профіль (напр. buyer_id=4134),
--   • перепривʼязує ВСІ замовлення старого на survivor.
--
-- Наша система цього не розуміла:
--   1. backfill-buyers робив /buyer/4134 → 404 → ставив full_name
--      "(видалено в KeyCRM)", але 9 старих замовлень так і лишались
--      висіти в sales на мертвому buyer_id=4134 (orphan sales).
--   2. survivor 4299 синхронізувався ДО об'єднання (порожні custom_fields),
--      тому is_wholesale=false. А pending_buyer_ids (міграція 034) його
--      більше не чіпає, бо full_name вже заповнений. Прапорець "Опт"
--      залишався втраченим назавжди.
--
-- РІШЕННЯ: крок reconcile в ingest.js (step=reconcile, + в авто-циклі):
--   • знаходить "мертві" профілі з orphan-замовленнями,
--   • перечитує їх замовлення з KeyCRM /order/{id} → знаходить survivor,
--   • UPDATE sales SET buyer_id = survivor,
--   • перечитує картку survivor /buyer/{id} → відновлює опт-прапорець/ім'я/email,
--   • видаляє порожній мертвий профіль.
--
-- Ця міграція додає SQL-помічники для цього кроку.

------------------------------------------------------------
-- 1. merged_buyer_candidates: "мертві" профілі (видалені в KeyCRM),
--    які ще мають замовлення в sales — їх треба перепривʼязати.
--    Сортуємо за кількістю замовлень спадно (спочатку найбільші).
------------------------------------------------------------
CREATE OR REPLACE FUNCTION merged_buyer_candidates(_limit int DEFAULT 25)
RETURNS TABLE(buyer_id bigint, orders_count bigint)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT b.buyer_id, COUNT(DISTINCT s.order_id)::bigint AS orders_count
  FROM buyers b
  JOIN sales s ON s.buyer_id = b.buyer_id
  WHERE b.full_name = '(видалено в KeyCRM)'
  GROUP BY b.buyer_id
  ORDER BY orders_count DESC
  LIMIT _limit;
$$;

GRANT EXECUTE ON FUNCTION merged_buyer_candidates(int) TO authenticated, anon, service_role;

------------------------------------------------------------
-- 2. cleanup_orphan_deleted_buyers: прибрати порожні "мертві" профілі —
--    позначені "(видалено в KeyCRM)" і вже БЕЗ жодного замовлення в sales
--    (всі замовлення перепривʼязані на survivor). Повертає кількість.
------------------------------------------------------------
CREATE OR REPLACE FUNCTION cleanup_orphan_deleted_buyers()
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  _deleted bigint;
BEGIN
  WITH del AS (
    DELETE FROM buyers b
    WHERE b.full_name = '(видалено в KeyCRM)'
      AND NOT EXISTS (SELECT 1 FROM sales s WHERE s.buyer_id = b.buyer_id)
    RETURNING b.buyer_id
  )
  SELECT COUNT(*)::bigint INTO _deleted FROM del;
  RETURN _deleted;
END;
$$;

GRANT EXECUTE ON FUNCTION cleanup_orphan_deleted_buyers() TO authenticated, anon, service_role;

------------------------------------------------------------
-- 2b. sales_orphan_buyer_ids: "бездомні" buyer_id — присутні в sales,
--     але відсутні в buyers. Виникають, коли після merge замовлення
--     переїхали на survivor, а сам survivor-профіль ingest не створив.
--     reconcile тягне їх з KeyCRM і створює картку.
------------------------------------------------------------
CREATE OR REPLACE FUNCTION sales_orphan_buyer_ids(_limit int DEFAULT 60)
RETURNS TABLE(buyer_id bigint)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT DISTINCT s.buyer_id
  FROM sales s
  LEFT JOIN buyers b ON b.buyer_id = s.buyer_id
  WHERE s.buyer_id IS NOT NULL
    AND b.buyer_id IS NULL
  LIMIT _limit;
$$;

GRANT EXECUTE ON FUNCTION sales_orphan_buyer_ids(int) TO authenticated, anon, service_role;

------------------------------------------------------------
-- 3. upsert_buyer_merge: не "усихати" повне ім'я.
--    Раніше на UPDATE будь-яке непорожнє EXCLUDED.full_name перетирало
--    наявне — тому новий заказ із коротким іменем ("Лінецька Олена")
--    затирав повне ("Лінецька Олена Павлівна"). Тепер зберігаємо ДОВШЕ
--    (більш повне) ім'я: коротке ім'я із форми замовлення не вкорочує
--    вже відоме повне.
--    Решта логіки (is_wholesale / custom_fields захист від порожніх)
--    лишається як у міграції 033.
------------------------------------------------------------
CREATE OR REPLACE FUNCTION upsert_buyer_merge(
  _buyer_id BIGINT,
  _full_name TEXT,
  _phone TEXT,
  _email TEXT,
  _is_wholesale BOOLEAN,
  _custom_fields_raw JSONB,
  _first_seen_at TIMESTAMPTZ
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO buyers (
    buyer_id, full_name, phone, email, is_wholesale,
    custom_fields_raw, first_seen_at, last_synced_at
  ) VALUES (
    _buyer_id, _full_name, _phone, _email,
    CASE
      WHEN _custom_fields_raw IS NULL
        OR _custom_fields_raw::text IN ('null', '[]', '{}') THEN NULL
      ELSE COALESCE(_is_wholesale, FALSE)
    END,
    _custom_fields_raw, _first_seen_at, NOW()
  )
  ON CONFLICT (buyer_id) DO UPDATE SET
    -- Ім'я: беремо ДОВШЕ (повніше). Порожнє EXCLUDED не чіпає наявне.
    full_name = CASE
      WHEN btrim(COALESCE(EXCLUDED.full_name, '')) = '' THEN buyers.full_name
      WHEN buyers.full_name IS NULL OR btrim(buyers.full_name) = '' THEN EXCLUDED.full_name
      -- "(видалено в KeyCRM)" ніколи не має перемагати реальне ім'я
      WHEN EXCLUDED.full_name = '(видалено в KeyCRM)' THEN buyers.full_name
      WHEN char_length(EXCLUDED.full_name) >= char_length(buyers.full_name) THEN EXCLUDED.full_name
      ELSE buyers.full_name
    END,
    phone            = COALESCE(NULLIF(btrim(EXCLUDED.phone), ''),     buyers.phone),
    email            = COALESCE(NULLIF(btrim(EXCLUDED.email), ''),     buyers.email),
    is_wholesale     = CASE
      WHEN EXCLUDED.custom_fields_raw IS NOT NULL
       AND EXCLUDED.custom_fields_raw::text NOT IN ('null', '[]', '{}')
      THEN EXCLUDED.is_wholesale
      ELSE buyers.is_wholesale
    END,
    custom_fields_raw = CASE
      WHEN EXCLUDED.custom_fields_raw IS NOT NULL
       AND EXCLUDED.custom_fields_raw::text NOT IN ('null', '[]', '{}')
      THEN EXCLUDED.custom_fields_raw
      ELSE buyers.custom_fields_raw
    END,
    first_seen_at    = LEAST(
                         COALESCE(buyers.first_seen_at, EXCLUDED.first_seen_at),
                         COALESCE(EXCLUDED.first_seen_at, buyers.first_seen_at)
                       ),
    last_synced_at   = NOW();
END;
$$;
