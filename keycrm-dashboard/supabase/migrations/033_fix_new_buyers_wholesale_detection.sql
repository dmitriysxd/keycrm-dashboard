-- Migration 033: фікс автообновлення опт-клієнтів
--
-- ПРОБЛЕМА (повідомлено юзером 2026-05-21):
-- Нові оптові клієнти з'являються в KeyCRM, але не показуються на сторінці
-- "Опт-клієнти" в дашборді.
--
-- КОРІНЬ:
-- 1. Щоденний ingest тягне /order з include=buyer, але KeyCRM в order.buyer
--    повертає тільки базові поля (id, full_name, phone). Поля custom_fields
--    немає — а саме там зберігається ознака "Опт/Роздріб" (UUID CT_1005).
-- 2. upsert_buyer_merge при INSERT нового buyer без custom_fields ставив
--    is_wholesale=FALSE за замовчуванням (COALESCE(_is_wholesale, FALSE)).
--    На UPDATE логіка правильна (зберігаємо існуюче), на INSERT — ні.
-- 3. backfill-buyers (який тягне повний /buyer/{id} з custom_fields)
--    шукав клієнтів через pending_buyer_ids() — але умова там
--    "немає full_name". Нові клієнти ВЖЕ мають full_name (з order.buyer),
--    тому backfill їх пропускав.
--
-- РЕЗУЛЬТАТ: нові клієнти лишались з is_wholesale=FALSE назавжди.
--
-- ВИПРАВЛЕННЯ:
-- (а) upsert_buyer_merge: на INSERT ставимо is_wholesale=NULL коли немає
--     достовірних даних (custom_fields_raw порожній). Це сигнал
--     "ще не перевіряли". В UI/API трактуємо NULL = ще не визначено.
-- (б) pending_buyer_ids: додаємо умову "custom_fields_raw IS NULL"
--     щоб backfill-buyers підхоплював саме таких клієнтів.

------------------------------------------------------------
-- 1. Оновлюємо upsert_buyer_merge — NULL для is_wholesale при INSERT
--    якщо немає достовірних custom_fields.
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
    -- На INSERT: якщо custom_fields порожні → NULL (ще не перевіряли).
    -- Якщо custom_fields передані → довіряємо переданому значенню.
    CASE
      WHEN _custom_fields_raw IS NULL
        OR _custom_fields_raw::text IN ('null', '[]', '{}') THEN NULL
      ELSE COALESCE(_is_wholesale, FALSE)
    END,
    _custom_fields_raw, _first_seen_at, NOW()
  )
  ON CONFLICT (buyer_id) DO UPDATE SET
    full_name        = COALESCE(NULLIF(btrim(EXCLUDED.full_name), ''), buyers.full_name),
    phone            = COALESCE(NULLIF(btrim(EXCLUDED.phone), ''),     buyers.phone),
    email            = COALESCE(NULLIF(btrim(EXCLUDED.email), ''),     buyers.email),
    is_wholesale     = CASE
      WHEN EXCLUDED.custom_fields_raw IS NOT NULL
       AND EXCLUDED.custom_fields_raw::text NOT IN ('null', '[]', '{}')
      THEN EXCLUDED.is_wholesale
      ELSE buyers.is_wholesale  -- порожні custom_fields → не торкаємо
    END,
    custom_fields_raw = CASE
      WHEN EXCLUDED.custom_fields_raw IS NOT NULL
       AND EXCLUDED.custom_fields_raw::text NOT IN ('null', '[]', '{}')
      THEN EXCLUDED.custom_fields_raw
      ELSE buyers.custom_fields_raw
    END,
    first_seen_at    = LEAST(buyers.first_seen_at, EXCLUDED.first_seen_at),
    last_synced_at   = NOW();
END;
$$;

------------------------------------------------------------
-- 2. Оновлюємо pending_buyer_ids: додаємо "немає custom_fields"
--    як умову для бекфіл.
------------------------------------------------------------
CREATE OR REPLACE FUNCTION pending_buyer_ids(_limit int DEFAULT 300, _force boolean DEFAULT false)
RETURNS TABLE(buyer_id bigint)
LANGUAGE sql STABLE
AS $$
  SELECT s.buyer_id
  FROM (SELECT DISTINCT buyer_id FROM sales WHERE buyer_id IS NOT NULL) s
  LEFT JOIN buyers b ON b.buyer_id = s.buyer_id
  WHERE
    CASE
      WHEN _force THEN (b.last_synced_at IS NULL OR b.last_synced_at < NOW() - INTERVAL '2 hours')
      ELSE (
        b.buyer_id IS NULL
        OR b.full_name IS NULL
        OR btrim(b.full_name) = ''
        -- НОВЕ: клієнт ніколи не отримував /buyer/{id} перевірку → custom_fields пусті,
        -- тому ми не знаємо чи він "опт"
        OR b.custom_fields_raw IS NULL
        OR b.custom_fields_raw::text IN ('null', '[]', '{}')
      )
    END
  ORDER BY b.last_synced_at NULLS FIRST
  LIMIT _limit;
$$;

CREATE OR REPLACE FUNCTION pending_buyer_ids_count(_force boolean DEFAULT false)
RETURNS bigint
LANGUAGE sql STABLE
AS $$
  SELECT COUNT(*)::bigint
  FROM (SELECT DISTINCT buyer_id FROM sales WHERE buyer_id IS NOT NULL) s
  LEFT JOIN buyers b ON b.buyer_id = s.buyer_id
  WHERE
    CASE
      WHEN _force THEN (b.last_synced_at IS NULL OR b.last_synced_at < NOW() - INTERVAL '2 hours')
      ELSE (
        b.buyer_id IS NULL
        OR b.full_name IS NULL
        OR btrim(b.full_name) = ''
        OR b.custom_fields_raw IS NULL
        OR b.custom_fields_raw::text IN ('null', '[]', '{}')
      )
    END;
$$;

------------------------------------------------------------
-- 3. Допоміжна функція для health-check: показати скільки клієнтів
--    мають is_wholesale=NULL (ще не перевірені)
------------------------------------------------------------
CREATE OR REPLACE FUNCTION buyers_wholesale_status_breakdown()
RETURNS TABLE(
  total_buyers      BIGINT,
  wholesale_true    BIGINT,
  wholesale_false   BIGINT,
  wholesale_unknown BIGINT,
  pending_backfill  BIGINT
)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT
    (SELECT COUNT(*) FROM buyers)::BIGINT,
    (SELECT COUNT(*) FROM buyers WHERE is_wholesale = TRUE)::BIGINT,
    (SELECT COUNT(*) FROM buyers WHERE is_wholesale = FALSE)::BIGINT,
    (SELECT COUNT(*) FROM buyers WHERE is_wholesale IS NULL)::BIGINT,
    pending_buyer_ids_count(FALSE)::BIGINT;
$$;

GRANT EXECUTE ON FUNCTION buyers_wholesale_status_breakdown() TO authenticated, anon, service_role;
