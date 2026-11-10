-- Migration 048: збір "першого джерела" (first-touch) клієнта з сайту.
--
-- ПРОБЛЕМА: Horoshop передає в KeyCRM лише ОСТАННЄ джерело (last-touch) і не
-- вміє класти кастомні поля в картку заказу. Тож "перший дотик" (реклама, яка
-- реально привела клієнта, навіть якщо він купив не одразу) з боку KeyCRM
-- побачити неможливо.
--
-- РІШЕННЯ: власний JS на сайті запам'ятовує перше джерело в браузері (180 днів)
-- і при оформленні заказу шле {телефон, first-touch} на наш /api/first-touch.
-- Ключ зв'язку — ТЕЛЕФОН, бо він єдиний надійно присутній і в формі сайту, і в
-- картці заказу KeyCRM. Пишемо insert-only: перше джерело для телефону
-- фіксуємо один раз і більше НЕ перезаписуємо (на те воно й "перше").
--
-- Безпека: ендпоінт публічний (скрипт видно у вихідному коді сайту). Це
-- прийнятно, бо:
--   • record_first_touch приймає лише валідний UA-телефон (12 цифр, 380...),
--   • аналітика JOIN-иться лише до РЕАЛЬНИХ покупців (buyers.phone),
--     тож фейкові телефони нікуди не чіпляються і статистику не псують,
--   • cleanup_unmatched_first_touch прибирає "осиротілі" рядки.

------------------------------------------------------------
-- 1. normalize_phone: канонічний UA-формат '380XXXXXXXXX' (12 цифр).
--    Єдине джерело правди — використовується і при записі first_touch,
--    і при JOIN до buyers, щоб ключі гарантовано збігались.
------------------------------------------------------------
CREATE OR REPLACE FUNCTION normalize_phone(_raw text)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN d IS NULL OR length(d) < 9 THEN NULL
    WHEN length(d) = 12 AND left(d, 3) = '380' THEN d
    WHEN length(d) = 11 AND left(d, 2) = '80'  THEN '3' || d
    WHEN length(d) = 10 AND left(d, 1) = '0'   THEN '38' || d
    WHEN length(d) = 9                          THEN '380' || d
    ELSE '380' || right(d, 9)
  END
  FROM (SELECT regexp_replace(COALESCE(_raw, ''), '\D', '', 'g') AS d) t;
$$;

GRANT EXECUTE ON FUNCTION normalize_phone(text) TO authenticated, anon, service_role;

------------------------------------------------------------
-- 2. Таблиця first_touch. Ключ — нормалізований телефон.
--    first_* заповнюється РАЗ (перший дотик, не чіпаємо надалі).
--    last_*  оновлюється щоразу (джерело на момент останнього заказу —
--    бонус для звірки з last-touch із KeyCRM).
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS first_touch (
  phone_norm      text PRIMARY KEY,
  first_source    text,
  first_medium    text,
  first_campaign  text,
  first_content   text,
  first_term      text,
  first_referrer  text,
  first_landing   text,
  first_touch_at  timestamptz,
  last_source     text,
  last_medium     text,
  last_campaign   text,
  last_content    text,
  last_term       text,
  hits            int NOT NULL DEFAULT 1,
  raw             jsonb,
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now()
);

------------------------------------------------------------
-- Хелпери (мінімальні, щоб INSERT був читабельним).
------------------------------------------------------------
CREATE OR REPLACE FUNCTION _ft_nz(_s text)
RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT NULLIF(btrim(COALESCE(_s, '')), '');
$$;

-- мілісекунди epoch (рядком) → timestamptz; сміття → now()
CREATE OR REPLACE FUNCTION _ft_ms(_s text)
RETURNS timestamptz LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE
    WHEN _s ~ '^\d{10,16}$' THEN to_timestamp(_s::bigint / 1000.0)
    ELSE now()
  END;
$$;

------------------------------------------------------------
-- 3. record_first_touch: атомарний запис.
--    Повертає нормалізований телефон (NULL, якщо телефон невалідний —
--    тоді нічого не пишемо).
------------------------------------------------------------
CREATE OR REPLACE FUNCTION record_first_touch(
  _phone text,
  _ft    jsonb,
  _lt    jsonb,
  _raw   jsonb
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  _pn text;
BEGIN
  _pn := normalize_phone(_phone);
  IF _pn IS NULL THEN
    RETURN NULL;
  END IF;

  INSERT INTO first_touch (
    phone_norm,
    first_source, first_medium, first_campaign, first_content, first_term,
    first_referrer, first_landing, first_touch_at,
    last_source, last_medium, last_campaign, last_content, last_term,
    hits, raw, created_at, updated_at
  ) VALUES (
    _pn,
    _ft_nz(_ft->>'s'),  _ft_nz(_ft->>'m'),  _ft_nz(_ft->>'c'),
    _ft_nz(_ft->>'ct'), _ft_nz(_ft->>'t'),
    _ft_nz(_ft->>'ref'), _ft_nz(_ft->>'lp'), _ft_ms(_ft->>'ts'),
    _ft_nz(_lt->>'s'),  _ft_nz(_lt->>'m'),  _ft_nz(_lt->>'c'),
    _ft_nz(_lt->>'ct'), _ft_nz(_lt->>'t'),
    1, _raw, now(), now()
  )
  ON CONFLICT (phone_norm) DO UPDATE SET
    -- first_* НЕ чіпаємо — це "перший дотик", він фіксується один раз.
    last_source   = COALESCE(_ft_nz(_lt->>'s'),  first_touch.last_source),
    last_medium   = COALESCE(_ft_nz(_lt->>'m'),  first_touch.last_medium),
    last_campaign = COALESCE(_ft_nz(_lt->>'c'),  first_touch.last_campaign),
    last_content  = COALESCE(_ft_nz(_lt->>'ct'), first_touch.last_content),
    last_term     = COALESCE(_ft_nz(_lt->>'t'),  first_touch.last_term),
    hits          = first_touch.hits + 1,
    raw           = _raw,
    updated_at    = now();

  RETURN _pn;
END;
$$;

GRANT EXECUTE ON FUNCTION record_first_touch(text, jsonb, jsonb, jsonb)
  TO authenticated, anon, service_role;

------------------------------------------------------------
-- 4. v_buyer_first_touch: зручний JOIN first-touch до реального покупця
--    (для майбутньої сторінки "Канали"). Телефон нормалізуємо з обох боків.
------------------------------------------------------------
CREATE OR REPLACE VIEW v_buyer_first_touch AS
  SELECT
    b.buyer_id,
    b.full_name,
    b.phone,
    b.is_wholesale,
    ft.first_source,
    ft.first_medium,
    ft.first_campaign,
    ft.first_content,
    ft.first_term,
    ft.first_touch_at,
    ft.last_source,
    ft.hits
  FROM buyers b
  JOIN first_touch ft ON ft.phone_norm = normalize_phone(b.phone);

GRANT SELECT ON v_buyer_first_touch TO authenticated, anon, service_role;

------------------------------------------------------------
-- 5. cleanup_unmatched_first_touch: прибрати "осиротілі" записи (старші за
--    _days днів і без відповідного покупця) — захист від спаму/сміття.
------------------------------------------------------------
CREATE OR REPLACE FUNCTION cleanup_unmatched_first_touch(_days int DEFAULT 90)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  _n bigint;
BEGIN
  WITH del AS (
    DELETE FROM first_touch ft
    WHERE ft.created_at < now() - make_interval(days => _days)
      AND NOT EXISTS (
        SELECT 1 FROM buyers b WHERE normalize_phone(b.phone) = ft.phone_norm
      )
    RETURNING 1
  )
  SELECT COUNT(*)::bigint INTO _n FROM del;
  RETURN _n;
END;
$$;

GRANT EXECUTE ON FUNCTION cleanup_unmatched_first_touch(int)
  TO authenticated, anon, service_role;
