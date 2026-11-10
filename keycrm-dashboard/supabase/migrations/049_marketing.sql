-- Migration 049: маркетингова аналітика (канали / витрати / сегменти).
--
-- Додає:
--   1. UTM-колонки в sales (last-touch з KeyCRM «UTM мітки»).
--   2. channel_costs — ручні витрати по каналах за місяць (для ROAS).
--   3. buyers.client_type — опт / роздріб / системні (з custom-field CT_1005),
--      з тригером авто-підтримки, щоб завжди був коректним.

------------------------------------------------------------
-- 1. UTM last-touch у sales.
--    Заповнюються ingest-ом із заказу KeyCRM. Для історичних заказів —
--    NULL, поки заказ не оновиться (або поки не прогониш backfill).
------------------------------------------------------------
ALTER TABLE sales
  ADD COLUMN IF NOT EXISTS utm_source   TEXT,
  ADD COLUMN IF NOT EXISTS utm_medium   TEXT,
  ADD COLUMN IF NOT EXISTS utm_campaign TEXT,
  ADD COLUMN IF NOT EXISTS utm_content  TEXT,
  ADD COLUMN IF NOT EXISTS utm_term     TEXT;

CREATE INDEX IF NOT EXISTS sales_utm_source_idx ON sales(utm_source) WHERE utm_source IS NOT NULL;

------------------------------------------------------------
-- 2. channel_costs — витрати на канал за місяць (вводяться вручну з дашборда).
--    period у форматі 'YYYY-MM'. Ключ (channel, period).
------------------------------------------------------------
CREATE TABLE IF NOT EXISTS channel_costs (
  channel    TEXT NOT NULL,
  period     TEXT NOT NULL,                 -- 'YYYY-MM'
  amount     NUMERIC(14,2) NOT NULL DEFAULT 0,
  currency   TEXT NOT NULL DEFAULT 'UAH',
  note       TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (channel, period)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON channel_costs TO authenticated, anon, service_role;

------------------------------------------------------------
-- 3. client_type: опт / роздріб / системні.
--    Джерело — custom-field CT_1005 «Опт/Роздріб» у картці покупця
--    (зберігається у buyers.custom_fields_raw як масив об'єктів).
------------------------------------------------------------
ALTER TABLE buyers ADD COLUMN IF NOT EXISTS client_type TEXT;

-- Повертає 'опт' | 'системні' | 'роздріб', або NULL якщо інформації нема
-- (custom_fields порожні/відсутні — тоді не затираємо попереднє значення).
CREATE OR REPLACE FUNCTION client_type_from_cf(_cf jsonb)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  _val text;
BEGIN
  IF _cf IS NULL OR jsonb_typeof(_cf) <> 'array' THEN
    RETURN NULL;
  END IF;

  -- Знаходимо елемент CT_1005 і витягуємо його значення (масив або скаляр).
  SELECT CASE
           WHEN jsonb_typeof(elem->'value') = 'array'
             THEN (SELECT string_agg(x, ' ') FROM jsonb_array_elements_text(elem->'value') x)
           ELSE elem->>'value'
         END
    INTO _val
  FROM jsonb_array_elements(_cf) elem
  WHERE elem->>'uuid' = 'CT_1005'
  LIMIT 1;

  IF _val IS NULL OR btrim(_val) = '' THEN
    RETURN NULL;
  END IF;

  _val := lower(_val);
  IF _val LIKE '%опт%'     THEN RETURN 'опт';      END IF;
  IF _val LIKE '%системн%' THEN RETURN 'системні'; END IF;
  RETURN 'роздріб';
END;
$$;

GRANT EXECUTE ON FUNCTION client_type_from_cf(jsonb) TO authenticated, anon, service_role;

-- Тригер: при кожному insert/update buyers перераховуємо client_type з
-- custom_fields_raw. Якщо custom-fields порожні (computed = NULL) — лишаємо
-- вже відомий тип (не деградуємо до невизначеного), як і is_wholesale.
CREATE OR REPLACE FUNCTION buyers_set_client_type()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.client_type := COALESCE(client_type_from_cf(NEW.custom_fields_raw), NEW.client_type);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_buyers_client_type ON buyers;
CREATE TRIGGER trg_buyers_client_type
  BEFORE INSERT OR UPDATE ON buyers
  FOR EACH ROW
  EXECUTE FUNCTION buyers_set_client_type();

-- Бекфіл існуючих покупців.
UPDATE buyers
  SET client_type = COALESCE(client_type_from_cf(custom_fields_raw), client_type)
  WHERE custom_fields_raw IS NOT NULL;

CREATE INDEX IF NOT EXISTS buyers_client_type_idx ON buyers(client_type) WHERE client_type IS NOT NULL;
