-- SQL-функція для merge-UPSERT покупця.
--
-- Проблема: щоденний ingest при обробці заказів робив звичайний UPSERT з
-- order.buyer об'єкта, але KeyCRM в order повертає мінімальний buyer
-- (id, іноді ім'я, без custom_fields). Тому old full_name="Іван Петров"
-- + is_wholesale=true перезаписувались на null/false наступного ранку.
--
-- Тепер: COALESCE на кожному полі — порожнє нове значення НЕ затирає
-- існуюче. Першу заливку від backfill або з повним buyer об'єктом ми
-- зберігаємо назавжди.

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
    _buyer_id, _full_name, _phone, _email, COALESCE(_is_wholesale, FALSE),
    _custom_fields_raw, _first_seen_at, NOW()
  )
  ON CONFLICT (buyer_id) DO UPDATE SET
    -- Нове значення лише якщо воно непорожнє; інакше залишаємо старе.
    full_name        = COALESCE(NULLIF(btrim(EXCLUDED.full_name), ''), buyers.full_name),
    phone            = COALESCE(NULLIF(btrim(EXCLUDED.phone), ''),     buyers.phone),
    email            = COALESCE(NULLIF(btrim(EXCLUDED.email), ''),     buyers.email),
    -- is_wholesale: оновлюємо тільки якщо в нас були custom_fields (нове
    -- значення достовірне). Якщо custom_fields_raw в EXCLUDED null/empty —
    -- значення в order'і минімальне, ми йому не довіряємо і не чіпаємо
    -- наявний is_wholesale.
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
    first_seen_at    = LEAST(buyers.first_seen_at, EXCLUDED.first_seen_at),
    last_synced_at   = NOW();
END;
$$;

-- Колонка під реальну суму замовлення з урахуванням знижок.
-- Заповнюється для всіх рядків одного order_id однаково (KeyCRM скидка на
-- весь заказ, а не на позицію). У buyer_rfm брати MAX(order_grand_total)
-- per order_id, інакше можемо порахувати MIN/AVG випадково.
ALTER TABLE sales ADD COLUMN IF NOT EXISTS order_grand_total NUMERIC(14,2);
ALTER TABLE sales ADD COLUMN IF NOT EXISTS order_discount    NUMERIC(14,2);
