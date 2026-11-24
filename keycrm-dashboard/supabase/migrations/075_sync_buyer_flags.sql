-- Migration 075: bulk-синхронізація прапорів клієнта (опт/розниця) з KeyCRM.
--
-- Щоденна звірка: ingest оновлював buyers лише коли клієнт робив новий заказ,
-- тож ручна зміна прапора «Опт» у KeyCRM на старому клієнті не підхоплювалась.
-- Крок sync_buyer_flags (ingest.js) тягне СПИСОК клієнтів KeyCRM з custom_fields
-- і викликає цю функцію пачками. Оновлюємо ЛИШЕ наявні картки (UPDATE…WHERE),
-- нові порожні не створюємо. Тригер trg_buyers_client_type перераховує
-- client_type з custom_fields_raw (у т.ч. пониження до «роздріб»), а
-- is_wholesale ставимо тут у ОБИДВА боки. Безпечно: сюди потрапляють лише
-- клієнти, у яких KeyCRM реально повернув непорожні custom_fields (Node-фільтр).

CREATE OR REPLACE FUNCTION sync_buyer_flags(_rows jsonb)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE _n integer;
BEGIN
  WITH src AS (
    SELECT (e->>'buyer_id')::bigint    AS bid,
           (e->>'is_wholesale')::boolean AS iw,
           (e->'cf')                    AS cf
    FROM jsonb_array_elements(_rows) e
  ),
  upd AS (
    UPDATE buyers b
    SET is_wholesale      = s.iw,
        custom_fields_raw = s.cf,
        last_synced_at    = now()
    FROM src s
    WHERE b.buyer_id = s.bid
      AND (b.is_wholesale IS DISTINCT FROM s.iw
           OR b.custom_fields_raw IS DISTINCT FROM s.cf)
    RETURNING 1
  )
  SELECT count(*) INTO _n FROM upd;
  RETURN COALESCE(_n, 0);
END;
$$;

GRANT EXECUTE ON FUNCTION sync_buyer_flags(jsonb) TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
