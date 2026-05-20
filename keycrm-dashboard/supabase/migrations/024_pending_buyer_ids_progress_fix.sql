-- Fix: pending_buyer_ids з force=true виявився беззупинним.
--
-- Стара функція: SELECT DISTINCT … LIMIT 60 без ORDER BY → PostgreSQL
-- повертав ОДНІ Й ТІ САМІ 60 buyer_id кожен виклик. З _force=true,
-- backfill-buyers вічно перепроцесував одну й ту ж сотню клієнтів, а
-- решта 3300 ніколи не торкалась.
--
-- Виправлено:
--   1) ORDER BY last_synced_at NULLS FIRST — гарантує прогрес (першими
--      йдуть найдавніші, в кінці — щойно оновлені).
--   2) З _force=true виключаємо buyers, синхронізованих в останні 30 хв
--      — щоб лічильник pending дійшов до 0 і цикл коректно завершився.

CREATE OR REPLACE FUNCTION pending_buyer_ids(_limit int DEFAULT 300, _force boolean DEFAULT false)
RETURNS TABLE(buyer_id bigint)
LANGUAGE sql
STABLE
AS $$
  SELECT s.buyer_id
  FROM (SELECT DISTINCT buyer_id FROM sales WHERE buyer_id IS NOT NULL) s
  LEFT JOIN buyers b ON b.buyer_id = s.buyer_id
  WHERE
    CASE
      WHEN _force THEN (b.last_synced_at IS NULL OR b.last_synced_at < NOW() - INTERVAL '30 minutes')
      ELSE (b.buyer_id IS NULL OR b.full_name IS NULL OR btrim(b.full_name) = '')
    END
  ORDER BY b.last_synced_at NULLS FIRST
  LIMIT _limit;
$$;

CREATE OR REPLACE FUNCTION pending_buyer_ids_count(_force boolean DEFAULT false)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT COUNT(*)::bigint
  FROM (SELECT DISTINCT buyer_id FROM sales WHERE buyer_id IS NOT NULL) s
  LEFT JOIN buyers b ON b.buyer_id = s.buyer_id
  WHERE
    CASE
      WHEN _force THEN (b.last_synced_at IS NULL OR b.last_synced_at < NOW() - INTERVAL '30 minutes')
      ELSE (b.buyer_id IS NULL OR b.full_name IS NULL OR btrim(b.full_name) = '')
    END;
$$;
