-- Fix: 30-хвилинне вікно для force=true виявилось закоротким.
--
-- Повний прохід backfill-buyers для 3400+ клієнтів займає ~40 хв. Buyers,
-- оброблені на початку циклу, ставали "стале" знову через 30 хв і
-- знову потрапляли в pending. Лічильник гойдався навколо 700 і не падав.
--
-- Розширюємо вікно до 2 годин — це з запасом перекриває один прохід і
-- не дозволяє переплутати "оце щойно зробили" з "давно вже не оновлювали".

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
      WHEN _force THEN (b.last_synced_at IS NULL OR b.last_synced_at < NOW() - INTERVAL '2 hours')
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
      WHEN _force THEN (b.last_synced_at IS NULL OR b.last_synced_at < NOW() - INTERVAL '2 hours')
      ELSE (b.buyer_id IS NULL OR b.full_name IS NULL OR btrim(b.full_name) = '')
    END;
$$;
