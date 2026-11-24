-- Migration 076: pending_buyer_ids таймаутив на великому sales
-- («canceling statement due to statement timeout»). DISTINCT buyer_id по всьому
-- sales — важкий скан. Додаємо індекс по buyer_id і піднімаємо statement_timeout
-- самих функцій (щоб не рубались дефолтним лімітом ролі API).

CREATE INDEX IF NOT EXISTS sales_buyer_id_idx ON sales(buyer_id);

CREATE OR REPLACE FUNCTION pending_buyer_ids(_limit int DEFAULT 300, _force boolean DEFAULT false)
RETURNS TABLE(buyer_id bigint)
LANGUAGE sql STABLE
SET statement_timeout = '55000'
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
      )
    END
  ORDER BY b.last_synced_at NULLS FIRST
  LIMIT _limit;
$$;

CREATE OR REPLACE FUNCTION pending_buyer_ids_count(_force boolean DEFAULT false)
RETURNS bigint
LANGUAGE sql STABLE
SET statement_timeout = '55000'
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
      )
    END;
$$;

NOTIFY pgrst, 'reload schema';
