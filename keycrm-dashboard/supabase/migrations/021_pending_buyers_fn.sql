-- SQL-функція для backfill-buyers: повертає buyer_id які є в sales,
-- але ще не мають заповненої карточки в buyers (full_name пустий).
--
-- Раніше backfill-buyers.js сканував sales і buyers через HTTP, що з'їдало
-- 30+ секунд лише на підготовку → Vercel timeout 60s. Тепер один RPC-виклик
-- і вся вибірка робиться в PostgreSQL.

CREATE OR REPLACE FUNCTION pending_buyer_ids(_limit int DEFAULT 300, _force boolean DEFAULT false)
RETURNS TABLE(buyer_id bigint)
LANGUAGE sql
STABLE
AS $$
  SELECT DISTINCT s.buyer_id
  FROM sales s
  WHERE s.buyer_id IS NOT NULL
    AND (
      _force
      OR NOT EXISTS (
        SELECT 1 FROM buyers b
        WHERE b.buyer_id = s.buyer_id
          AND b.full_name IS NOT NULL
          AND btrim(b.full_name) <> ''
      )
    )
  LIMIT _limit;
$$;

-- Лічильник лишку — щоб клієнт міг знати, скільки ще треба прокачати.
CREATE OR REPLACE FUNCTION pending_buyer_ids_count(_force boolean DEFAULT false)
RETURNS bigint
LANGUAGE sql
STABLE
AS $$
  SELECT COUNT(*)::bigint FROM (
    SELECT DISTINCT s.buyer_id
    FROM sales s
    WHERE s.buyer_id IS NOT NULL
      AND (
        _force
        OR NOT EXISTS (
          SELECT 1 FROM buyers b
          WHERE b.buyer_id = s.buyer_id
            AND b.full_name IS NOT NULL
            AND btrim(b.full_name) <> ''
        )
      )
  ) t;
$$;

-- Допоміжний індекс — пришвидшує DISTINCT по великому sales.
CREATE INDEX IF NOT EXISTS sales_buyer_id_idx ON sales(buyer_id) WHERE buyer_id IS NOT NULL;
