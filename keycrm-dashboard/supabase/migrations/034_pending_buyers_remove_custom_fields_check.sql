-- Migration 034: фікс pending_buyer_ids — прибрати false positive по custom_fields_raw
--
-- ПРОБЛЕМА (виявлено юзером 2026-05-22):
-- Health-check показав pending_backfill=2671, але SQL показав що
-- synced_today=3433 з 3435 — тобто всі клієнти реально оновлені сьогодні.
--
-- Корінь: міграція 033 додала умову "custom_fields_raw IS NULL"
-- в pending_buyer_ids. Це створило false positive — 2671 РОЗДРІБНИХ
-- клієнтів в KeyCRM просто не мають заповненого поля "Опт/Роздріб"
-- (custom_field CT_1005). KeyCRM повертає для них custom_fields=[]
-- або null, ми зберігаємо null. На наступному виклику pending_buyer_ids
-- знов вважає їх pending, на нескінченно.
--
-- РЕЗУЛЬТАТ: квота 1200 клієнтів/день з'їдалась на тих самих 2671
-- роздрібних, замість обходу всіх 3435.
--
-- ВИПРАВЛЕННЯ: для force=false ловимо тільки реально нових клієнтів
-- (без full_name — ingest нічого крім id не дав).
-- Для force=true все працює правильно: ротація по last_synced_at.

CREATE OR REPLACE FUNCTION pending_buyer_ids(_limit int DEFAULT 300, _force boolean DEFAULT false)
RETURNS TABLE(buyer_id bigint)
LANGUAGE sql STABLE
AS $$
  SELECT s.buyer_id
  FROM (SELECT DISTINCT buyer_id FROM sales WHERE buyer_id IS NOT NULL) s
  LEFT JOIN buyers b ON b.buyer_id = s.buyer_id
  WHERE
    CASE
      -- force=true: ротація — беремо всіх з last_synced_at старшим за 2 години
      WHEN _force THEN (b.last_synced_at IS NULL OR b.last_synced_at < NOW() - INTERVAL '2 hours')
      -- force=false: тільки реально нові клієнти (без full_name).
      -- НЕ перевіряємо custom_fields_raw — багато клієнтів його не мають
      -- (роздріб без явної мітки опт/роздріб у KeyCRM).
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
