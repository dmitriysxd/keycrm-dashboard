-- Migration 074: вимірювач влучності рекомендацій + сильніша поправка на
-- популярність у рекомендателі.
--
-- 1) reorder_recs_log — що і кому порекомендували (пишемо при «Скопіювати
--    підбірку» = менеджер реально відправляє).
-- 2) reorder_recs_hitrate() — скільки з надісланого клієнт потім купив.
-- 3) reorder_recs — підсилена нормалізація: /pow(buyers,0.9) замість /sqrt
--    (0.5) + поріг twins>=2 (не рекомендувати те, що взяв лише 1 двійник).

CREATE TABLE IF NOT EXISTS reorder_recs_log (
  id         BIGSERIAL PRIMARY KEY,
  buyer_id   BIGINT      NOT NULL,
  product_id BIGINT      NOT NULL,
  shown_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS reorder_recs_log_bp_idx ON reorder_recs_log(buyer_id, product_id);

-- Влучність: серед «дозрілих» рекомендацій (старше _min_age_days) — частка,
-- де клієнт потім купив саме цей товар.
CREATE OR REPLACE FUNCTION reorder_recs_hitrate(_min_age_days int DEFAULT 3)
RETURNS TABLE(sent bigint, bought bigint, hitrate_pct numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '15000'
AS $$
  WITH logg AS (
    SELECT buyer_id, product_id, min(shown_at) AS shown_at
    FROM reorder_recs_log
    GROUP BY buyer_id, product_id
  ),
  mature AS (
    SELECT * FROM logg WHERE shown_at < now() - make_interval(days => _min_age_days)
  ),
  hit AS (
    SELECT m.buyer_id, m.product_id
    FROM mature m
    WHERE EXISTS (
      SELECT 1 FROM sales s
      WHERE s.buyer_id = m.buyer_id AND s.product_id = m.product_id
        AND s.ordered_at > m.shown_at
        AND coalesce(s.order_status,'') <> ALL (_excluded_statuses())
    )
  )
  SELECT (SELECT count(*) FROM mature)::bigint,
         (SELECT count(*) FROM hit)::bigint,
         round(100.0 * (SELECT count(*) FROM hit) / nullif((SELECT count(*) FROM mature), 0), 1);
$$;

-- Рекомендатель — сильніша поправка на популярність + поріг двійників.
CREATE OR REPLACE FUNCTION reorder_recs(_buyer_id bigint, _new_days int DEFAULT 45, _limit int DEFAULT 15)
RETURNS TABLE(product_id bigint, article text, name text, category text,
              price numeric, stock numeric, twins bigint, score numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH op AS (
    SELECT DISTINCT s.buyer_id, s.product_id
    FROM sales s
    JOIN buyers b ON b.buyer_id = s.buyer_id AND b.client_type = 'опт'
    WHERE s.product_id IS NOT NULL
      AND coalesce(s.order_status,'') <> ALL (_excluded_statuses())
      AND coalesce(s.order_status,'') NOT LIKE 'Об%єднання замовлень'
      AND coalesce(s.source_name,'') <> 'gamijok.com.ua'
  ),
  me AS (SELECT product_id FROM op WHERE buyer_id = _buyer_id),
  me_size AS (SELECT greatest(count(*), 1)::numeric AS n FROM me),
  sizes AS (SELECT buyer_id, count(*)::numeric AS n FROM op GROUP BY buyer_id),
  sim AS (
    SELECT o.buyer_id,
           count(*)::numeric / sqrt((SELECT n FROM me_size) * sz.n) AS cosine
    FROM op o
    JOIN sizes sz ON sz.buyer_id = o.buyer_id
    WHERE o.buyer_id <> _buyer_id
      AND o.product_id IN (SELECT product_id FROM me)
    GROUP BY o.buyer_id, sz.n
  ),
  nb AS (SELECT buyer_id, cosine FROM sim ORDER BY cosine DESC LIMIT 60),
  firstsale AS (
    SELECT product_id, min(ordered_at) AS first_at
    FROM sales WHERE product_id IS NOT NULL GROUP BY product_id
  ),
  pop AS (SELECT product_id, count(*)::numeric AS buyers FROM op GROUP BY product_id),
  cand AS (
    SELECT o.product_id, sum(nb.cosine) AS raw_score, count(DISTINCT o.buyer_id) AS twins
    FROM op o
    JOIN nb ON nb.buyer_id = o.buyer_id
    JOIN firstsale f ON f.product_id = o.product_id
    LEFT JOIN me ON me.product_id = o.product_id
    WHERE me.product_id IS NULL
      AND f.first_at >= now() - make_interval(days => _new_days)
    GROUP BY o.product_id
    HAVING count(DISTINCT o.buyer_id) >= 2        -- не рекомендувати те, що взяв лише 1 двійник
  ),
  prod AS (
    SELECT product_id, max(sku) AS article, max(name) AS name, max(category_name) AS category,
           sum(coalesce(current_stock, 0)) AS stock
    FROM sku_metrics GROUP BY product_id
  ),
  prc AS (SELECT product_id, max(price) AS price FROM skus GROUP BY product_id)
  SELECT c.product_id::bigint, pr.article, pr.name, pr.category, prc.price, pr.stock, c.twins,
         round(c.raw_score / power(coalesce(p.buyers, 1), 0.9), 4) AS score  -- сильніша поправка на популярність
  FROM cand c
  JOIN prod pr ON pr.product_id = c.product_id
  LEFT JOIN prc ON prc.product_id = c.product_id
  LEFT JOIN pop p ON p.product_id = c.product_id
  WHERE coalesce(pr.stock, 0) > 0
  ORDER BY score DESC
  LIMIT _limit;
$$;

GRANT SELECT, INSERT ON reorder_recs_log TO authenticated, anon, service_role;
GRANT USAGE, SELECT ON SEQUENCE reorder_recs_log_id_seq TO authenticated, anon, service_role;
GRANT EXECUTE ON FUNCTION reorder_recs_hitrate(int) TO authenticated, anon, service_role;
GRANT EXECUTE ON FUNCTION reorder_recs(bigint, int, int) TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
