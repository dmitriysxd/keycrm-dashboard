-- Migration 073: рекомендатель новинок для оптовика («вкусовые двойники»).
--
-- User-based CF: знаходимо оптовиків із схожим смаком (косинус по наборах
-- куплених product_id), і рекомендуємо НОВИНКИ (перша продажа за _new_days),
-- які вже беруть його двійники, в наявності, яких він ще не брав.
-- Поправка на популярність (/sqrt(buyers)) — щоб вилазив смак, а не бестселери.
-- Бэктест (random holdout) підтвердив: CF б'є топ-бестселери (3.03 vs 1.69
-- влучань у топ-20, hit-rate 90% vs 65%).
--
-- Показ: артикул (skus.sku), назва, категорія, ціна, залишок (sku_metrics),
-- twins = скільки двійників уже взяли. Лише опт; системні/gamijok/скасовані
-- виключені. Рахується по одному покупцю (ліниво при відкритті картки).

CREATE OR REPLACE FUNCTION reorder_recs(_buyer_id bigint, _new_days int DEFAULT 45, _limit int DEFAULT 15)
RETURNS TABLE(product_id bigint, article text, name text, category text,
              price numeric, stock numeric, twins bigint, score numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH op AS (  -- опт покупець × товар (distinct)
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
  sim AS (  -- косинус-схожість інших до нашого покупця
    SELECT o.buyer_id,
           count(*)::numeric / sqrt((SELECT n FROM me_size) * sz.n) AS cosine
    FROM op o
    JOIN sizes sz ON sz.buyer_id = o.buyer_id
    WHERE o.buyer_id <> _buyer_id
      AND o.product_id IN (SELECT product_id FROM me)
    GROUP BY o.buyer_id, sz.n
  ),
  nb AS (SELECT buyer_id, cosine FROM sim ORDER BY cosine DESC LIMIT 60),
  firstsale AS (  -- «дата появи» товару = перша продажа
    SELECT product_id, min(ordered_at) AS first_at
    FROM sales WHERE product_id IS NOT NULL GROUP BY product_id
  ),
  pop AS (SELECT product_id, count(*)::numeric AS buyers FROM op GROUP BY product_id),
  cand AS (  -- новинки від двійників, яких я ще не брав
    SELECT o.product_id,
           sum(nb.cosine) AS raw_score,
           count(DISTINCT o.buyer_id) AS twins
    FROM op o
    JOIN nb ON nb.buyer_id = o.buyer_id
    JOIN firstsale f ON f.product_id = o.product_id
    LEFT JOIN me ON me.product_id = o.product_id
    WHERE me.product_id IS NULL
      AND f.first_at >= now() - make_interval(days => _new_days)
    GROUP BY o.product_id
  ),
  prod AS (  -- показ + залишок (агрегуємо по product_id)
    SELECT product_id,
           max(sku) AS article, max(name) AS name, max(category_name) AS category,
           sum(coalesce(current_stock, 0)) AS stock
    FROM sku_metrics GROUP BY product_id
  ),
  prc AS (SELECT product_id, max(price) AS price FROM skus GROUP BY product_id)
  SELECT c.product_id::bigint,
         pr.article, pr.name, pr.category, prc.price, pr.stock,
         c.twins,
         round(c.raw_score / sqrt(coalesce(p.buyers, 1)), 3) AS score
  FROM cand c
  JOIN prod pr ON pr.product_id = c.product_id
  LEFT JOIN prc ON prc.product_id = c.product_id
  LEFT JOIN pop p ON p.product_id = c.product_id
  WHERE coalesce(pr.stock, 0) > 0
  ORDER BY score DESC
  LIMIT _limit;
$$;

GRANT EXECUTE ON FUNCTION reorder_recs(bigint, int, int) TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
