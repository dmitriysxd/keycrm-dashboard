-- Migration 032: підготовка інфраструктури для дизайн-аналізу і рекомендацій.
--
-- 1. Включаємо розширення pgvector — для зберігання embeddings (1536-мірних
--    векторів від OpenAI) і швидких nearest-neighbor запитів.
-- 2. Додаємо колонки в skus для зберігання результатів аналізу фото.
-- 3. HNSW індекс для embedding — для cosine similarity пошуку <100ms на 4000 SKU.

------------------------------------------------------------
-- 1. pgvector extension
------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS vector;

------------------------------------------------------------
-- 2. Колонки в skus
------------------------------------------------------------
ALTER TABLE skus
  ADD COLUMN IF NOT EXISTS image_url           TEXT,
  ADD COLUMN IF NOT EXISTS visual_description  TEXT,
  ADD COLUMN IF NOT EXISTS visual_tags         TEXT[],
  ADD COLUMN IF NOT EXISTS design_attributes   JSONB,
  ADD COLUMN IF NOT EXISTS image_embedding     vector(1536),
  ADD COLUMN IF NOT EXISTS image_analyzed_at   TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS image_analysis_error TEXT;

------------------------------------------------------------
-- 3. HNSW індекс для cosine similarity пошуку
--    Параметри m=16, ef_construction=64 — стандартні для каталогу <100k.
--    vector_cosine_ops підтримує cosine distance (1 - cosine_similarity).
------------------------------------------------------------
CREATE INDEX IF NOT EXISTS skus_image_embedding_idx
  ON skus USING hnsw (image_embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);

------------------------------------------------------------
-- 4. Допоміжний індекс для пошуку необроблених SKU
------------------------------------------------------------
CREATE INDEX IF NOT EXISTS skus_pending_analysis_idx
  ON skus(offer_id)
  WHERE image_analyzed_at IS NULL AND image_url IS NOT NULL;

------------------------------------------------------------
-- 5. Лічильник прогресу для UI
------------------------------------------------------------
CREATE OR REPLACE FUNCTION image_analysis_progress()
RETURNS TABLE(
  total_with_image     BIGINT,
  analyzed             BIGINT,
  pending              BIGINT,
  with_errors          BIGINT,
  latest_analysis_at   TIMESTAMPTZ
)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT
    COUNT(*) FILTER (WHERE image_url IS NOT NULL)::BIGINT                  AS total_with_image,
    COUNT(*) FILTER (WHERE image_embedding IS NOT NULL)::BIGINT            AS analyzed,
    COUNT(*) FILTER (WHERE image_url IS NOT NULL
                          AND image_analyzed_at IS NULL)::BIGINT           AS pending,
    COUNT(*) FILTER (WHERE image_analysis_error IS NOT NULL)::BIGINT       AS with_errors,
    MAX(image_analyzed_at)                                                 AS latest_analysis_at
  FROM skus
  WHERE is_active = TRUE;
$$;

GRANT EXECUTE ON FUNCTION image_analysis_progress() TO authenticated, anon, service_role;

------------------------------------------------------------
-- 6. RPC для рекомендацій (попередньо створюємо, заповниться даними після
--    запуску analyze-images). Поки повертає порожньо коли немає embeddings.
------------------------------------------------------------
CREATE OR REPLACE FUNCTION similar_to_offer(_offer_id BIGINT, _limit INT DEFAULT 10)
RETURNS TABLE(
  offer_id       BIGINT,
  product_id     BIGINT,
  sku            TEXT,
  name           TEXT,
  category_name  TEXT,
  price          NUMERIC,
  image_url      TEXT,
  similarity     NUMERIC
)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  WITH target AS (
    SELECT image_embedding FROM skus WHERE offer_id = _offer_id
  )
  SELECT
    s.offer_id, s.product_id, s.sku, s.name, s.category_name, s.price, s.image_url,
    ROUND((1 - (s.image_embedding <=> t.image_embedding))::numeric, 3) AS similarity
  FROM skus s, target t
  WHERE s.offer_id != _offer_id
    AND s.image_embedding IS NOT NULL
    AND t.image_embedding IS NOT NULL
    AND s.is_active = TRUE
  ORDER BY s.image_embedding <=> t.image_embedding
  LIMIT _limit;
$$;

CREATE OR REPLACE FUNCTION recommend_for_buyer(_buyer_id BIGINT, _limit INT DEFAULT 10)
RETURNS TABLE(
  offer_id       BIGINT,
  product_id     BIGINT,
  sku            TEXT,
  name           TEXT,
  category_name  TEXT,
  price          NUMERIC,
  image_url      TEXT,
  similarity     NUMERIC,
  purchased_count_among_similar INT
)
LANGUAGE plpgsql STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
DECLARE
  avg_vec vector(1536);
  purchased_offer_ids BIGINT[];
BEGIN
  -- Збираємо ID куплених товарів
  SELECT ARRAY_AGG(DISTINCT s.offer_id) INTO purchased_offer_ids
  FROM sales s
  WHERE s.buyer_id = _buyer_id
    AND s.offer_id IS NOT NULL
    AND COALESCE(s.order_status, '') NOT IN
        ('cancelled','rejected','canceled','Повернули','Відмовились','incorrect_data','underbid')
    AND COALESCE(s.order_status, '') NOT LIKE 'Об%єднання замовлень';

  IF purchased_offer_ids IS NULL OR array_length(purchased_offer_ids, 1) = 0 THEN
    RETURN;  -- нема покупок — нема рекомендацій
  END IF;

  -- Усереднюємо вектори куплених товарів → preference vector клієнта
  SELECT AVG(image_embedding) INTO avg_vec
  FROM skus
  WHERE offer_id = ANY(purchased_offer_ids)
    AND image_embedding IS NOT NULL;

  IF avg_vec IS NULL THEN
    RETURN;  -- ні в одного купленого товару нема embedding
  END IF;

  RETURN QUERY
  SELECT
    sk.offer_id, sk.product_id, sk.sku, sk.name, sk.category_name, sk.price, sk.image_url,
    ROUND((1 - (sk.image_embedding <=> avg_vec))::numeric, 3) AS similarity,
    -- Скільки клієнтів купляли цей товар (proxy популярності)
    (SELECT COUNT(DISTINCT buyer_id)::INT FROM sales
     WHERE sales.offer_id = sk.offer_id AND buyer_id IS NOT NULL) AS purchased_count_among_similar
  FROM skus sk
  -- JOIN з sku_metrics щоб фільтрувати по current_stock (поле з matview, не skus)
  LEFT JOIN sku_metrics sm ON sm.offer_id = sk.offer_id
  WHERE sk.image_embedding IS NOT NULL
    AND sk.is_active = TRUE
    AND (sm.current_stock IS NULL OR sm.current_stock > 0)
    AND NOT (sk.offer_id = ANY(purchased_offer_ids))
  ORDER BY sk.image_embedding <=> avg_vec
  LIMIT _limit;
END;
$$;

GRANT EXECUTE ON FUNCTION similar_to_offer(BIGINT, INT) TO authenticated, anon, service_role;
GRANT EXECUTE ON FUNCTION recommend_for_buyer(BIGINT, INT) TO authenticated, anon, service_role;
