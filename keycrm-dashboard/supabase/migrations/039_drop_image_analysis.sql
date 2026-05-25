-- Migration 039: видалити всю AI image-analysis інфраструктуру.
--
-- Юзер вирішив не продовжувати з аналізом фото товарів. Прибираємо:
-- - колонки в skus (image_url, visual_*, design_attributes, image_embedding, ...)
-- - індекси (HNSW, image_analyzed_at)
-- - RPC функції (image_analysis_progress, similar_to_offer, recommend_for_buyer)
--
-- pgvector extension НЕ видаляємо — він може стати в нагоді для іншого
-- (RAG для довідки, fuzzy-пошук по описах товарів, тощо).
--
-- Файли видалені паралельно з міграцією:
-- - api/cron/analyze-images.js
-- - lib/openai.js
-- - згадки в vercel.json і ingest.js

-- 1. RPC функції — спочатку, бо вони використовують колонки/типи.
DROP FUNCTION IF EXISTS image_analysis_progress();
DROP FUNCTION IF EXISTS similar_to_offer(BIGINT, INT);
DROP FUNCTION IF EXISTS recommend_for_buyer(BIGINT, INT);

-- 2. Індекси (CASCADE щоб не вимагати порядку).
DROP INDEX IF EXISTS skus_image_embedding_idx;
DROP INDEX IF EXISTS skus_image_analyzed_at_idx;

-- 3. Колонки skus.
ALTER TABLE skus
  DROP COLUMN IF EXISTS image_url,
  DROP COLUMN IF EXISTS visual_description,
  DROP COLUMN IF EXISTS visual_tags,
  DROP COLUMN IF EXISTS design_attributes,
  DROP COLUMN IF EXISTS image_embedding,
  DROP COLUMN IF EXISTS image_analyzed_at,
  DROP COLUMN IF EXISTS image_analysis_error;

-- 4. Якщо ми тримали матеріалізоване представлення sku_metrics зі згадками
--    цих колонок (напр. для UI), його треба перебудувати. Перевіряємо вручну
--    через REFRESH — якщо колонки не використовувались, операції OK.
REFRESH MATERIALIZED VIEW sku_metrics;
