-- Migration 044: дроблення кроку 'sales' в auto-циклі.
--
-- ПРОБЛЕМА (статистика юзера, ~30 днів):
-- 28 з 30 днів мали мінімум 1 застряглий цикл, ~6 з 30 днів — ЖОДЕН цикл
-- не завершився повністю. Причина: 'sales' робив 2 паралельні проходи
-- (created_between + updated_between), кожен до 200 сторінок × 50 заказів,
-- не вкладався в 60s Vercel-ліміт. Vercel вбивав функцію посеред потоку,
-- ingest_state лишався в 'running' назавжди, до наступного ручного дзвінка.
--
-- РІШЕННЯ: 'sales' тепер дробиться на чанки (3 сторінки за виклик), як
-- 'products'. State машина переключає sub-passes: created → updated → done.
--
-- Ця міграція додає колонку current_substep, в якій зберігається активний
-- прохід ('created' / 'updated' / NULL).

ALTER TABLE ingest_state
  ADD COLUMN IF NOT EXISTS current_substep TEXT;

COMMENT ON COLUMN ingest_state.current_substep IS
  'Підкрок поточного step (для chunked sales: created/updated). NULL для решти steps.';
