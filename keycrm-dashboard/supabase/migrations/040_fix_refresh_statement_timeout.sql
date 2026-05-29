-- Migration 040: фікс "sku_metrics не оновлюється — statement timeout".
--
-- ПРОБЛЕМА (виявлено 2026-05-29):
-- sku_metrics відставав від sales на 95 годин (4 дні). У health-check видно:
--   metrics_rfm: true                                    ← buyer_rfm рефрешиться
--   metrics_sku_error: "canceling statement due to statement timeout"
--
-- КОРІНЬ:
-- Коли ми перенесли REFRESH з pg_cron (міграція 036) у RPC-виклик з ingest,
-- запит почав іти від ролі service_role через PostgREST. Ця роль має дефолтний
-- statement_timeout (Supabase зазвичай ~8с для API-запитів). Раніше pg_cron
-- виконував REFRESH від postgres-власника без цього ліміту.
--
-- sku_metrics — важкий matview (CTE по sales, що ростуть: сьогодні залилось
-- 7085 рядків). Його REFRESH займає більше за дефолтний timeout → щоразу
-- падає. buyer_rfm легший → встигає, тому й працює.
--
-- ВИПРАВЛЕННЯ:
-- ALTER FUNCTION ... SET statement_timeout = '180000' (3 хв) встановлює
-- локальний таймаут на час виконання функції, перевизначаючи дефолт ролі.
-- Це задокументований патерн Supabase для важких RPC.
--
-- CONCURRENTLY лишаємо — щоб денний ручний рефреш (кнопка в UI) не блокував
-- читачів дашборду. 3 хвилини з запасом покриває зростання даних.

-- 1. sku_metrics refresh — головний фікс.
ALTER FUNCTION refresh_sku_metrics() SET statement_timeout = '180000';

-- 2. buyer_rfm refresh — теж піднімаємо про запас (зараз встигає, але хай
--    не стане наступною жертвою зростання даних).
ALTER FUNCTION refresh_buyer_rfm() SET statement_timeout = '180000';

-- 3. Одразу освіжаємо sku_metrics — щоб дані стали актуальними без чекання
--    наступного ingest. Виконується від власника міграції (postgres) —
--    statement_timeout міграційної сесії не обмежений, тому пройде.
REFRESH MATERIALIZED VIEW CONCURRENTLY sku_metrics;
