-- Migration 051: marketing_order_facts — швидка агрегація замовлень для маркетингу.
--
-- ПРОБЛЕМА: сторінка «Наскрізна аналітика» читала вью v_order_facts з фільтрами
-- .eq(source_name) / .gte(ordered_at). Але у вью ці колонки — АГРЕГАТИ
-- (max(source_name), min(ordered_at)), тож фільтр застосовувався ПІСЛЯ GROUP BY
-- → Postgres агрегував усю таблицю sales щоразу, та ще й постранично (кожна
-- сторінка = повторна агрегація). Результат: "canceling statement due to
-- statement timeout".
--
-- РІШЕННЯ: RPC-функція, де WHERE стоїть на БАЗОВИХ колонках sales
-- (s.source_name, s.ordered_at) — тобто фільтр ДО агрегації, по індексах.
-- Групуємо лише потрібну підмножину (замовлення сайту за період) і повертаємо
-- все одним запитом без OFFSET-пагінації.

CREATE INDEX IF NOT EXISTS sales_source_ordered_idx ON sales(source_name, ordered_at);

CREATE OR REPLACE FUNCTION marketing_order_facts(_since timestamptz, _source text)
RETURNS TABLE(
  order_id     bigint,
  buyer_id     bigint,
  ordered_at   timestamptz,
  order_status text,
  revenue      numeric,
  utm_source   text,
  utm_medium   text,
  source_name  text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT
    s.order_id,
    max(s.buyer_id)                                     AS buyer_id,
    min(s.ordered_at)                                   AS ordered_at,
    max(s.order_status)                                 AS order_status,
    COALESCE(max(s.order_grand_total), sum(s.revenue))  AS revenue,
    max(s.utm_source)                                   AS utm_source,
    max(s.utm_medium)                                   AS utm_medium,
    max(s.source_name)                                  AS source_name
  FROM sales s
  WHERE s.order_id IS NOT NULL
    AND s.ordered_at >= _since
    AND s.source_name = _source
  GROUP BY s.order_id;
$$;

GRANT EXECUTE ON FUNCTION marketing_order_facts(timestamptz, text)
  TO authenticated, anon, service_role;
