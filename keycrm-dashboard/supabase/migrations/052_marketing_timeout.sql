-- Migration 052: власний statement_timeout для marketing_order_facts.
--
-- Навіть відфільтрована агрегація замовлень сайту може перевищувати низький
-- statement_timeout ролі БД (Supabase за замовчуванням ставить кілька секунд).
-- Даємо функції власний ліміт 25с (у межах 30с Vercel), щоб її не вбивало.
-- SET у визначенні функції також вимикає inlining — тож ліміт точно діє.

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
SET statement_timeout = '25000'
AS $$
  SELECT
    s.order_id,
    max(s.buyer_id),
    min(s.ordered_at),
    max(s.order_status),
    COALESCE(max(s.order_grand_total), sum(s.revenue)),
    max(s.utm_source),
    max(s.utm_medium),
    max(s.source_name)
  FROM sales s
  WHERE s.order_id IS NOT NULL
    AND s.ordered_at >= _since
    AND s.source_name = _source
  GROUP BY s.order_id;
$$;

GRANT EXECUTE ON FUNCTION marketing_order_facts(timestamptz, text)
  TO authenticated, anon, service_role;
