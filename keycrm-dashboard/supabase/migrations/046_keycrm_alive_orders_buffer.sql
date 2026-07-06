-- Migration 046: буфер живих замовлень KeyCRM для чистки фантомів.
--
-- ПРИЧИНА: щоб знайти "фантомні" замовлення (видалені в KeyCRM, але лишились
-- у нашій sales → задвоєння), треба перелічити ВСІ живі order_id з KeyCRM і
-- відняти від наших. Замовлень ~7700 (155 сторінок × 50), за один виклик
-- Vercel (60с) при ліміті KeyCRM 60 req/min це не влазить. Тож перелічуємо
-- по частинах у цей буфер, а різницю рахує Postgres (anti-join, миттєво).

CREATE TABLE IF NOT EXISTS keycrm_alive_orders (
  order_id BIGINT PRIMARY KEY
);

------------------------------------------------------------
-- Кандидати на видалення: order_id, що є в sales, але НЕМА в буфері
-- живих замовлень KeyCRM. DISTINCT — бо sales це рядки-позиції.
------------------------------------------------------------
CREATE OR REPLACE FUNCTION sales_orders_not_alive(_limit int DEFAULT 500)
RETURNS TABLE(order_id bigint)
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT DISTINCT s.order_id
  FROM sales s
  LEFT JOIN keycrm_alive_orders a ON a.order_id = s.order_id
  WHERE s.order_id IS NOT NULL
    AND a.order_id IS NULL
  ORDER BY s.order_id
  LIMIT _limit;
$$;

GRANT EXECUTE ON FUNCTION sales_orders_not_alive(int) TO authenticated, anon, service_role;

CREATE OR REPLACE FUNCTION sales_orders_not_alive_count()
RETURNS bigint
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
AS $$
  SELECT COUNT(DISTINCT s.order_id)::bigint
  FROM sales s
  LEFT JOIN keycrm_alive_orders a ON a.order_id = s.order_id
  WHERE s.order_id IS NOT NULL
    AND a.order_id IS NULL;
$$;

GRANT EXECUTE ON FUNCTION sales_orders_not_alive_count() TO authenticated, anon, service_role;
