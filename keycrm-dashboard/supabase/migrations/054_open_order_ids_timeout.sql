-- Migration 054: власний statement_timeout + індекс для open_order_ids.
--
-- Запит списку «відкритих» заказів (DISTINCT order_id + NOT IN + сортування
-- по всій sales) іноді впирався в низький statement_timeout ролі БД
-- ("canceling statement due to statement timeout" у кроці refresh_open).
-- Даємо функції 25с і індекс на order_status, щоб фільтр був швидким.

CREATE INDEX IF NOT EXISTS sales_order_status_idx ON sales(order_status);

CREATE OR REPLACE FUNCTION open_order_ids(_limit int DEFAULT 500)
RETURNS TABLE(order_id bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  SELECT DISTINCT s.order_id
  FROM sales s
  WHERE s.order_id IS NOT NULL
    AND s.order_status IS NOT NULL
    AND s.order_status NOT IN (
      'completed', 'виконано',
      'Повернули', 'Повернення', 'canceled', 'cancelled', 'rejected',
      'Відмовились', 'underbid', 'incorrect_data', 'Перенос (дублікат)'
    )
    AND s.order_status NOT LIKE 'Об%єднання замовлень%'
  ORDER BY s.order_id DESC
  LIMIT _limit;
$$;

GRANT EXECUTE ON FUNCTION open_order_ids(int) TO authenticated, anon, service_role;
