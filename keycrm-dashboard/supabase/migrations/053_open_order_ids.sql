-- Migration 053: open_order_ids — «відкриті» (не фінальні) заказы для
-- самопочинки статусів.
--
-- ПРОБЛЕМА: ingest оновлює заказы лише за updated_between вікно (3 дні). Якщо
-- статус заказу змінився в KeyCRM у день, коли автозагрузка висіла, цей заказ
-- більше ніколи не перечитується → назавжди застрягає зі старим статусом
-- (напр. order 9311 лишився 'new', хоча в KeyCRM вже 'completed').
--
-- РІШЕННЯ: крок refresh_open у ingest перечитує «відкриті» заказы (не
-- completed і не «мертві») — їх небагато (~100-150). Ця функція повертає
-- їхні order_id. Фінальні статуси (completed / повернення / скасування /
-- дублі / обʼєднання) не чіпаємо — вони вже не зміняться.

CREATE OR REPLACE FUNCTION open_order_ids(_limit int DEFAULT 500)
RETURNS TABLE(order_id bigint)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
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
