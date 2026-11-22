-- Migration 066: виправлення — «Сегменти» рахують виручку по ІТОГУ заказу зі
-- знижкою (order_grand_total), а не сумою позицій.
--
-- Баг у 065: sum(revenue) = сума позицій (ціна×к-сть) БЕЗ знижок на заказ →
-- завищення проти KeyCRM (де є знижки, особливо в опта). Вкладки
-- «Маркетинг»/«Реклама» давно рахують через COALESCE(order_grand_total,
-- sum(revenue)) і сходяться з KeyCRM — приводимо «Сегменти» до того ж.
--
-- Рахуємо спершу на рівні ЗАКАЗУ (grand_total один на заказ), потім агрегуємо.
-- gross_profit = (виручка заказу зі знижкою) − (собівартість позицій заказу).

CREATE OR REPLACE FUNCTION segment_monthly(_months int DEFAULT 24)
RETURNS TABLE(
  month text, segment text,
  revenue numeric, gross_profit numeric,
  active_buyers bigint, orders bigint,
  new_buyers bigint, repeat_buyers bigint
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH ord AS (
    SELECT s.order_id,
           max(s.buyer_id) AS buyer_id,
           min(s.ordered_at) AS ordered_at,
           coalesce(max(s.order_grand_total), sum(s.revenue)) AS revenue,
           coalesce(max(s.order_grand_total), sum(s.revenue)) - sum(s.line_cost * s.quantity) AS gross_profit
    FROM sales s
    WHERE coalesce(s.order_status,'') <> ALL (_excluded_statuses())
    GROUP BY s.order_id
  ),
  fo AS (SELECT buyer_id, min(ordered_at) AS first_at FROM ord GROUP BY buyer_id)
  SELECT
    to_char(o.ordered_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM'),
    CASE WHEN b.client_type = 'опт' THEN 'опт' ELSE 'розниця' END,
    round(sum(o.revenue)), round(sum(o.gross_profit)),
    count(DISTINCT o.buyer_id), count(DISTINCT o.order_id),
    count(DISTINCT CASE WHEN to_char(fo.first_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM')
                           = to_char(o.ordered_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM')
                        THEN o.buyer_id END),
    count(DISTINCT CASE WHEN to_char(fo.first_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM')
                          <> to_char(o.ordered_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM')
                        THEN o.buyer_id END)
  FROM ord o
  JOIN buyers b ON b.buyer_id = o.buyer_id AND b.client_type IS DISTINCT FROM 'системні'
  JOIN fo ON fo.buyer_id = o.buyer_id
  WHERE o.ordered_at >= now() - make_interval(months => _months)
  GROUP BY 1, 2;
$$;

CREATE OR REPLACE FUNCTION segment_retention(_min_age_days int DEFAULT 90)
RETURNS TABLE(
  segment text, buyers bigint, returned bigint,
  repeat_pct numeric, avg_orders numeric, avg_profit numeric
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH ord AS (
    SELECT s.order_id,
           max(s.buyer_id) AS buyer_id,
           min(s.ordered_at) AS ordered_at,
           coalesce(max(s.order_grand_total), sum(s.revenue)) - sum(s.line_cost * s.quantity) AS gp
    FROM sales s
    WHERE coalesce(s.order_status,'') <> ALL (_excluded_statuses())
    GROUP BY s.order_id
  ),
  fo AS (
    SELECT buyer_id, min(ordered_at) AS first_at, count(*) AS orders, sum(gp) AS profit
    FROM ord GROUP BY buyer_id
  )
  SELECT
    CASE WHEN b.client_type = 'опт' THEN 'опт' ELSE 'розниця' END,
    count(*), count(*) FILTER (WHERE fo.orders >= 2),
    round(100.0 * count(*) FILTER (WHERE fo.orders >= 2) / nullif(count(*),0), 1),
    round(avg(fo.orders), 2), round(avg(fo.profit))
  FROM fo
  JOIN buyers b ON b.buyer_id = fo.buyer_id AND b.client_type IS DISTINCT FROM 'системні'
  WHERE fo.first_at < now() - make_interval(days => _min_age_days)
  GROUP BY 1;
$$;
