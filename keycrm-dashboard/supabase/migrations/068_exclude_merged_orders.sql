-- Migration 068: «Сегменти» не рахують статус «Обʼєднання замовлень».
--
-- Звірка з KeyCRM за жовтень 2026:
--   KeyCRM виручка = completed (1 111 223) + delivered (1 352) = 1 112 575 ₴.
--   «Сегменти» давали 1 126 022 ₴ — на 13 447 ₴ більше. Різниця рівно =
--   статус «Обʼєднання замовлень» (9 замовлень, 13 447 ₴): це заказы, склеєні в
--   один; їхня виручка вже врахована в основному заказі, тому KeyCRM їх не
--   рахує. У JS (Маркетинг/Тренди) цей статус уже виключається через
--   isExcluded() (LIKE 'Об%єднання замовлень'), а в SQL-функціях — ні.
--
-- Фікс: 1) додаємо точний рядок у _excluded_statuses(); 2) у segment_*
-- додаємо NOT LIKE 'Об%єднання замовлень' як страховку від різних варіантів
-- апострофа (ʼ U+02BC / ' U+2019 / ' U+0027) — так само, як робить JS.

CREATE OR REPLACE FUNCTION _excluded_statuses()
RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['cancelled','canceled','rejected','Повернули','Повернення',
               'Відмовились','incorrect_data','underbid','Перенос (дублікат)',
               'Обʼєднання замовлень']
$$;

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
      AND coalesce(s.order_status,'') NOT LIKE 'Об%єднання замовлень'
      AND s.source_name = 'bijboom-opt.com.ua'
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
      AND coalesce(s.order_status,'') NOT LIKE 'Об%єднання замовлень'
      AND s.source_name = 'bijboom-opt.com.ua'
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
