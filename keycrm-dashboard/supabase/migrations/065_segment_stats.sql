-- Migration 065: агрегати по сегментах (опт / розниця) для вкладки «Сегменти».
--
-- Рахуємо на стороні БД (швидко, без вивантаження sales у Node). «системні»
-- виключаємо. Враховуємо ВСІ замовлення (не лише сайт) — це здоров'я бізнесу
-- загалом, а не тільки маркетинг-канали.
--
-- gross_profit = sum(revenue − line_cost*quantity): line_cost — закупка за
-- ОДИНИЦЮ, тому множимо на quantity. Де line_cost порожній (історія до бекфілу)
-- — рядок не рахується в прибуток (тому маржа за старі місяці занижена, поки
-- не залито собівартість).

-- Статуси-виключення одним масивом (повтор у кількох функціях).
CREATE OR REPLACE FUNCTION _excluded_statuses()
RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT ARRAY['cancelled','canceled','rejected','Повернули','Повернення',
               'Відмовились','incorrect_data','underbid','Перенос (дублікат)']
$$;

-- Помісячні агрегати по сегментах.
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
  WITH fo AS (
    SELECT buyer_id, min(ordered_at) AS first_at
    FROM sales
    WHERE coalesce(order_status,'') <> ALL (_excluded_statuses())
    GROUP BY buyer_id
  )
  SELECT
    to_char(s.ordered_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM') AS month,
    CASE WHEN b.client_type = 'опт' THEN 'опт' ELSE 'розниця' END AS segment,
    round(sum(s.revenue)) AS revenue,
    round(sum(s.revenue - s.line_cost * s.quantity)) AS gross_profit,
    count(DISTINCT s.buyer_id) AS active_buyers,
    count(DISTINCT s.order_id) AS orders,
    count(DISTINCT CASE WHEN to_char(fo.first_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM')
                           = to_char(s.ordered_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM')
                        THEN s.buyer_id END) AS new_buyers,
    count(DISTINCT CASE WHEN to_char(fo.first_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM')
                          <> to_char(s.ordered_at AT TIME ZONE 'Europe/Kyiv','YYYY-MM')
                        THEN s.buyer_id END) AS repeat_buyers
  FROM sales s
  JOIN buyers b ON b.buyer_id = s.buyer_id AND b.client_type IS DISTINCT FROM 'системні'
  JOIN fo ON fo.buyer_id = s.buyer_id
  WHERE s.ordered_at >= now() - make_interval(months => _months)
    AND coalesce(s.order_status,'') <> ALL (_excluded_statuses())
  GROUP BY 1, 2;
$$;

-- Утримання по сегментах (лише клієнти, залучені > _min_age_days тому — встигли
-- повернутися). Повертає % повторних, середнє заказів і середній lifetime-прибуток.
CREATE OR REPLACE FUNCTION segment_retention(_min_age_days int DEFAULT 90)
RETURNS TABLE(
  segment text, buyers bigint, returned bigint,
  repeat_pct numeric, avg_orders numeric, avg_profit numeric
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH fo AS (
    SELECT buyer_id,
           min(ordered_at) AS first_at,
           count(DISTINCT order_id) AS orders,
           sum(revenue - line_cost * quantity) AS profit
    FROM sales
    WHERE coalesce(order_status,'') <> ALL (_excluded_statuses())
    GROUP BY buyer_id
  )
  SELECT
    CASE WHEN b.client_type = 'опт' THEN 'опт' ELSE 'розниця' END AS segment,
    count(*) AS buyers,
    count(*) FILTER (WHERE fo.orders >= 2) AS returned,
    round(100.0 * count(*) FILTER (WHERE fo.orders >= 2) / nullif(count(*),0), 1) AS repeat_pct,
    round(avg(fo.orders), 2) AS avg_orders,
    round(avg(fo.profit)) AS avg_profit
  FROM fo
  JOIN buyers b ON b.buyer_id = fo.buyer_id AND b.client_type IS DISTINCT FROM 'системні'
  WHERE fo.first_at < now() - make_interval(days => _min_age_days)
  GROUP BY 1;
$$;

GRANT EXECUTE ON FUNCTION _excluded_statuses() TO authenticated, anon, service_role;
GRANT EXECUTE ON FUNCTION segment_monthly(int) TO authenticated, anon, service_role;
GRANT EXECUTE ON FUNCTION segment_retention(int) TO authenticated, anon, service_role;
