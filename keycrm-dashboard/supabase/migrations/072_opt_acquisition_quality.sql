-- Migration 072: «Якість залучення опту» — по місяцях залучення нові оптовики
-- + ранній повтор (1 міс / 3 міс). Фіксоване вікно, щоб когорти були порівнянні.
--
-- Лише опт (client_type='опт'), bijboom, системні/gamijok/скасовані/«Обʼєднання»
-- виключені. Беремо тільки когорти, що «дожили» до +3 міс (age>=3), інакше
-- повт_3міс неповний. _months — скільки місяців історії показати.

CREATE OR REPLACE FUNCTION opt_acquisition_quality(_months int DEFAULT 24)
RETURNS TABLE(cohort text, new_buyers bigint, repeat_1m_pct numeric, repeat_3m_pct numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH act AS (
    SELECT s.buyer_id, to_char(s.ordered_at AT TIME ZONE 'UTC','YYYY-MM') AS m
    FROM sales s
    JOIN buyers b ON b.buyer_id = s.buyer_id AND b.client_type = 'опт'
    WHERE s.buyer_id IS NOT NULL
      AND coalesce(s.order_status,'') <> ALL (_excluded_statuses())
      AND coalesce(s.order_status,'') NOT LIKE 'Об%єднання замовлень'
      AND coalesce(s.source_name,'') <> 'gamijok.com.ua'
    GROUP BY 1, 2
  ),
  fo AS (SELECT buyer_id, min(m) AS cohort FROM act GROUP BY buyer_id),
  nm AS (SELECT to_char(now() AT TIME ZONE 'UTC','YYYY-MM') AS v),
  ma AS (
    SELECT fo.cohort, fo.buyer_id,
           (substr(act.m,1,4)::int - substr(fo.cohort,1,4)::int) * 12
            + (substr(act.m,6,2)::int - substr(fo.cohort,6,2)::int) AS d
    FROM fo JOIN act ON act.buyer_id = fo.buyer_id
  ),
  per_buyer AS (
    SELECT cohort, buyer_id,
           max(CASE WHEN d = 1 THEN 1 ELSE 0 END) AS ret1,
           max(CASE WHEN d BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS ret3
    FROM ma GROUP BY cohort, buyer_id
  ),
  agec AS (
    SELECT DISTINCT cohort,
           (substr((SELECT v FROM nm),1,4)::int - substr(cohort,1,4)::int) * 12
            + (substr((SELECT v FROM nm),6,2)::int - substr(cohort,6,2)::int) AS age
    FROM fo
  )
  SELECT pb.cohort,
         count(*)::bigint,
         round(100.0 * sum(ret1) / count(*), 1),
         round(100.0 * sum(ret3) / count(*), 1)
  FROM per_buyer pb
  JOIN agec a ON a.cohort = pb.cohort
  WHERE a.age >= 3
    AND pb.cohort >= to_char(now() - make_interval(months => _months), 'YYYY-MM')
  GROUP BY pb.cohort
  ORDER BY pb.cohort;
$$;

GRANT EXECUTE ON FUNCTION opt_acquisition_quality(int) TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
