-- Migration 071: когорти в розрізі сегмента (опт / розниця).
--
-- Раніше cohort_retention рахувала лише опт (is_wholesale). Тепер повертає
-- сегмент (client_type: опт/розниця) окремою колонкою — для двох матриць на
-- вкладці «Когорти». Клієнт належить одному сегменту, тож когорти природно
-- розділяються. Системні/gamijok/скасовані/«Обʼєднання» виключені.

CREATE OR REPLACE FUNCTION cohort_retention(_months int DEFAULT 24)
RETURNS TABLE(segment text, cohort text, month_after int, size bigint, active bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH act AS (
    SELECT s.buyer_id,
           CASE WHEN b.client_type = 'опт' THEN 'опт' ELSE 'розниця' END AS segment,
           to_char(s.ordered_at AT TIME ZONE 'UTC','YYYY-MM') AS m
    FROM sales s
    JOIN buyers b ON b.buyer_id = s.buyer_id AND b.client_type IS DISTINCT FROM 'системні'
    WHERE s.buyer_id IS NOT NULL
      AND coalesce(s.order_status,'') <> ALL (_excluded_statuses())
      AND coalesce(s.order_status,'') NOT LIKE 'Об%єднання замовлень'
      AND coalesce(s.source_name,'') <> 'gamijok.com.ua'
      AND s.ordered_at >= CASE WHEN _months IS NULL THEN now() - interval '5 years'
                               ELSE now() - make_interval(months => _months + 12) END
    GROUP BY s.buyer_id, 2, 3
  ),
  fo AS (SELECT buyer_id, segment, min(m) AS cohort FROM act GROUP BY buyer_id, segment),
  sizes AS (SELECT segment, cohort, count(*) AS size FROM fo GROUP BY segment, cohort),
  joined AS (
    SELECT fo.segment, fo.cohort, a.buyer_id,
           ((substr(a.m,1,4)::int - substr(fo.cohort,1,4)::int) * 12
            + (substr(a.m,6,2)::int - substr(fo.cohort,6,2)::int)) AS month_after
    FROM act a JOIN fo ON fo.buyer_id = a.buyer_id
  )
  SELECT j.segment, j.cohort, j.month_after, sz.size, count(DISTINCT j.buyer_id)
  FROM joined j
  JOIN sizes sz ON sz.segment = j.segment AND sz.cohort = j.cohort
  WHERE j.month_after >= 0
  GROUP BY j.segment, j.cohort, j.month_after, sz.size;
$$;

GRANT EXECUTE ON FUNCTION cohort_retention(int) TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
