-- Migration 069: перенос розрахунку «Тренди» та «Когорти» у SQL (RPC).
--
-- Раніше handleTrends/handleCohorts тягли ВСЮ історію sales у Node і рахували
-- там — важко, і під навантаженням (бекфіл / нічний ingest) запит не вкладався
-- у ліміт Vercel → HTTP 500. Переносимо агрегацію в БД (як segment_*/marketing).
--
-- Логіка 1-в-1 з колишнім Node-кодом:
--   • лише опт-клієнти (buyers.is_wholesale = true), заказы з buyer_id;
--   • виключаємо статуси-скасування (_excluded_statuses) + «Обʼєднання
--     замовлень» (NOT LIKE) + другий магазин gamijok.com.ua (NULL source
--     лишаємо — це переважно bijboom, історія до бекфілу джерела);
--   • виручка = coalesce(order_grand_total, sum(line revenue)) на заказ;
--   • бакети/місяці — у UTC (як стара JS-логіка через toISOString()).

-- ── Тренди: ряд по тижнях / місяцях / роках ──────────────────────────────
CREATE OR REPLACE FUNCTION trends_series(_bucket text DEFAULT 'month')
RETURNS TABLE(bucket text, orders bigint, buyers bigint, revenue numeric, aov numeric)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH ord AS (
    SELECT s.order_id,
           max(s.buyer_id) AS buyer_id,
           min(s.ordered_at) AS ordered_at,
           coalesce(max(s.order_grand_total), sum(s.revenue)) AS revenue
    FROM sales s
    JOIN buyers b ON b.buyer_id = s.buyer_id AND b.is_wholesale = true
    WHERE s.ordered_at >= now() - interval '2 years'
      AND coalesce(s.order_status,'') <> ALL (_excluded_statuses())
      AND coalesce(s.order_status,'') NOT LIKE 'Об%єднання замовлень'
      AND coalesce(s.source_name,'') <> 'gamijok.com.ua'
    GROUP BY s.order_id
  )
  SELECT
    CASE _bucket
      WHEN 'year' THEN to_char(o.ordered_at AT TIME ZONE 'UTC','YYYY')
      WHEN 'week' THEN to_char(date_trunc('week', o.ordered_at AT TIME ZONE 'UTC'),'YYYY-MM-DD')
      ELSE to_char(o.ordered_at AT TIME ZONE 'UTC','YYYY-MM')
    END AS bucket,
    count(DISTINCT o.order_id) AS orders,
    count(DISTINCT o.buyer_id) AS buyers,
    round(sum(o.revenue)) AS revenue,
    CASE WHEN count(DISTINCT o.order_id) > 0
         THEN round(sum(o.revenue) / count(DISTINCT o.order_id)) ELSE 0 END AS aov
  FROM ord o
  GROUP BY 1
  ORDER BY 1;
$$;

-- ── Когорти: утримання по місячних когортах ──────────────────────────────
-- Повертає «довгий» формат: рядок на (cohort, month_after) з розміром когорти
-- та к-стю активних. _months = NULL → вся історія (5 років); інакше вікно
-- _months+12 міс (запас, щоб поміряти retention ранніх когорт). Обрізання до
-- останніх _months когорт і заповнення нулів робить Node.
CREATE OR REPLACE FUNCTION cohort_retention(_months int DEFAULT 24)
RETURNS TABLE(cohort text, month_after int, size bigint, active bigint)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH act AS (  -- унікальні (опт-клієнт × місяць активності)
    SELECT s.buyer_id,
           to_char(s.ordered_at AT TIME ZONE 'UTC','YYYY-MM') AS m
    FROM sales s
    JOIN buyers b ON b.buyer_id = s.buyer_id AND b.is_wholesale = true
    WHERE s.buyer_id IS NOT NULL
      AND coalesce(s.order_status,'') <> ALL (_excluded_statuses())
      AND coalesce(s.order_status,'') NOT LIKE 'Об%єднання замовлень'
      AND coalesce(s.source_name,'') <> 'gamijok.com.ua'
      AND s.ordered_at >= CASE WHEN _months IS NULL THEN now() - interval '5 years'
                               ELSE now() - make_interval(months => _months + 12) END
    GROUP BY s.buyer_id, 2
  ),
  fo AS (SELECT buyer_id, min(m) AS cohort FROM act GROUP BY buyer_id),
  sizes AS (SELECT cohort, count(*) AS size FROM fo GROUP BY cohort),
  joined AS (
    SELECT fo.cohort,
           a.buyer_id,
           ( (substr(a.m,1,4)::int - substr(fo.cohort,1,4)::int) * 12
             + (substr(a.m,6,2)::int - substr(fo.cohort,6,2)::int) ) AS month_after
    FROM act a JOIN fo ON fo.buyer_id = a.buyer_id
  )
  SELECT j.cohort, j.month_after, sz.size, count(DISTINCT j.buyer_id) AS active
  FROM joined j
  JOIN sizes sz ON sz.cohort = j.cohort
  WHERE j.month_after >= 0
  GROUP BY j.cohort, j.month_after, sz.size;
$$;

GRANT EXECUTE ON FUNCTION trends_series(text) TO authenticated, anon, service_role;
GRANT EXECUTE ON FUNCTION cohort_retention(int) TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
