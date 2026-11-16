-- Migration 059: marketing_order_facts повертає ще й utm_content.
--
-- Потрібно для звіту «Реклама» — drill-down кампанія→оголошення у Meta
-- (order.utm_content = назва оголошення {{ad.name}}). Колонка вже є в sales
-- (міграція 049). Зберігаємо SET statement_timeout з міграції 052.

DROP FUNCTION IF EXISTS marketing_order_facts(timestamptz, text);

CREATE OR REPLACE FUNCTION marketing_order_facts(_since timestamptz, _source text)
RETURNS TABLE(
  order_id     bigint,
  buyer_id     bigint,
  ordered_at   timestamptz,
  order_status text,
  revenue      numeric,
  utm_source   text,
  utm_medium   text,
  utm_campaign text,
  utm_content  text,
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
    max(s.buyer_id)                                     AS buyer_id,
    min(s.ordered_at)                                   AS ordered_at,
    max(s.order_status)                                 AS order_status,
    COALESCE(max(s.order_grand_total), sum(s.revenue))  AS revenue,
    max(s.utm_source)                                   AS utm_source,
    max(s.utm_medium)                                   AS utm_medium,
    max(s.utm_campaign)                                 AS utm_campaign,
    max(s.utm_content)                                  AS utm_content,
    max(s.source_name)                                  AS source_name
  FROM sales s
  WHERE s.order_id IS NOT NULL
    AND s.ordered_at >= _since
    AND s.source_name = _source
  GROUP BY s.order_id;
$$;

GRANT EXECUTE ON FUNCTION marketing_order_facts(timestamptz, text)
  TO authenticated, anon, service_role;
