-- Migration 058: marketing_order_facts повертає ще й utm_campaign.
--
-- Потрібно для розведення Horoshop-тригерних листів (utm_medium='horoshop_trigger')
-- за utm_campaign на канали: «Системні (Технічні)», «Авто-тригери CMS»,
-- «Інші тригери CMS». Колонка utm_campaign уже є в sales (міграція 049) і
-- пишеться в ingest — лишилось віддати її з RPC.
--
-- Зміна типу повернення → спершу DROP (CREATE OR REPLACE не дозволяє міняти
-- набір колонок RETURNS TABLE).

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
  source_name  text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = public, pg_catalog
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
    max(s.source_name)                                  AS source_name
  FROM sales s
  WHERE s.order_id IS NOT NULL
    AND s.ordered_at >= _since
    AND s.source_name = _source
  GROUP BY s.order_id;
$$;

GRANT EXECUTE ON FUNCTION marketing_order_facts(timestamptz, text)
  TO authenticated, anon, service_role;
