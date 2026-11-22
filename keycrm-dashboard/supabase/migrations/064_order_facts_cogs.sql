-- Migration 064: marketing_order_facts повертає СОБІВАРТІСТЬ (COGS) замовлення.
--
-- Для ROAS беззбитковості / маржі / прибутку грошима у вкладках «Маркетинг»
-- і «Реклама». Обидві використовують цей RPC, тож зміна покриває обидві.
--
-- ВАЖЛИВО про одиниці:
--   sales.line_cost  = закупка за ОДИНИЦЮ (item.purchased_price, snapshot).
--   sales.revenue    = виручка за ВСЮ позицію (ціна × кількість).
-- Тому COGS замовлення = sum(line_cost * quantity), а НЕ sum(line_cost).
--
-- cogs_known = true лише якщо В УСІХ позицій замовлення заповнений line_cost.
-- Інакше маржа ненадійна (частина собівартості невідома) → фронт не рахує
-- маржу/прибуток по таких замовленнях, а показує покриття.

DROP FUNCTION IF EXISTS marketing_order_facts(timestamptz, text);

CREATE OR REPLACE FUNCTION marketing_order_facts(_since timestamptz, _source text)
RETURNS TABLE(
  order_id     bigint,
  buyer_id     bigint,
  ordered_at   timestamptz,
  order_status text,
  revenue      numeric,
  cogs         numeric,
  cogs_known   boolean,
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
    sum(s.line_cost * s.quantity)                       AS cogs,        -- собівартість (одиниця × к-сть)
    bool_and(s.line_cost IS NOT NULL)                   AS cogs_known,  -- усі позиції мають закупку
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
