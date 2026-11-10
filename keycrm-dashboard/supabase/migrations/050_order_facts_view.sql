-- Migration 050: v_order_facts — один рядок на ЗАМОВЛЕННЯ (дедуп sales у БД).
--
-- ПРОБЛЕМА: маркетингова аналітика тягла в Node усі рядки-позиції sales за
-- 2 роки (~100k рядків, ~100 послідовних сторінок) і впиралась у 30с ліміт
-- Vercel → 504 FUNCTION_INVOCATION_TIMEOUT, сторінка «Маркетинг» не вантажилась.
--
-- РІШЕННЯ: дедуп на рівні заказу робить Postgres (GROUP BY order_id), а в Node
-- їде ~10k рядків-замовлень. Швидко й стабільно. Заодно віддаємо utm_medium
-- (для правила «cpc = платний Google Ads»).

CREATE OR REPLACE VIEW v_order_facts AS
  SELECT
    s.order_id,
    max(s.buyer_id)                                    AS buyer_id,
    min(s.ordered_at)                                  AS ordered_at,
    max(s.order_status)                                AS order_status,
    COALESCE(max(s.order_grand_total), sum(s.revenue)) AS revenue,
    max(s.utm_source)                                  AS utm_source,
    max(s.utm_medium)                                  AS utm_medium,
    max(s.source_name)                                 AS source_name
  FROM sales s
  WHERE s.order_id IS NOT NULL
  GROUP BY s.order_id;

GRANT SELECT ON v_order_facts TO authenticated, anon, service_role;
