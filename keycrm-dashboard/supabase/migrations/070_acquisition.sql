-- Migration 070: атрибуція залучення (first-touch) для вкладки «Залучення».
--
-- Повертає ПО ОДНОМУ РЯДКУ НА ПОКУПЦЯ: сегмент (опт/розниця) + utm/джерело
-- його ПЕРШОГО заказу. Класифікацію каналу робить бекенд (normChannel) — щоб
-- канали збігались із вкладкою «Маркетинг». Легко (≈к-сть покупців рядків).
--
-- Фільтри ті самі, що всюди: bijboom (не gamijok), виключені статуси +
-- «Обʼєднання замовлень», системні клієнти викинуті. _since (дата залучення =
-- дата першого заказу) — NULL = уся історія.

CREATE OR REPLACE FUNCTION acquisition_first_touch(_since timestamptz DEFAULT NULL)
RETURNS TABLE(segment text, utm_source text, utm_medium text, utm_campaign text, source_name text)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = public, pg_catalog
SET statement_timeout = '25000'
AS $$
  WITH fo AS (
    SELECT DISTINCT ON (s.buyer_id)
           s.buyer_id, s.ordered_at,
           s.utm_source, s.utm_medium, s.utm_campaign, s.source_name
    FROM sales s
    JOIN buyers b ON b.buyer_id = s.buyer_id AND b.client_type IS DISTINCT FROM 'системні'
    WHERE s.buyer_id IS NOT NULL
      AND s.ordered_at IS NOT NULL
      AND coalesce(s.order_status,'') <> ALL (_excluded_statuses())
      AND coalesce(s.order_status,'') NOT LIKE 'Об%єднання замовлень'
      AND coalesce(s.source_name,'') <> 'gamijok.com.ua'
    ORDER BY s.buyer_id, s.ordered_at ASC
  )
  SELECT
    CASE WHEN b.client_type = 'опт' THEN 'опт' ELSE 'розниця' END AS segment,
    fo.utm_source, fo.utm_medium, fo.utm_campaign, fo.source_name
  FROM fo
  JOIN buyers b ON b.buyer_id = fo.buyer_id
  WHERE _since IS NULL OR fo.ordered_at >= _since;
$$;

GRANT EXECUTE ON FUNCTION acquisition_first_touch(timestamptz) TO authenticated, anon, service_role;

NOTIFY pgrst, 'reload schema';
