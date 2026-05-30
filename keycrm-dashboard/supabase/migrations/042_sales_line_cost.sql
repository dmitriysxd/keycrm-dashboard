-- Migration 042: точний cost на рівні позиції замовлення.
--
-- KeyCRM на кожній позиції замовлення зберігає purchased_price — фактичну
-- закупівельну ціну на момент створення замовлення. Це snapshot який не
-- змінюється навіть якщо потім cost у картці товару підняти.
--
-- Раніше ми брали поточний skus.cost — це давало неточну маржу для
-- історичних замовлень (продав за 100 грн коли закупка була 50, а в
-- картці зараз 55 → маржа рахувалась як 100−55=45 замість 100−50=50).
--
-- Тепер: sales.line_cost = item.purchased_price на момент створення.
-- Якщо null (історичні до накатки міграції чи KeyCRM не повернув) —
-- fallback на skus.cost у запитах.

ALTER TABLE sales
  ADD COLUMN IF NOT EXISTS line_cost NUMERIC(14,2);

CREATE INDEX IF NOT EXISTS sales_line_cost_idx ON sales(line_cost) WHERE line_cost IS NOT NULL;

-- Бекфіл всі історичні замовлення — окремий workflow
-- (.github/workflows/backfill-source.yml + step=backfill_source у
-- /api/cron/ingest, який тепер тягне ще й line_cost з KeyCRM).
