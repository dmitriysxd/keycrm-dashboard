-- Migration 056: розхід Meta у гривні (конвертація з валюти кабінету).
--
-- Рекламний кабінет Meta у USD, а виручка — у UAH. Щоб ROAS був коректний,
-- зберігаємо spend_uah (розхід дня × офіційний курс НБУ на цю дату).
-- spend лишається в оригінальній валюті (currency), для звірки.

ALTER TABLE meta_spend
  ADD COLUMN IF NOT EXISTS spend_uah numeric(14,2),
  ADD COLUMN IF NOT EXISTS fx_rate  numeric(12,4);
