-- Migration 041: додаємо джерело замовлення в sales.
--
-- KeyCRM повертає в /order поле source як вкладений об'єкт {id, name},
-- іноді як source_id+source_name на верхньому рівні. Зберігаємо обидва
-- значення щоб у дашборді можна було фільтрувати по джерелу.
--
-- ВАЖЛИВО: історичні замовлення (до накатки цієї міграції) матимуть
-- source_id=NULL. Заповнюються при наступному ingest для НОВИХ заказів.
-- Для бекфіл усієї історії — окрема одноразова процедура (нижче коментар).

ALTER TABLE sales
  ADD COLUMN IF NOT EXISTS source_id   BIGINT,
  ADD COLUMN IF NOT EXISTS source_name TEXT;

CREATE INDEX IF NOT EXISTS sales_source_id_idx ON sales(source_id) WHERE source_id IS NOT NULL;

-- БЕКФІЛ (опціонально): якщо захочеш заповнити source для всієї історії —
-- треба окремий one-off endpoint, який пройде по всіх замовленнях через
-- KeyCRM /order і UPDATE'ить sales. Зараз цього немає, додамо коли скажеш.
