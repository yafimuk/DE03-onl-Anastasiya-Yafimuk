DROP TABLE IF EXISTS orders_demo CASCADE;
DROP TABLE IF EXISTS fct_sales CASCADE;
DROP TABLE IF EXISTS customers_demo CASCADE;

-- ---------------------------------
-- 1) Создаём и наполняем большую таблицу (orders_demo)
-- ---------------------------------
CREATE TABLE IF NOT EXISTS orders_demo (
    order_id      BIGSERIAL PRIMARY KEY,     -- PK создаёт уникальный B-Tree индекс автоматически
    customer_id   INT NOT NULL,
    status        TEXT NOT NULL,
    order_date    DATE NOT NULL,
    amount        NUMERIC(10,2) NOT NULL,
    email         TEXT                        -- здесь покажем уникальный индекс и дубликаты
);

-- Наполняем ~1 млн строк случайными данными
INSERT INTO orders_demo (customer_id, status, order_date, amount, email)
SELECT
    (random()*100000)::INT,                                        -- до 100k клиентов
    (ARRAY['New','Processing','Completed','Cancelled'])[
        1 + (floor(random()*4))::INT
    ],
    DATE '2020-01-01' + (random()*1825)::INT,                      -- 2020-2025
    round((random()*500)::numeric, 2),
    'user_' || (100000 + (random()*900000)::INT)::TEXT || '@mail.com'
FROM generate_series(1, 1000000);


SELECT * FROM orders_demo;

-- Прогоняем сбор статистики, чтобы оптимизатор видел распределение данных
ANALYZE orders_demo;

-- ---------------------------------
-- 2) Покажем «как есть» — запрос БЕЗ индекса
-- ---------------------------------
EXPLAIN ANALYZE
SELECT *
FROM orders_demo
WHERE status = 'Completed';

-- Ожидаем Seq Scan (полный просмотр), время ощутимое при 1млн строк

-- ---------------------------------
-- 3) Индексы: безопасно пересоздаём и демонстрируем вызовы
-- ---------------------------------

-- 3.1) Обычный B-Tree по статусу
DROP INDEX IF EXISTS idx_orders_status;
CREATE INDEX idx_orders_status ON orders_demo(status);

EXPLAIN ANALYZE
SELECT *
FROM orders_demo
WHERE status = 'Completed';
-- Теперь должен быть Index Scan по idx_orders_status

-- 3.2) Составной (Composite): customer_id + order_date
DROP INDEX IF EXISTS idx_orders_customer_date;
CREATE INDEX idx_orders_customer_date
  ON orders_demo(customer_id, order_date);

EXPLAIN ANALYZE
SELECT order_id, customer_id, order_date, amount
FROM orders_demo
WHERE order_date BETWEEN DATE '2024-01-01' AND DATE '2024-12-31' AND customer_id = 42  
ORDER BY order_date DESC;
-- Должен использовать составной индекс (фильтр по 1-му столбцу + диапазон по 2-му)

-- 3.3) Частичный индекс: только для Completed
DROP INDEX IF EXISTS idx_orders_completed_date;
CREATE INDEX idx_orders_completed_date
  ON orders_demo(order_date)
  WHERE status = 'Completed';

EXPLAIN ANALYZE
SELECT order_id, order_date, amount
FROM orders_demo
WHERE status = 'Completed'
  AND order_date >= DATE '2025-01-01';
-- Должен браться частичный индекс (меньше размер, быстрее обновления)

-- 3.4) Функциональный индекс: LOWER(email) (поиск без учета регистра)
DROP INDEX IF EXISTS idx_orders_lower_email;
CREATE INDEX idx_orders_lower_email
  ON orders_demo(LOWER(email));

EXPLAIN ANALYZE
SELECT order_id, email
FROM orders_demo
WHERE LOWER(email) = 'user_440138@mail.com';
-- Используется функциональный индекс


-- 3.5) Полнотекстовый (GIN) — НЕ B-Tree (для примера добавим текст)
ALTER TABLE orders_demo ADD COLUMN description TEXT;
-- Заполним описание для части строк (чтобы индекс имел смысл)
UPDATE orders_demo
SET description = CASE
    WHEN random() < 0.2 THEN 'fast shipping and easy refund policy'
    WHEN random() < 0.4 THEN 'express delivery available'
    ELSE 'standard delivery'
END;

-- создаём GIN (Generalized Inverted Index) по to_tsvector
DROP INDEX IF EXISTS idx_orders_desc_fts;
CREATE INDEX idx_orders_desc_fts
  ON orders_demo USING gin (to_tsvector('english', description));

EXPLAIN ANALYZE
SELECT order_id
FROM orders_demo
WHERE to_tsvector('english', description) @@ plainto_tsquery('english', 'refund policy');
-- Демонстрирует отличия от B-Tree: структура GIN для текста

-- ---------------------------------
-- 4) Уникальный индекс по email: сначала покажем дубликаты и удалим их
-- ---------------------------------
-- Ищем дубликаты
WITH dups AS (
  SELECT email, COUNT(*) AS cnt
  FROM orders_demo
  GROUP BY email
  HAVING COUNT(*) > 1
)
SELECT COUNT(*) AS duplicated_values, SUM(cnt) AS total_rows_in_dups
FROM dups;

-- Удаляем дубликаты, оставляя по одной строке (используем ctid, PG-специфично)
WITH d AS (
  SELECT email, ctid,
         ROW_NUMBER() OVER (PARTITION BY email ORDER BY ctid) AS rn
  FROM orders_demo
)
DELETE FROM orders_demo t
USING d
WHERE t.ctid = d.ctid
  AND d.rn > 1;

-- Теперь создадим/пересоздадим уникальный индекс
DROP INDEX IF EXISTS idx_orders_email_unique;
CREATE UNIQUE INDEX idx_orders_email_unique ON orders_demo(email);

-- Проверочный вызов: оптимизатор пойдёт по уникальному индексу
EXPLAIN ANALYZE
SELECT order_id, email
FROM orders_demo
WHERE email = 'user_440138@mail.com';

-- ---------------------------------
-- 5) Кластеризация (PostgreSQL): физически упорядочим таблицу по дате
-- ---------------------------------
-- Кластеризация не «живая», её нужно периодически повторять; сначала индекс-ключ
DROP INDEX IF EXISTS idx_orders_order_date;
CREATE INDEX idx_orders_order_date ON orders_demo(order_date);

-- Физическая пересортировка таблицы на диске:
CLUSTER orders_demo USING idx_orders_order_date;

-- После кластеризации диапазонные запросы по дате читают последовательные блоки
EXPLAIN ANALYZE
SELECT *
FROM orders_demo
WHERE order_date BETWEEN '2024-03-01' AND '2027-03-31';

-- ---------------------------------
-- 6) Партиционирование: мини-пример (факт продаж по годам)
-- ---------------------------------
CREATE TABLE IF NOT EXISTS fct_sales (
  sale_id     BIGSERIAL,
  sale_date   DATE NOT NULL,
  region      TEXT,
  amount      NUMERIC(12,2)
) PARTITION BY RANGE (sale_date);

-- Партиции по годам
CREATE TABLE fct_sales_2024 PARTITION OF fct_sales
  FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE fct_sales_2025 PARTITION OF fct_sales
  FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- Наполним обе партиции
INSERT INTO fct_sales (sale_date, region, amount)
SELECT
  DATE '2024-01-01' + (random()*365)::INT,
  (ARRAY['East','West','North','South'])[1 + (floor(random()*4))::INT],
  round((random()*1000)::numeric, 2)
FROM generate_series(1, 500000);

INSERT INTO fct_sales (sale_date, region, amount)
SELECT
  DATE '2025-01-01' + (random()*300)::INT,
  (ARRAY['East','West','North','South'])[1 + (floor(random()*4))::INT],
  round((random()*1000)::numeric, 2)
FROM generate_series(1, 500000);


ANALYZE fct_sales;

-- Индекс внутри партиции 2025 по region (для локального поиска)
DROP INDEX IF EXISTS idx_fct_sales_2025_region;
CREATE INDEX idx_fct_sales_2025_region ON fct_sales_2025(region);

-- (опционально) Кластеризуем только партицию 2025 по region
CLUSTER fct_sales_2025 USING idx_fct_sales_2025_region;

-- Запрос бьёт только нужную партицию по дате + пользуется локальным индексом
EXPLAIN ANALYZE
SELECT region
FROM fct_sales
WHERE region = 'West'
GROUP BY region;

-- ---------------------------------
-- 7) Доп. утилитные вызовы (что есть сейчас)
-- ---------------------------------
-- Список индексов по orders_demo
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'orders_demo'
ORDER BY indexname;

-- Оценка «веса» индексов
SELECT
  indexrelname AS index_name,
  pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
FROM pg_stat_all_indexes
WHERE schemaname = current_schema()
  AND indexrelname LIKE 'idx_orders_%'
ORDER BY pg_relation_size(indexrelid) DESC;
