-- ============================================
-- Примеры запросов для демонстрации
-- Модуль 2: Работа с данными в GreenPlum
-- ============================================

-- ============================================
-- ЧАСТЬ 1: Стратегии распределения данных
-- ============================================

-- Пример 1: Создание таблицы с HASH распределением
CREATE TABLE sales_hash_demo (
    sale_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    sale_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
) DISTRIBUTED BY (customer_id);

-- Пример 2: Реплицированная таблица
CREATE TABLE products_replicated_demo (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    price DECIMAL(10, 2) NOT NULL
) DISTRIBUTED REPLICATED;

-- Пример 3: Случайное распределение
CREATE TABLE staging_data_demo (
    id BIGSERIAL PRIMARY KEY,
    data_column TEXT
) DISTRIBUTED RANDOMLY;

-- Проверка распределения данных
SELECT 
    gp_segment_id,
    count(*) as row_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage
FROM sales_hash_demo
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- ============================================
-- ЧАСТЬ 2: Партиционирование
-- ============================================

-- Пример 4: Партиционирование по диапазону дат
CREATE TABLE orders_partitioned_demo (
    order_id BIGSERIAL,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (order_id, order_date)
)
DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (order_date)
(
    START (DATE '2024-01-01') INCLUSIVE
    END (DATE '2025-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION outliers
);

-- Просмотр созданных партиций
SELECT 
    partitiontablename,
    partitionname,
    partitionrank,
    partitionboundary
FROM pg_partitions
WHERE tablename = 'orders_partitioned_demo'
ORDER BY partitionrank;

-- Пример 5: Партиционирование по списку
CREATE TABLE sales_by_region_demo (
    sale_id BIGSERIAL,
    region VARCHAR(50) NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    sale_date DATE NOT NULL,
    PRIMARY KEY (sale_id, region)
)
DISTRIBUTED BY (sale_id)
PARTITION BY LIST (region)
(
    PARTITION north VALUES ('North', 'Northeast', 'Northwest'),
    PARTITION south VALUES ('South', 'Southeast', 'Southwest'),
    PARTITION central VALUES ('Central', 'Midwest'),
    DEFAULT PARTITION other
);

-- ============================================
-- ЧАСТЬ 3: Демонстрация Partition Pruning
-- ============================================

-- Вставка тестовых данных
INSERT INTO orders_partitioned_demo (customer_id, order_date, amount)
SELECT 
    (random() * 1000 + 1)::int,
    DATE '2024-01-01' + (random() * 365)::int,
    (random() * 1000 + 10)::decimal(10,2)
FROM generate_series(1, 100000);

ANALYZE orders_partitioned_demo;

-- Запрос С partition pruning (эффективно)
EXPLAIN ANALYZE
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_spent
FROM orders_partitioned_demo
WHERE order_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;

-- Обратите внимание в плане на:
-- - Partition Selector (показывает, какие партиции используются)
-- - Количество сканируемых партиций
-- - Время выполнения

-- ============================================
-- ЧАСТЬ 4: JOIN по distribution key
-- ============================================

-- Создание таблиц для демонстрации
CREATE TABLE customers_demo (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    country VARCHAR(100)
) DISTRIBUTED BY (customer_id);

CREATE TABLE orders_demo (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
) DISTRIBUTED BY (customer_id);

-- Загрузка данных
INSERT INTO customers_demo
SELECT 
    generate_series as customer_id,
    'Customer ' || generate_series as customer_name,
    CASE (random() * 3)::int
        WHEN 0 THEN 'USA'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'Germany'
        ELSE 'France'
    END as country
FROM generate_series(1, 1000);

INSERT INTO orders_demo
SELECT 
    generate_series as order_id,
    (random() * 999 + 1)::int as customer_id,
    CURRENT_DATE - (random() * 365)::int as order_date,
    (random() * 1000 + 10)::decimal(10,2) as amount
FROM generate_series(1, 50000);

ANALYZE customers_demo;
ANALYZE orders_demo;

-- Эффективный JOIN (по distribution key)
EXPLAIN ANALYZE
SELECT 
    c.country,
    COUNT(*) as order_count,
    SUM(o.amount) as total_revenue
FROM orders_demo o
INNER JOIN customers_demo c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY c.country
ORDER BY total_revenue DESC;

-- Обратите внимание:
-- - Нет Redistribute Motion (данные уже правильно распределены)
-- - Быстрое выполнение

-- ============================================
-- ЧАСТЬ 5: Демонстрация проблем с неправильным распределением
-- ============================================

-- Создание таблицы с неоптимальным распределением
CREATE TABLE orders_bad_distribution (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL
) DISTRIBUTED RANDOMLY;

INSERT INTO orders_bad_distribution
SELECT * FROM orders_demo;

ANALYZE orders_bad_distribution;

-- Неэффективный JOIN (требует перемещения данных)
EXPLAIN ANALYZE
SELECT 
    c.country,
    COUNT(*) as order_count,
    SUM(o.amount) as total_revenue
FROM orders_bad_distribution o
INNER JOIN customers_demo c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY c.country
ORDER BY total_revenue DESC;

-- Обратите внимание:
-- - Redistribute Motion присутствует
-- - Увеличенное время выполнения

-- ============================================
-- ЧАСТЬ 6: Сравнение производительности индексов
-- ============================================

-- Создание таблицы без индексов
CREATE TABLE large_table_no_index (
    id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    status VARCHAR(20),
    created_date DATE NOT NULL,
    amount DECIMAL(10, 2)
) DISTRIBUTED BY (id);

-- Загрузка данных
INSERT INTO large_table_no_index
SELECT 
    generate_series as id,
    (random() * 1000 + 1)::int as customer_id,
    CASE (random() * 3)::int
        WHEN 0 THEN 'active'
        WHEN 1 THEN 'pending'
        WHEN 2 THEN 'completed'
        ELSE 'cancelled'
    END as status,
    CURRENT_DATE - (random() * 730)::int as created_date,
    (random() * 1000 + 10)::decimal(10,2) as amount
FROM generate_series(1, 500000);

ANALYZE large_table_no_index;

-- Запрос БЕЗ индекса
EXPLAIN ANALYZE
SELECT *
FROM large_table_no_index
WHERE customer_id = 500
    AND status = 'completed'
ORDER BY created_date DESC
LIMIT 10;

-- Создание индекса
CREATE INDEX idx_customer_status ON large_table_no_index(customer_id, status);
ANALYZE large_table_no_index;

-- Запрос С индексом
EXPLAIN ANALYZE
SELECT *
FROM large_table_no_index
WHERE customer_id = 500
    AND status = 'completed'
ORDER BY created_date DESC
LIMIT 10;

-- Сравните время выполнения и тип сканирования

-- ============================================
-- ЧАСТЬ 7: Оптимизация с помощью CTE
-- ============================================

-- Неоптимальный запрос (множественные сканирования)
EXPLAIN ANALYZE
SELECT 
    o.customer_id,
    (SELECT COUNT(*) FROM orders_demo WHERE customer_id = o.customer_id) as total_orders,
    (SELECT SUM(amount) FROM orders_demo WHERE customer_id = o.customer_id) as total_spent,
    o.order_date,
    o.amount
FROM orders_demo o
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY o.amount DESC
LIMIT 20;

-- Оптимизированный запрос с CTE
EXPLAIN ANALYZE
WITH customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        SUM(amount) as total_spent
    FROM orders_demo
    GROUP BY customer_id
),
recent_orders AS (
    SELECT 
        customer_id,
        order_date,
        amount
    FROM orders_demo
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    ro.customer_id,
    cs.total_orders,
    cs.total_spent,
    ro.order_date,
    ro.amount
FROM recent_orders ro
JOIN customer_stats cs ON ro.customer_id = cs.customer_id
ORDER BY ro.amount DESC
LIMIT 20;

-- ============================================
-- ЧАСТЬ 8: Window Functions для аналитики
-- ============================================

-- Ранжирование клиентов по выручке внутри страны
EXPLAIN ANALYZE
SELECT 
    c.customer_id,
    c.customer_name,
    c.country,
    SUM(o.amount) as total_spent,
    RANK() OVER (PARTITION BY c.country ORDER BY SUM(o.amount) DESC) as rank_in_country,
    SUM(SUM(o.amount)) OVER (PARTITION BY c.country) as country_total,
    ROUND(100.0 * SUM(o.amount) / SUM(SUM(o.amount)) OVER (PARTITION BY c.country), 2) as percentage_of_country
FROM orders_demo o
JOIN customers_demo c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name, c.country
ORDER BY c.country, rank_in_country;

-- ============================================
-- ЧАСТЬ 9: Материализованное представление
-- ============================================

-- Создание материализованного представления
CREATE MATERIALIZED VIEW daily_sales_summary AS
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value,
    COUNT(DISTINCT customer_id) as unique_customers
FROM orders_demo
GROUP BY order_date
DISTRIBUTED BY (order_date);

-- Создание индекса на MV
CREATE INDEX idx_mv_date ON daily_sales_summary(order_date);

-- Использование MV (быстро)
EXPLAIN ANALYZE
SELECT 
    order_date,
    total_revenue,
    avg_order_value
FROM daily_sales_summary
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY order_date;

-- Сравнение с прямым запросом (медленнее)
EXPLAIN ANALYZE
SELECT 
    order_date,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value
FROM orders_demo
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY order_date
ORDER BY order_date;

-- ============================================
-- ЧАСТЬ 10: Анализ skew (неравномерности)
-- ============================================

-- Проверка равномерности распределения
SELECT 
    gp_segment_id,
    count(*) as row_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage,
    CASE 
        WHEN count(*) > avg(count(*)) OVER () * 1.2 THEN 'OVERLOADED'
        WHEN count(*) < avg(count(*)) OVER () * 0.8 THEN 'UNDERLOADED'
        ELSE 'OK'
    END as status
FROM orders_demo
GROUP BY gp_segment_id
ORDER BY row_count DESC;

-- Поиск "горячих" ключей
SELECT 
    customer_id,
    count(*) as order_count,
    CASE 
        WHEN count(*) > (SELECT avg(cnt) * 3 FROM (SELECT count(*) as cnt FROM orders_demo GROUP BY customer_id) x)
        THEN 'HOT KEY'
        ELSE 'OK'
    END as status
FROM orders_demo
GROUP BY customer_id
HAVING count(*) > (SELECT avg(cnt) * 3 FROM (SELECT count(*) as cnt FROM orders_demo GROUP BY customer_id) x)
ORDER BY order_count DESC
LIMIT 20;

-- ============================================
-- ЧАСТЬ 11: Полезные запросы для мониторинга
-- ============================================

-- Просмотр текущих запросов
SELECT 
    pid,
    usename,
    datname,
    client_addr,
    state,
    query_start,
    state_change,
    substring(query, 1, 100) as query_preview
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY query_start;

-- Размер таблиц
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as indexes_size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Статистика по таблицам
SELECT 
    schemaname,
    tablename,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    last_vacuum,
    last_analyze
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;

-- ============================================
-- ЧАСТЬ 12: Очистка демонстрационных объектов
-- ============================================

-- Удаление материализованных представлений
DROP MATERIALIZED VIEW IF EXISTS daily_sales_summary;

-- Удаление таблиц
DROP TABLE IF EXISTS sales_hash_demo CASCADE;
DROP TABLE IF EXISTS products_replicated_demo CASCADE;
DROP TABLE IF EXISTS staging_data_demo CASCADE;
DROP TABLE IF EXISTS orders_partitioned_demo CASCADE;
DROP TABLE IF EXISTS sales_by_region_demo CASCADE;
DROP TABLE IF EXISTS customers_demo CASCADE;
DROP TABLE IF EXISTS orders_demo CASCADE;
DROP TABLE IF EXISTS orders_bad_distribution CASCADE;
DROP TABLE IF EXISTS large_table_no_index CASCADE;

-- ============================================
-- КОНЕЦ ПРИМЕРОВ
-- ============================================

-- Полезные команды для работы:
-- \dt - список таблиц
-- \d+ table_name - подробная информация о таблице
-- \di - список индексов
-- \dv - список представлений
-- \dm - список материализованных представлений
