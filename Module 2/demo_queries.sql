-- ============================================
-- Примеры запросов для демонстрации
-- Модуль 2: Работа с данными в GreenPlum
-- Основано на Лабораторной работе 2
-- ============================================

-- ============================================
-- ЧАСТЬ 1: Создание партиционированных таблиц
-- ============================================

-- Создание схемы для работы
CREATE SCHEMA IF NOT EXISTS lab2;
SET search_path TO lab2, public;

-- Таблица заказов, партиционированная по месяцам
CREATE TABLE orders_partitioned (
    order_id BIGSERIAL,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    order_time TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    shipping_address TEXT,
    PRIMARY KEY (order_id, order_date,customer_id)
)
DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (order_date)
(
    START (DATE '2024-01-01') INCLUSIVE
    END (DATE '2025-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION outliers
);

-- Непартиционированная таблица для сравнения
CREATE TABLE orders_no_partition (
    order_id BIGSERIAL ,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    order_time TIMESTAMP NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    shipping_address TEXT,
    PRIMARY KEY (order_id, order_date,customer_id)
) DISTRIBUTED BY (customer_id);

-- ============================================
-- Загрузка тестовых данных
-- ============================================

-- Генерация 2 миллионов заказов за 2024 год
-- ВНИМАНИЕ: Загрузка может занять 5-10 минут!
INSERT INTO orders_partitioned 
    (customer_id, product_id, order_date, order_time, quantity, 
     unit_price, total_amount, status, shipping_address)
SELECT 
    (random() * 9999 + 1)::int as customer_id,
    (random() * 999 + 1)::int as product_id,
    DATE '2024-01-01' + (random() * 365)::int as order_date,
    TIMESTAMP '2024-01-01' + (random() * 365 || ' days')::interval + (random() * 86400 || ' seconds')::interval as order_time,
    (random() * 10 + 1)::int as quantity,
    (random() * 200 + 10)::decimal(10,2) as unit_price,
    0 as total_amount, -- обновим позже
    CASE (random() * 4)::int
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'processing'
        WHEN 2 THEN 'shipped'
        WHEN 3 THEN 'delivered'
        ELSE 'cancelled'
    END as status,
    'Address ' || generate_series as shipping_address
FROM generate_series(1, 2000000);

-- Обновление total_amount
UPDATE orders_partitioned 
SET total_amount = quantity * unit_price;

-- Копирование данных в непартиционированную таблицу
INSERT INTO orders_no_partition 
SELECT * FROM orders_partitioned;

-- Обновление статистики
ANALYZE orders_partitioned;
ANALYZE orders_no_partition;

-- ============================================
-- Просмотр информации о партициях
-- ============================================

-- Список партиций
SELECT 
    schemaname,
    tablename,
    partitiontablename,
    partitionname,
    partitionrank,
    partitionboundary
FROM pg_partitions
WHERE schemaname = 'lab2'
    AND tablename = 'orders_partitioned'
ORDER BY partitionrank;


-- Вариант 3: Оценка через статистику (быстро, но приблизительно)
SELECT 
    p.partitiontablename,
    p.partitionrank,
    p.partitionboundary,
    c.reltuples::bigint as estimated_rows
FROM pg_partitions p
LEFT JOIN pg_class c ON c.relname = p.partitiontablename
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.partitionschemaname
WHERE p.schemaname = 'lab2'
    AND p.tablename = 'orders_partitioned'
ORDER BY p.partitionrank;

-- ============================================
-- Создание справочных таблиц
-- ============================================

-- Справочник клиентов
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    city VARCHAR(100),
    country VARCHAR(100),
    registration_date DATE
) DISTRIBUTED REPLICATED;

-- Справочник продуктов
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL
) DISTRIBUTED REPLICATED;

-- Загрузка данных в справочники
INSERT INTO customers
SELECT 
    generate_series as customer_id,
    'Customer ' || generate_series as customer_name,
    'customer' || generate_series || '@example.com' as email,
    'City ' || (random() * 100)::int as city,
    CASE (random() * 5)::int
        WHEN 0 THEN 'USA'
        WHEN 1 THEN 'UK'
        WHEN 2 THEN 'Germany'
        WHEN 3 THEN 'France'
        ELSE 'Canada'
    END as country,
    CURRENT_DATE - (random() * 1000)::int as registration_date
FROM generate_series(1, 10000);

INSERT INTO products
SELECT 
    generate_series as product_id,
    'Product ' || generate_series as product_name,
    CASE (random() * 5)::int
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Food'
        WHEN 3 THEN 'Books'
        ELSE 'Home'
    END as category,
    (random() * 500 + 10)::decimal(10,2) as price
FROM generate_series(1, 1000);

ANALYZE customers;
ANALYZE products;

-- ============================================
-- ЧАСТЬ 2: Анализ планов выполнения запросов
-- ============================================

-- Простой запрос с фильтром по дате на партиционированной таблице
EXPLAIN ANALYZE
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM orders_partitioned
WHERE order_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY order_date
ORDER BY order_date;

-- Что искать в плане выполнения:
-- 1. Partition Pruning - исключение ненужных партиций
-- 2. Seq Scan vs Index Scan - тип сканирования
-- 3. Motion операции - перемещение данных между сегментами
-- 4. Execution Time - время выполнения

-- Тот же запрос на непартиционированной таблице (для сравнения)
EXPLAIN ANALYZE
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM orders_no_partition
WHERE order_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY order_date
ORDER BY order_date;

-- Задание: Сравните:
-- - Количество сканируемых строк
-- - Время выполнения
-- - Наличие partition pruning

-- ============================================
-- Анализ JOIN операций с реплицированными таблицами
-- ============================================

-- JOIN с реплицированными таблицами
EXPLAIN ANALYZE
SELECT 
    c.country,
    p.category,
    COUNT(*) as order_count,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value
FROM orders_partitioned o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN products p ON o.product_id = p.product_id
WHERE o.order_date >= '2024-07-01'
GROUP BY c.country, p.category
ORDER BY total_revenue DESC;

-- Обратите внимание:
-- - Отсутствие Redistribute Motion (благодаря REPLICATED)
-- - Эффективность Broadcast Join
-- - Применение partition pruning

-- ============================================
-- ЧАСТЬ 3: Оптимизация запросов
-- ============================================

-- ============================================
-- Проблема: неоптимальный JOIN не по distribution key
-- ============================================

-- Создание таблицы с RANDOM распределением (плохо для JOIN)
CREATE TABLE order_items (
    item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL
) DISTRIBUTED RANDOMLY;

-- Загрузка данных
INSERT INTO order_items (order_id, product_id, quantity, price)
SELECT 
    order_id,
    product_id,
    quantity,
    unit_price
FROM orders_partitioned
LIMIT 1000000;

ANALYZE order_items;

-- Неэффективный запрос (JOIN не по distribution key)
EXPLAIN ANALYZE
SELECT 
    o.order_id,
    o.order_date,
    COUNT(oi.item_id) as item_count,
    SUM(oi.quantity) as total_quantity
FROM orders_partitioned o
LEFT JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.order_date >= '2024-10-01'
GROUP BY o.order_id, o.order_date
LIMIT 100;

-- Обратите внимание на Redistribute Motion в плане выполнения!

-- ============================================
-- Решение: правильное распределение
-- ============================================

-- Создание таблицы с согласованным распределением
CREATE TABLE order_items_optimized (
    item_id BIGSERIAL,
    order_id BIGINT NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    PRIMARY KEY (item_id, order_id)
) DISTRIBUTED BY (order_id);

-- Копирование данных
INSERT INTO order_items_optimized 
SELECT * FROM order_items;

ANALYZE order_items_optimized;

-- Создаем соответствующую таблицу заказов
CREATE TABLE orders_by_order_id (
    LIKE orders_partitioned
) DISTRIBUTED BY (order_id)
PARTITION BY RANGE (order_date)
(
    START (DATE '2024-01-01') INCLUSIVE
    END (DATE '2025-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month'),
    DEFAULT PARTITION outliers
);

INSERT INTO orders_by_order_id SELECT * FROM orders_partitioned;
ANALYZE orders_by_order_id;

-- Оптимизированный запрос (JOIN по distribution key)
EXPLAIN ANALYZE
SELECT 
    o.order_id,
    o.order_date,
    COUNT(oi.item_id) as item_count,
    SUM(oi.quantity) as total_quantity
FROM orders_by_order_id o
LEFT JOIN order_items_optimized oi ON o.order_id = oi.order_id
WHERE o.order_date >= '2024-10-01'
GROUP BY o.order_id, o.order_date
LIMIT 100;

-- Сравните:
-- - Отсутствие Redistribute Motion
-- - Время выполнения

-- ============================================
-- Оптимизация с помощью индексов
-- ============================================

-- Создание индексов для ускорения поиска
CREATE INDEX idx_orders_date_status 
ON orders_no_partition(order_date, status);

CREATE INDEX idx_orders_customer 
ON orders_no_partition(customer_id)
WHERE status = 'delivered';

ANALYZE orders_no_partition;

-- Запрос с селективным фильтром
EXPLAIN ANALYZE
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent
FROM orders_no_partition
WHERE order_date >= '2024-11-01'
    AND status = 'delivered'
GROUP BY customer_id
HAVING COUNT(*) > 5
ORDER BY total_spent DESC
LIMIT 20;

-- ============================================
-- Оптимизация с помощью CTE (WITH)
-- ============================================

-- Неоптимальный запрос (множественные сканирования)
EXPLAIN ANALYZE
SELECT 
    o.customer_id,
    (SELECT COUNT(*) FROM orders_partitioned WHERE customer_id = o.customer_id) as total_orders,
    (SELECT SUM(total_amount) FROM orders_partitioned WHERE customer_id = o.customer_id) as total_spent,
    o.order_date,
    o.total_amount
FROM orders_partitioned o
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY o.total_amount DESC
LIMIT 20;

-- Оптимизированный запрос с CTE
EXPLAIN ANALYZE
WITH customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        SUM(total_amount) as total_spent
    FROM orders_partitioned
    GROUP BY customer_id
),
recent_orders AS (
    SELECT 
        customer_id,
        order_date,
        total_amount
    FROM orders_partitioned
    WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    ro.customer_id,
    cs.total_orders,
    cs.total_spent,
    ro.order_date,
    ro.total_amount
FROM recent_orders ro
JOIN customer_stats cs ON ro.customer_id = cs.customer_id
ORDER BY ro.total_amount DESC
LIMIT 20;

-- ============================================
-- Материализованные представления
-- ============================================

-- Создание материализованного представления
CREATE MATERIALIZED VIEW monthly_sales_summary AS
SELECT 
    DATE_TRUNC('month', o.order_date) as month,
    c.country,
    p.category,
    COUNT(*) as order_count,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    COUNT(DISTINCT o.customer_id) as unique_customers
FROM orders_partitioned o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
GROUP BY DATE_TRUNC('month', o.order_date), c.country, p.category
DISTRIBUTED BY (month);

-- Создание индексов на материализованном представлении
CREATE INDEX idx_monthly_sales_month ON monthly_sales_summary(month);
CREATE INDEX idx_monthly_sales_country ON monthly_sales_summary(country);

-- Использование материализованного представления (БЫСТРО)
EXPLAIN ANALYZE
SELECT 
    month,
    country,
    SUM(total_revenue) as revenue,
    SUM(order_count) as orders
FROM monthly_sales_summary
WHERE month >= '2024-06-01'
GROUP BY month, country
ORDER BY month, revenue DESC;

-- Сравнение с прямым запросом (МЕДЛЕННЕЕ)
EXPLAIN ANALYZE
SELECT 
    DATE_TRUNC('month', o.order_date) as month,
    c.country,
    SUM(o.total_amount) as revenue,
    COUNT(*) as orders
FROM orders_partitioned o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-06-01'
GROUP BY DATE_TRUNC('month', o.order_date), c.country
ORDER BY month, revenue DESC;

-- ============================================
-- ЧАСТЬ 4: Практические задания
-- ============================================

-- ============================================
-- Задание 1: Оптимизация медленного запроса
-- ============================================

-- Медленный запрос с EXISTS
SELECT 
    *
FROM orders_partitioned o
WHERE EXISTS (
    SELECT 1 
    FROM orders_partitioned o2
    WHERE o2.customer_id = o.customer_id
        AND o2.order_date > o.order_date
        AND o2.total_amount > o.total_amount
)
ORDER BY o.order_date DESC;

-- Оптимизированная версия с window functions
EXPLAIN ANALYZE
SELECT 
    order_id,
    customer_id,
    order_date,
    total_amount
FROM (
    SELECT 
        order_id,
        customer_id,
        order_date,
        total_amount,
        LEAD(total_amount) OVER (
            PARTITION BY customer_id 
            ORDER BY order_date
        ) as next_amount
    FROM orders_partitioned
) subq
WHERE next_amount > total_amount
ORDER BY order_date DESC;

-- ============================================
-- Задание 2: Написание эффективного отчета
-- ============================================

-- Отчет с выручкой по странам и категориям
WITH recent_period AS (
    SELECT 
        o.customer_id,
        c.country,
        p.category,
        o.total_amount,
        o.order_date
    FROM orders_partitioned o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN products p ON o.product_id = p.product_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '3 months'
),
previous_period AS (
    SELECT 
        o.customer_id,
        c.country,
        p.category,
        o.total_amount
    FROM orders_partitioned o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN products p ON o.product_id = p.product_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '6 months'
        AND o.order_date < CURRENT_DATE - INTERVAL '3 months'
),
country_category_sales AS (
    SELECT 
        country,
        category,
        SUM(total_amount) as current_revenue
    FROM recent_period
    GROUP BY country, category
),
previous_sales AS (
    SELECT 
        country,
        category,
        SUM(total_amount) as previous_revenue
    FROM previous_period
    GROUP BY country, category
),
top_customers AS (
    SELECT 
        customer_id,
        country,
        SUM(total_amount) as customer_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(total_amount) DESC) as rank
    FROM recent_period
    GROUP BY customer_id, country
)
SELECT 
    ccs.country,
    ccs.category,
    ccs.current_revenue,
    COALESCE(ps.previous_revenue, 0) as previous_revenue,
    ROUND(
        100.0 * (ccs.current_revenue - COALESCE(ps.previous_revenue, 0)) / 
        NULLIF(ps.previous_revenue, 0), 
        2
    ) as growth_percentage
FROM country_category_sales ccs
LEFT JOIN previous_sales ps 
    ON ccs.country = ps.country 
    AND ccs.category = ps.category
ORDER BY ccs.current_revenue DESC;

-- Top-10 клиентов
WITH recent_period AS (
    SELECT 
        o.customer_id,
        c.country,
        p.category,
        o.total_amount,
        o.order_date
    FROM orders_partitioned o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN products p ON o.product_id = p.product_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '3 months'
),
top_customers AS (
    SELECT 
        customer_id,
        country,
        SUM(total_amount) as customer_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(total_amount) DESC) as rank
    FROM recent_period
    GROUP BY customer_id, country
)
SELECT 
    tc.customer_id,
    c.customer_name,
    tc.country,
    tc.customer_revenue
FROM top_customers tc
JOIN customers c ON tc.customer_id = c.customer_id
WHERE tc.rank <= 10
ORDER BY tc.rank;

-- ============================================
-- Задание 3: Отладка проблемы с производительностью
-- ============================================

-- Медленный запрос
SELECT 
    p.category,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    COUNT(*) as order_count,
    SUM(o.total_amount) as revenue
FROM orders_no_partition o
JOIN products p ON o.product_id = p.product_id
WHERE o.status IN ('delivered', 'shipped')
    AND o.order_date >= '2024-01-01'
GROUP BY p.category
ORDER BY revenue DESC;

-- Используйте EXPLAIN ANALYZE для анализа
EXPLAIN ANALYZE
SELECT 
    p.category,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    COUNT(*) as order_count,
    SUM(o.total_amount) as revenue
FROM orders_no_partition o
JOIN products p ON o.product_id = p.product_id
WHERE o.status IN ('delivered', 'shipped')
    AND o.order_date >= '2024-01-01'
GROUP BY p.category
ORDER BY revenue DESC;

-- ============================================
-- Таблица результатов тестов производительности
-- ============================================

CREATE TABLE query_performance_tests (
    test_id SERIAL PRIMARY KEY,
    test_name VARCHAR(200),
    query_description TEXT,
    table_setup VARCHAR(200),
    execution_time_ms DECIMAL(10, 2),
    rows_scanned BIGINT,
    rows_returned INTEGER,
    has_redistribute BOOLEAN,
    has_broadcast BOOLEAN,
    partition_pruning BOOLEAN,
    optimization_notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Примеры записей
INSERT INTO query_performance_tests 
    (test_name, query_description, table_setup, execution_time_ms, 
     rows_scanned, rows_returned, has_redistribute, partition_pruning, optimization_notes)
VALUES 
    ('Партиционированная vs. обычная', 'SELECT с фильтром по дате', 
     'orders_partitioned', 1234.56, 60000, 30000, false, true, 'Partition pruning сработал отлично');

-- ============================================
-- Полезные запросы для мониторинга
-- ============================================

-- Просмотр текущих запросов
SELECT 
    pid,
    usename,
    datname,
    client_addr,
    query_start,
    state,
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
WHERE schemaname = 'lab2'
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
WHERE schemaname = 'lab2'
ORDER BY n_live_tup DESC;

-- ============================================
-- ОЧИСТКА РЕСУРСОВ
-- ============================================

-- ВНИМАНИЕ: Это удалит все данные!
-- Раскомментируйте только если уверены

/*
-- Удаление материализованных представлений
DROP MATERIALIZED VIEW IF EXISTS monthly_sales_summary;

-- Удаление таблиц
DROP TABLE IF EXISTS query_performance_tests;
DROP TABLE IF EXISTS order_items_optimized;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders_by_order_id;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS orders_no_partition;
DROP TABLE IF EXISTS orders_partitioned CASCADE;

-- Удаление схемы
DROP SCHEMA IF EXISTS lab2 CASCADE;
*/

-- ============================================
-- КОНЕЦ ПРИМЕРОВ ИЗ LAB2
-- ============================================

-- Полезные команды для работы в psql:
-- \dt - список таблиц
-- \d+ table_name - подробная информация о таблице
-- \di - список индексов
-- \dv - список представлений
-- \dm - список материализованных представлений
-- \timing on - включить отображение времени выполнения
-- \x - переключение расширенного вывода

