-- ================================================================================
-- ДЕМОНСТРАЦИОННЫЕ SQL ЗАПРОСЫ - ЛАБОРАТОРНАЯ РАБОТА 1
-- Тема: Создание и оптимизация таблиц в Yandex MPP Analytics
-- ================================================================================

-- ================================================================================
-- ЧАСТЬ 1: ПРОВЕРКА ПОДКЛЮЧЕНИЯ И КОНФИГУРАЦИИ КЛАСТЕРА
-- ================================================================================

-- Проверка версии GreenPlum
SELECT version();

-- Просмотр конфигурации кластера
SELECT * FROM gp_segment_configuration;

-- Количество сегментов
SELECT count(*) as segment_count 
FROM gp_segment_configuration 
WHERE role = 'p';

-- ================================================================================
-- ЧАСТЬ 2: СОЗДАНИЕ СХЕМЫ И ТАБЛИЦ
-- ================================================================================

-- Создаем схему для лабораторной работы
CREATE SCHEMA lab1;

-- Устанавливаем схему как текущую
SET search_path TO lab1, public;

-- --------------------------------------------------------------------------------
-- 2.1 Создание таблицы с HASH распределением
-- --------------------------------------------------------------------------------

-- Таблица продаж (большая таблица фактов)
CREATE TABLE sales_hash (
    sale_id BIGSERIAL,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    sale_date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    region VARCHAR(50),
    payment_method VARCHAR(20)
) DISTRIBUTED BY (customer_id);

-- Проверка распределения
SELECT 
    gp_segment_id,
    count(*) as row_count
FROM sales_hash
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- --------------------------------------------------------------------------------
-- 2.2 Создание реплицируемых таблиц-справочников
-- --------------------------------------------------------------------------------

-- Справочник продуктов (малая таблица)
CREATE TABLE products_replicated (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_cost DECIMAL(10, 2) NOT NULL
) DISTRIBUTED REPLICATED;

-- Справочник клиентов (средняя таблица)
CREATE TABLE customers_replicated (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    registration_date DATE NOT NULL,
    customer_segment VARCHAR(50),
    loyalty_level VARCHAR(20)
) DISTRIBUTED REPLICATED;

-- Проверка: реплицированная таблица есть на каждом сегменте
-- Проверка распределения
SELECT 
    gp_segment_id,
    count(*) as row_count
FROM customers_replicated
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- --------------------------------------------------------------------------------
-- 2.3 Создание таблицы с RANDOM распределением
-- --------------------------------------------------------------------------------

-- Таблица продаж с RANDOM распределением
CREATE TABLE sales_random (
    sale_id BIGSERIAL,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    sale_date DATE NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    region VARCHAR(50),
    payment_method VARCHAR(20)
) DISTRIBUTED RANDOMLY;

-- --------------------------------------------------------------------------------
-- 2.4 Просмотр информации о таблицах
-- --------------------------------------------------------------------------------
SELECT * FROM gp_distribution_policy LIMIT 1;

-- Информация о распределении таблиц
-- Самый простой и понятный вариант
-- Правильный запрос для Yandex MPP Analytics
-- Упрощенная версия
SELECT 
    n.nspname as schemaname,
    c.relname as tablename,
    CASE 
        WHEN p.policytype = 'p' THEN 'RANDOMLY'
        WHEN p.policytype = 'r' THEN 'REPLICATED'
        WHEN p.policytype = 'e' THEN 'HASH(' || array_to_string(
            ARRAY(
                SELECT a.attname 
                FROM pg_attribute a 
                WHERE a.attrelid = c.oid 
                AND a.attnum = ANY(p.distkey)
                ORDER BY a.attnum
            ), ', '
        ) || ')'
        ELSE 'UNKNOWN'
    END as distribution_strategy,
    p.policytype as policy_code,
    p.distkey as distribution_columns,
    pg_size_pretty(pg_total_relation_size(c.oid)) as table_size,
    c.reltuples::bigint as estimated_rows
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN gp_distribution_policy p ON c.oid = p.localoid
WHERE n.nspname = 'lab1'
AND c.relkind = 'r'
ORDER BY c.relname;
-- ================================================================================
-- ЧАСТЬ 3: ЗАГРУЗКА ТЕСТОВЫХ ДАННЫХ
-- ================================================================================

-- --------------------------------------------------------------------------------
-- 3.1 Загрузка данных в справочники
-- --------------------------------------------------------------------------------

-- Вставка данных в products_replicated
INSERT INTO products_replicated (product_id, product_name, category, subcategory, brand, unit_cost)
SELECT 
    generate_series(1, 1000) as product_id,
    'Product ' || generate_series(1, 1000) as product_name,
    CASE (random() * 5)::int
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        WHEN 2 THEN 'Food'
        WHEN 3 THEN 'Books'
        WHEN 4 THEN 'Toys'
        ELSE 'Other'
    END as category,
    'Subcategory ' || (random() * 20)::int as subcategory,
    'Brand ' || (random() * 50)::int as brand,
    (random() * 1000 + 10)::decimal(10,2) as unit_cost;

-- Вставка данных в customers_replicated
INSERT INTO customers_replicated (customer_id, customer_name, email, registration_date, customer_segment, loyalty_level)
SELECT 
    generate_series(1, 10000) as customer_id,
    'Customer ' || generate_series(1, 10000) as customer_name,
    'customer' || generate_series(1, 10000) || '@example.com' as email,
    CURRENT_DATE - (random() * 1000)::int as registration_date,
    CASE (random() * 3)::int
        WHEN 0 THEN 'Individual'
        WHEN 1 THEN 'Small Business'
        WHEN 2 THEN 'Enterprise'
        ELSE 'Government'
    END as customer_segment,
    CASE (random() * 3)::int
        WHEN 0 THEN 'Bronze'
        WHEN 1 THEN 'Silver'
        WHEN 2 THEN 'Gold'
        ELSE 'Platinum'
    END as loyalty_level;

-- --------------------------------------------------------------------------------
-- 3.2 Генерация больших объемов данных для таблиц продаж
-- --------------------------------------------------------------------------------

-- Вставка 1 миллиона записей в sales_hash
INSERT INTO sales_hash (customer_id, product_id, sale_date, quantity, unit_price, total_amount, region, payment_method)
SELECT 
    (random() * 9999 + 1)::int as customer_id,
    (random() * 999 + 1)::int as product_id,
    CURRENT_DATE - (random() * 365)::int as sale_date,
    (random() * 10 + 1)::int as quantity,
    (random() * 100 + 5)::decimal(10,2) as unit_price,
    ((random() * 10 + 1) * (random() * 100 + 5))::decimal(12,2) as total_amount,
    CASE (random() * 4)::int
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        WHEN 3 THEN 'West'
        ELSE 'Central'
    END as region,
    CASE (random() * 3)::int
        WHEN 0 THEN 'Credit Card'
        WHEN 1 THEN 'Cash'
        WHEN 2 THEN 'PayPal'
        ELSE 'Bank Transfer'
    END as payment_method
FROM generate_series(1, 1000000);

-- Вставка тех же данных в sales_random
INSERT INTO sales_random 
SELECT * FROM sales_hash;

-- --------------------------------------------------------------------------------
-- 3.3 Проверка распределения данных
-- --------------------------------------------------------------------------------

-- Проверка равномерности распределения для sales_hash
SELECT 
    gp_segment_id,
    count(*) as row_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage
FROM sales_hash
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- Проверка распределения для sales_random
SELECT 
    gp_segment_id,
    count(*) as row_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage
FROM sales_random
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- ================================================================================
-- ЧАСТЬ 4: СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ ЗАПРОСОВ
-- ================================================================================

-- --------------------------------------------------------------------------------
-- 4.1 Запросы с JOIN по distribution key
-- --------------------------------------------------------------------------------

-- Тест 1: JOIN sales_hash с customers_replicated
EXPLAIN ANALYZE
SELECT 
    c.customer_segment,
    COUNT(*) as order_count,
    SUM(s.total_amount) as total_revenue
FROM sales_hash s
INNER JOIN customers_replicated c ON s.customer_id = c.customer_id
WHERE s.sale_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

-- --------------------------------------------------------------------------------
-- 4.2 Запросы со случайным распределением
-- --------------------------------------------------------------------------------

-- Тест 2: Тот же запрос для sales_random
EXPLAIN ANALYZE
SELECT 
    c.customer_segment,
    COUNT(*) as order_count,
    SUM(s.total_amount) as total_revenue
FROM sales_random s
INNER JOIN customers_replicated c ON s.customer_id = c.customer_id
WHERE s.sale_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

-- --------------------------------------------------------------------------------
-- 4.3 Агрегация по distribution key
-- --------------------------------------------------------------------------------

-- Тест 3: Агрегация по customer_id (distribution key) на sales_hash
EXPLAIN ANALYZE
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value
FROM sales_hash
WHERE sale_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY customer_id
HAVING COUNT(*) > 5
ORDER BY total_spent DESC
LIMIT 100;

-- Тест 4: Та же агрегация на sales_random
EXPLAIN ANALYZE
SELECT 
    customer_id,
    COUNT(*) as order_count,
    SUM(total_amount) as total_spent,
    AVG(total_amount) as avg_order_value
FROM sales_random
WHERE sale_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY customer_id
HAVING COUNT(*) > 5
ORDER BY total_spent DESC
LIMIT 100;

-- --------------------------------------------------------------------------------
-- 4.4 Сравнение с JOIN маленькой таблицы
-- --------------------------------------------------------------------------------

-- Создание customers с HASH распределением
CREATE TABLE customers_hash (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    registration_date DATE NOT NULL,
    customer_segment VARCHAR(50),
    loyalty_level VARCHAR(20)
) DISTRIBUTED BY (customer_id);

-- Копирование данных
INSERT INTO customers_hash SELECT * FROM customers_replicated;

-- Тест 5: JOIN sales_hash с customers_hash (оба по customer_id)
EXPLAIN ANALYZE
SELECT 
    c.customer_segment,
    COUNT(DISTINCT s.customer_id) as unique_customers,
    COUNT(*) as order_count,
    SUM(s.total_amount) as total_revenue
FROM sales_hash s
INNER JOIN customers_hash c ON s.customer_id = c.customer_id
WHERE s.sale_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY c.customer_segment
ORDER BY total_revenue DESC;

-- --------------------------------------------------------------------------------
-- 4.5 Создание таблицы результатов
-- --------------------------------------------------------------------------------

CREATE TABLE performance_results (
    test_id SERIAL PRIMARY KEY,
    test_name VARCHAR(200),
    table_config VARCHAR(100),
    execution_time_ms DECIMAL(10, 2),
    rows_processed INTEGER,
    redistribute_motion BOOLEAN,
    notes TEXT
);

-- Пример заполнения результатами тестов
INSERT INTO performance_results (test_name, table_config, execution_time_ms, rows_processed, redistribute_motion, notes)
VALUES 
    ('JOIN с реплицированной таблицей', 'sales_hash + customers_replicated', 0.00, 1000000, false, 'Укажите фактическое время'),
    ('JOIN с реплицированной таблицей', 'sales_random + customers_replicated', 0.00, 1000000, false, 'Укажите фактическое время');

-- ================================================================================
-- ДОПОЛНИТЕЛЬНЫЕ ЗАДАНИЯ
-- ================================================================================

-- --------------------------------------------------------------------------------
-- Задание 1: Разные ключи распределения
-- --------------------------------------------------------------------------------

-- Создание таблицы с распределением по product_id
CREATE TABLE sales_by_product (
    LIKE sales_hash
) DISTRIBUTED BY (product_id);

INSERT INTO sales_by_product SELECT * FROM sales_hash;

-- Сравнение производительности для запросов по продуктам
EXPLAIN ANALYZE
SELECT 
    product_id,
    COUNT(*) as sales_count,
    SUM(total_amount) as total_revenue
FROM sales_by_product
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 100;

-- --------------------------------------------------------------------------------
-- Задание 2: Анализ skew (неравномерности)
-- --------------------------------------------------------------------------------

-- Поиск "горячих" клиентов
SELECT 
    customer_id,
    count(*) as order_count
FROM sales_hash
GROUP BY customer_id
ORDER BY order_count DESC
LIMIT 20;

-- Проверка skew на уровне сегментов
SELECT 
    gp_segment_id,
    count(*) as row_count,
    min(customer_id) as min_cust,
    max(customer_id) as max_cust
FROM sales_hash
GROUP BY gp_segment_id
ORDER BY row_count DESC;

-- ================================================================================
-- ОЧИСТКА РЕСУРСОВ
-- ================================================================================

-- Удаление таблиц
DROP TABLE IF EXISTS performance_results;
DROP TABLE IF EXISTS sales_by_product;
DROP TABLE IF EXISTS customers_hash;
DROP TABLE IF EXISTS sales_random;
DROP TABLE IF EXISTS sales_hash;
DROP TABLE IF EXISTS customers_replicated;
DROP TABLE IF EXISTS products_replicated;

-- Удаление схемы
DROP SCHEMA IF EXISTS lab1 CASCADE;

-- ================================================================================
-- КОНЕЦ ФАЙЛА
-- ================================================================================
