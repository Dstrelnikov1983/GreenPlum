-- ============================================
-- Модуль 1: Примеры SQL для практики
-- Тема: Стратегии распределения данных
-- ============================================

-- Подключение к базе данных
-- psql -h <master_host> -p 6432 -U admin -d postgres

-- ============================================
-- 1. HASH DISTRIBUTION (Хеш-распределение)
-- ============================================

-- Пример 1: Таблица транзакций (большая фактовая таблица)
CREATE TABLE transactions (
    transaction_id BIGSERIAL,
    user_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    status VARCHAR(20),
    payment_method VARCHAR(50)
) DISTRIBUTED BY (user_id);

-- Почему user_id? 
-- - Часто используется в JOIN с таблицей users
-- - Хорошая кардинальность (много уникальных значений)
-- - Равномерное распределение

-- Пример 2: Таблица заказов
CREATE TABLE orders (
    order_id BIGSERIAL,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    delivery_date DATE,
    total_amount NUMERIC(12,2),
    shipping_address TEXT,
    order_status VARCHAR(30)
) DISTRIBUTED BY (customer_id);

-- Вставка тестовых данных
INSERT INTO orders (customer_id, order_date, total_amount, order_status)
SELECT 
    (random() * 100000)::integer + 1 as customer_id,
    current_date - (random() * 730)::integer as order_date,
    (random() * 5000 + 10)::numeric(12,2) as total_amount,
    CASE (random() * 4)::integer
        WHEN 0 THEN 'Pending'
        WHEN 1 THEN 'Processing'
        WHEN 2 THEN 'Shipped'
        ELSE 'Delivered'
    END as order_status
FROM generate_series(1, 100000);

-- Проверка распределения данных по сегментам
SELECT 
    gp_segment_id,
    count(*) as row_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage
FROM orders
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- ============================================
-- 2. RANDOM DISTRIBUTION (Случайное распределение)
-- ============================================

-- Пример 1: Таблица логов (staging таблица)
CREATE TABLE application_logs (
    log_id BIGSERIAL,
    log_timestamp TIMESTAMP DEFAULT now(),
    log_level VARCHAR(10),
    application_name VARCHAR(100),
    message TEXT,
    user_id INTEGER,
    session_id VARCHAR(100)
) DISTRIBUTED RANDOMLY;

-- Почему RANDOMLY?
-- - Нет явного ключа для распределения
-- - Данные загружаются пачками
-- - Часто используется как промежуточная таблица

-- Пример 2: Temporary staging таблица
CREATE TEMP TABLE staging_data (
    raw_data TEXT,
    load_timestamp TIMESTAMP DEFAULT now()
) DISTRIBUTED RANDOMLY;

-- Вставка данных
INSERT INTO application_logs (log_level, application_name, message, user_id)
SELECT 
    CASE (random() * 3)::integer
        WHEN 0 THEN 'INFO'
        WHEN 1 THEN 'WARNING'
        ELSE 'ERROR'
    END as log_level,
    'app_' || (random() * 10)::integer as application_name,
    'Log message ' || generate_series as message,
    (random() * 1000)::integer as user_id
FROM generate_series(1, 50000);

-- Проверка распределения
SELECT 
    gp_segment_id,
    count(*) as row_count
FROM application_logs
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- ============================================
-- 3. REPLICATED DISTRIBUTION (Репликация)
-- ============================================

-- Пример 1: Справочник стран (очень маленькая таблица)
CREATE TABLE countries (
    country_id SERIAL PRIMARY KEY,
    country_code CHAR(2) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    population BIGINT
) DISTRIBUTED REPLICATED;

-- Вставка данных
INSERT INTO countries (country_code, country_name, region, population) VALUES
('US', 'United States', 'North America', 331000000),
('CN', 'China', 'Asia', 1400000000),
('IN', 'India', 'Asia', 1380000000),
('RU', 'Russia', 'Europe', 146000000),
('BR', 'Brazil', 'South America', 212000000),
('JP', 'Japan', 'Asia', 126000000),
('DE', 'Germany', 'Europe', 83000000),
('GB', 'United Kingdom', 'Europe', 67000000),
('FR', 'France', 'Europe', 65000000),
('IT', 'Italy', 'Europe', 60000000);

-- Пример 2: Справочник категорий продуктов
CREATE TABLE product_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER,
    description TEXT,
    is_active BOOLEAN DEFAULT true
) DISTRIBUTED REPLICATED;

INSERT INTO product_categories (category_name, description) VALUES
('Electronics', 'Electronic devices and accessories'),
('Clothing', 'Apparel and fashion items'),
('Food', 'Food and beverages'),
('Books', 'Physical and digital books'),
('Home & Garden', 'Home improvement and garden supplies'),
('Sports', 'Sports equipment and accessories'),
('Toys', 'Toys and games'),
('Beauty', 'Beauty and personal care products');

-- Проверка репликации (данные должны быть на всех сегментах)
SELECT 
    gp_segment_id,
    count(*) as row_count
FROM product_categories
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- Должны видеть одинаковое количество на всех сегментах!

-- ============================================
-- 4. СРАВНЕНИЕ ПРОИЗВОДИТЕЛЬНОСТИ
-- ============================================

-- Создадим таблицу пользователей для JOIN
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    country_code CHAR(2),
    registration_date DATE,
    is_active BOOLEAN DEFAULT true
) DISTRIBUTED REPLICATED; -- Маленькая таблица, реплицируем

-- Заполнение данных
INSERT INTO users (user_id, username, email, country_code, registration_date)
SELECT 
    generate_series as user_id,
    'user_' || generate_series as username,
    'user' || generate_series || '@example.com' as email,
    CASE (random() * 9)::integer
        WHEN 0 THEN 'US'
        WHEN 1 THEN 'CN'
        WHEN 2 THEN 'IN'
        WHEN 3 THEN 'RU'
        WHEN 4 THEN 'BR'
        WHEN 5 THEN 'JP'
        WHEN 6 THEN 'DE'
        WHEN 7 THEN 'GB'
        WHEN 8 THEN 'FR'
        ELSE 'IT'
    END as country_code,
    current_date - (random() * 1095)::integer as registration_date
FROM generate_series(1, 10000);

-- ============================================
-- JOIN с оптимизацией (replicated table)
-- ============================================

-- Запрос 1: JOIN с replicated таблицей (быстро)
EXPLAIN ANALYZE
SELECT 
    u.country_code,
    c.country_name,
    count(*) as user_count,
    count(DISTINCT t.transaction_id) as transaction_count,
    sum(t.amount) as total_amount
FROM users u
JOIN countries c ON u.country_code = c.country_code
LEFT JOIN transactions t ON u.user_id = t.user_id
GROUP BY u.country_code, c.country_name
ORDER BY user_count DESC;

-- Обратите внимание: нет Redistribute Motion для replicated таблиц!

-- ============================================
-- 5. ПАРТИЦИОНИРОВАНИЕ (будет подробно в Модуле 2)
-- ============================================

-- Пример партиционированной таблицы по диапазону дат
CREATE TABLE sales_partitioned (
    sale_id BIGSERIAL,
    sale_date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    amount NUMERIC(12,2) NOT NULL,
    region VARCHAR(50)
) DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (sale_date)
(
    PARTITION sales_2023_q1 START ('2023-01-01') END ('2023-04-01'),
    PARTITION sales_2023_q2 START ('2023-04-01') END ('2023-07-01'),
    PARTITION sales_2023_q3 START ('2023-07-01') END ('2023-10-01'),
    PARTITION sales_2023_q4 START ('2023-10-01') END ('2024-01-01'),
    PARTITION sales_2024_q1 START ('2024-01-01') END ('2024-04-01'),
    DEFAULT PARTITION sales_other
);

-- Вставка данных в партиции
INSERT INTO sales_partitioned (sale_date, product_id, customer_id, amount, region)
SELECT 
    '2023-01-01'::date + (random() * 365)::integer as sale_date,
    (random() * 1000)::integer + 1 as product_id,
    (random() * 10000)::integer + 1 as customer_id,
    (random() * 1000)::numeric(12,2) as amount,
    CASE (random() * 3)::integer
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        ELSE 'West'
    END as region
FROM generate_series(1, 100000);

-- Запрос с partition pruning
EXPLAIN
SELECT 
    region,
    count(*) as sales_count,
    sum(amount) as total_amount
FROM sales_partitioned
WHERE sale_date BETWEEN '2023-07-01' AND '2023-09-30'
GROUP BY region;

-- Должны увидеть, что сканируется только одна партиция (Q3)!

-- ============================================
-- 6. АНАЛИЗ ПЕРЕКОСА ДАННЫХ (Data Skew)
-- ============================================

-- Пример плохого distribution key
CREATE TABLE bad_distribution_example (
    id SERIAL,
    status VARCHAR(20), -- ПЛОХОЙ ВЫБОР: мало уникальных значений
    user_id INTEGER,
    data TEXT
) DISTRIBUTED BY (status); -- Только несколько уникальных значений!

-- Заполнение данных
INSERT INTO bad_distribution_example (status, user_id, data)
SELECT 
    CASE (random() * 2)::integer
        WHEN 0 THEN 'active'
        WHEN 1 THEN 'inactive'
        ELSE 'pending'
    END as status,
    (random() * 1000)::integer as user_id,
    md5(random()::text) as data
FROM generate_series(1, 100000);

-- Анализ перекоса
SELECT 
    gp_segment_id,
    count(*) as row_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage,
    CASE 
        WHEN round(100.0 * count(*) / sum(count(*)) OVER (), 2) > 150.0 / (SELECT count(DISTINCT gp_segment_id) FROM bad_distribution_example)
        THEN '⚠️  SKEW DETECTED!'
        ELSE '✓ OK'
    END as skew_status
FROM bad_distribution_example
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- Правильная версия
CREATE TABLE good_distribution_example (
    id SERIAL,
    status VARCHAR(20),
    user_id INTEGER,
    data TEXT
) DISTRIBUTED BY (user_id); -- ХОРОШИЙ ВЫБОР: много уникальных значений

-- ============================================
-- 7. ПОЛЕЗНЫЕ ЗАПРОСЫ ДЛЯ АНАЛИЗА
-- ============================================

-- Просмотр всех таблиц и их стратегий распределения
SELECT 
    schemaname,
    tablename,
    CASE 
        WHEN attrnums IS NULL THEN 'REPLICATED'
        WHEN array_length(attrnums, 1) = 0 THEN 'RANDOM'
        ELSE 'HASH (' || array_to_string(
            ARRAY(
                SELECT a.attname 
                FROM pg_attribute a 
                WHERE a.attrelid = c.oid 
                AND a.attnum = ANY(p.attrnums)
            ), ', '
        ) || ')'
    END as distribution_strategy
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
LEFT JOIN gp_distribution_policy p ON c.oid = p.localoid
WHERE n.nspname = 'public'
AND c.relkind = 'r'
ORDER BY schemaname, tablename;

-- Размер таблиц на каждом сегменте
SELECT 
    tablename,
    gp_segment_id,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables, 
     (SELECT gp_segment_id FROM gp_segment_configuration WHERE role = 'p' AND content >= 0) as segments
WHERE schemaname = 'public'
ORDER BY tablename, gp_segment_id;

-- Статистика распределения для конкретной таблицы
CREATE OR REPLACE FUNCTION check_distribution(table_name text)
RETURNS TABLE (
    segment_id integer,
    row_count bigint,
    percentage numeric,
    deviation_from_avg numeric
) AS $$
DECLARE
    avg_count numeric;
BEGIN
    -- Вычисляем средний count
    EXECUTE format('SELECT avg(cnt) FROM (SELECT count(*) as cnt FROM %I GROUP BY gp_segment_id) t', table_name)
    INTO avg_count;
    
    -- Возвращаем статистику
    RETURN QUERY EXECUTE format('
        SELECT 
            gp_segment_id::integer,
            count(*)::bigint as row_count,
            round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage,
            round(100.0 * (count(*) - %s) / %s, 2) as deviation_from_avg
        FROM %I
        GROUP BY gp_segment_id
        ORDER BY gp_segment_id
    ', avg_count, avg_count, table_name);
END;
$$ LANGUAGE plpgsql;

-- Использование функции
SELECT * FROM check_distribution('orders');

-- ============================================
-- 8. CLEANUP (если нужно)
-- ============================================

-- Удаление всех тестовых таблиц
/*
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS application_logs CASCADE;
DROP TABLE IF EXISTS countries CASCADE;
DROP TABLE IF EXISTS product_categories CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS sales_partitioned CASCADE;
DROP TABLE IF EXISTS bad_distribution_example CASCADE;
DROP TABLE IF EXISTS good_distribution_example CASCADE;
DROP FUNCTION IF EXISTS check_distribution(text);
*/

-- ============================================
-- ЗАДАНИЯ ДЛЯ САМОСТОЯТЕЛЬНОЙ РАБОТЫ
-- ============================================

/*
Задание 1: Создайте схему данных для интернет-магазина
- Таблица товаров (100,000 записей)
- Таблица клиентов (50,000 записей)
- Таблица заказов (500,000 записей)
- Таблица позиций заказов (2,000,000 записей)

Выберите правильные стратегии распределения для каждой таблицы.

Задание 2: Напишите запрос с JOIN всех четырех таблиц
Проанализируйте EXPLAIN ANALYZE результат.

Задание 3: Сравните производительность
- Создайте две версии таблицы заказов: с HASH и RANDOM
- Выполните одинаковые запросы
- Сравните время выполнения

Задание 4: Найдите data skew
- Создайте таблицу с плохим distribution key
- Выявите перекос
- Пересоздайте с правильным ключом
*/
