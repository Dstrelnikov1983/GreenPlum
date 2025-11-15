-- ================================================================================
-- РАСШИРЕННЫЕ ПРИМЕРЫ: ДЕМОНСТРАЦИЯ РЕАЛЬНЫХ РАЗЛИЧИЙ В ПРОИЗВОДИТЕЛЬНОСТИ
-- Тема: Сравнение стратегий распределения данных в Yandex MPP Analytics
-- ================================================================================
-- Цель: Показать явные преимущества и недостатки каждой стратегии распределения
-- ================================================================================

-- ================================================================================
-- ПОДГОТОВКА: СОЗДАНИЕ ТЕСТОВЫХ ТАБЛИЦ
-- ================================================================================

-- Создаем схему для расширенных тестов
CREATE SCHEMA IF NOT EXISTS lab1_advanced;
SET search_path TO lab1_advanced, public;

-- --------------------------------------------------------------------------------
-- НАБОР 1: Таблицы для демонстрации CO-LOCATED JOIN
-- --------------------------------------------------------------------------------

-- Таблица заказов с HASH по customer_id (1 млн записей)
DROP TABLE IF EXISTS orders_hash CASCADE;
CREATE TABLE orders_hash (
    order_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date DATE NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    status VARCHAR(20),
    shipping_region VARCHAR(50)
) DISTRIBUTED BY (customer_id);

-- Таблица платежей с HASH по customer_id (2 млн записей)
DROP TABLE IF EXISTS payments_hash CASCADE;
CREATE TABLE payments_hash (
    payment_id BIGSERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    payment_date TIMESTAMP NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    payment_method VARCHAR(30),
    status VARCHAR(20)
) DISTRIBUTED BY (customer_id);

-- Таблица заказов с RANDOM распределением (для сравнения)
DROP TABLE IF EXISTS orders_random CASCADE;
CREATE TABLE orders_random (
    LIKE orders_hash INCLUDING ALL
) DISTRIBUTED RANDOMLY;

-- Таблица платежей с RANDOM распределением (для сравнения)
DROP TABLE IF EXISTS payments_random CASCADE;
CREATE TABLE payments_random (
    LIKE payments_hash INCLUDING ALL
) DISTRIBUTED RANDOMLY;

-- Справочник клиентов (REPLICATED)
DROP TABLE IF EXISTS customers_dim CASCADE;
CREATE TABLE customers_dim (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(100),
    registration_date DATE NOT NULL,
    customer_segment VARCHAR(50),
    lifetime_value DECIMAL(12, 2)
) DISTRIBUTED REPLICATED;

-- --------------------------------------------------------------------------------
-- НАБОР 2: Таблицы для демонстрации GROUP BY оптимизации
-- --------------------------------------------------------------------------------

-- Большая таблица транзакций с HASH по account_id
DROP TABLE IF EXISTS transactions_hash CASCADE;
CREATE TABLE transactions_hash (
    transaction_id BIGSERIAL,
    account_id INTEGER NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    amount DECIMAL(12, 2) NOT NULL,
    transaction_type VARCHAR(20),
    merchant_category VARCHAR(50)
) DISTRIBUTED BY (account_id);

-- Та же таблица с RANDOM распределением
DROP TABLE IF EXISTS transactions_random CASCADE;
CREATE TABLE transactions_random (
    LIKE transactions_hash INCLUDING ALL
) DISTRIBUTED RANDOMLY;

-- ================================================================================
-- ЗАГРУЗКА ТЕСТОВЫХ ДАННЫХ
-- ================================================================================

-- --------------------------------------------------------------------------------
-- Загрузка данных в справочник клиентов (50,000 клиентов)
-- --------------------------------------------------------------------------------
INSERT INTO customers_dim (customer_id, customer_name, email, registration_date, customer_segment, lifetime_value)
SELECT 
    generate_series(1, 50000) as customer_id,
    'Customer ' || generate_series(1, 50000) as customer_name,
    'customer' || generate_series(1, 50000) || '@example.com' as email,
    CURRENT_DATE - (random() * 1500)::int as registration_date,
    CASE (random() * 4)::int
        WHEN 0 THEN 'VIP'
        WHEN 1 THEN 'Premium'
        WHEN 2 THEN 'Standard'
        WHEN 3 THEN 'Basic'
        ELSE 'Trial'
    END as customer_segment,
    (random() * 50000 + 100)::decimal(12,2) as lifetime_value;

-- --------------------------------------------------------------------------------
-- Загрузка заказов (1 млн записей)
-- --------------------------------------------------------------------------------
INSERT INTO orders_hash (customer_id, order_date, total_amount, status, shipping_region)
SELECT 
    (random() * 49999 + 1)::int as customer_id,
    CURRENT_DATE - (random() * 730)::int as order_date,
    (random() * 5000 + 50)::decimal(12,2) as total_amount,
    CASE (random() * 4)::int
        WHEN 0 THEN 'completed'
        WHEN 1 THEN 'pending'
        WHEN 2 THEN 'cancelled'
        WHEN 3 THEN 'shipped'
        ELSE 'processing'
    END as status,
    CASE (random() * 5)::int
        WHEN 0 THEN 'North America'
        WHEN 1 THEN 'Europe'
        WHEN 2 THEN 'Asia Pacific'
        WHEN 3 THEN 'Latin America'
        WHEN 4 THEN 'Middle East'
        ELSE 'Africa'
    END as shipping_region
FROM generate_series(1, 1000000);

-- Копируем данные в orders_random
INSERT INTO orders_random SELECT * FROM orders_hash;

-- --------------------------------------------------------------------------------
-- Загрузка платежей (2 млн записей)
-- --------------------------------------------------------------------------------
INSERT INTO payments_hash (customer_id, payment_date, amount, payment_method, status)
SELECT 
    (random() * 49999 + 1)::int as customer_id,
    CURRENT_TIMESTAMP - (random() * 730 || ' days')::interval as payment_date,
    (random() * 5000 + 10)::decimal(12,2) as amount,
    CASE (random() * 5)::int
        WHEN 0 THEN 'Credit Card'
        WHEN 1 THEN 'PayPal'
        WHEN 2 THEN 'Bank Transfer'
        WHEN 3 THEN 'Apple Pay'
        WHEN 4 THEN 'Google Pay'
        ELSE 'Cryptocurrency'
    END as payment_method,
    CASE (random() * 3)::int
        WHEN 0 THEN 'successful'
        WHEN 1 THEN 'pending'
        WHEN 2 THEN 'failed'
        ELSE 'refunded'
    END as status
FROM generate_series(1, 2000000);

-- Копируем данные в payments_random
INSERT INTO payments_random SELECT * FROM payments_hash;

-- --------------------------------------------------------------------------------
-- Загрузка транзакций (5 млн записей)
-- --------------------------------------------------------------------------------
INSERT INTO transactions_hash (account_id, transaction_date, amount, transaction_type, merchant_category)
SELECT 
    (random() * 99999 + 1)::int as account_id,
    CURRENT_TIMESTAMP - (random() * 365 || ' days')::interval as transaction_date,
    (random() * 10000 - 5000)::decimal(12,2) as amount,
    CASE (random() * 3)::int
        WHEN 0 THEN 'purchase'
        WHEN 1 THEN 'refund'
        WHEN 2 THEN 'transfer'
        ELSE 'withdrawal'
    END as transaction_type,
    CASE (random() * 10)::int
        WHEN 0 THEN 'Groceries'
        WHEN 1 THEN 'Electronics'
        WHEN 2 THEN 'Restaurants'
        WHEN 3 THEN 'Transportation'
        WHEN 4 THEN 'Entertainment'
        WHEN 5 THEN 'Healthcare'
        WHEN 6 THEN 'Education'
        WHEN 7 THEN 'Utilities'
        WHEN 8 THEN 'Travel'
        WHEN 9 THEN 'Shopping'
        ELSE 'Other'
    END as merchant_category
FROM generate_series(1, 5000000);

-- Копируем данные в transactions_random
INSERT INTO transactions_random SELECT * FROM transactions_hash;

-- Запускаем ANALYZE для обновления статистики
ANALYZE orders_hash;
ANALYZE orders_random;
ANALYZE payments_hash;
ANALYZE payments_random;
ANALYZE customers_dim;
ANALYZE transactions_hash;
ANALYZE transactions_random;

-- ================================================================================
-- ТЕСТ 1: CO-LOCATED JOIN (Преимущество HASH над RANDOM)
-- ================================================================================
-- Сравнение: JOIN двух больших таблиц по distribution key
-- ================================================================================

\echo '==================================================================================='
\echo 'ТЕСТ 1A: CO-LOCATED JOIN - обе таблицы HASH по customer_id'
\echo 'ОЖИДАНИЕ: НЕТ Redistribute Motion, локальный JOIN на каждом сегменте'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    o.customer_id,
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(DISTINCT p.payment_id) as payment_count,
    SUM(o.total_amount) as total_orders,
    SUM(p.amount) as total_payments,
    SUM(o.total_amount) - SUM(p.amount) as balance
FROM orders_hash o
INNER JOIN payments_hash p ON o.customer_id = p.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '180 days'
  AND p.payment_date >= CURRENT_TIMESTAMP - INTERVAL '180 days'
GROUP BY o.customer_id
HAVING COUNT(DISTINCT o.order_id) > 5
ORDER BY balance DESC
LIMIT 1000;

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 1B: JOIN с RANDOM распределением - обе таблицы RANDOMLY'
\echo 'ОЖИДАНИЕ: Redistribute Motion на ОБЕИХ таблицах (миллионы строк)'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    o.customer_id,
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(DISTINCT p.payment_id) as payment_count,
    SUM(o.total_amount) as total_orders,
    SUM(p.amount) as total_payments,
    SUM(o.total_amount) - SUM(p.amount) as balance
FROM orders_random o
INNER JOIN payments_random p ON o.customer_id = p.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '180 days'
  AND p.payment_date >= CURRENT_TIMESTAMP - INTERVAL '180 days'
GROUP BY o.customer_id
HAVING COUNT(DISTINCT o.order_id) > 5
ORDER BY balance DESC
LIMIT 1000;

-- ================================================================================
-- ТЕСТ 2: JOIN С REPLICATED ТАБЛИЦЕЙ
-- ================================================================================
-- Сравнение: Влияние REPLICATED таблицы на производительность
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 2A: JOIN HASH таблицы с REPLICATED справочником'
\echo 'ОЖИДАНИЕ: Локальный JOIN без перемещения данных'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    c.customer_segment,
    c.lifetime_value,
    COUNT(*) as order_count,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.total_amount) as max_order_value
FROM orders_hash o
INNER JOIN customers_dim c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
  AND o.status = 'completed'
GROUP BY c.customer_segment, c.lifetime_value
HAVING SUM(o.total_amount) > 10000
ORDER BY total_revenue DESC;

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 2B: JOIN RANDOM таблицы с REPLICATED справочником'
\echo 'ОЖИДАНИЕ: Также локальный JOIN (REPLICATED нивелирует разницу)'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    c.customer_segment,
    c.lifetime_value,
    COUNT(*) as order_count,
    SUM(o.total_amount) as total_revenue,
    AVG(o.total_amount) as avg_order_value,
    MAX(o.total_amount) as max_order_value
FROM orders_random o
INNER JOIN customers_dim c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '90 days'
  AND o.status = 'completed'
GROUP BY c.customer_segment, c.lifetime_value
HAVING SUM(o.total_amount) > 10000
ORDER BY total_revenue DESC;

-- ================================================================================
-- ТЕСТ 3: GROUP BY ПО DISTRIBUTION KEY
-- ================================================================================
-- Сравнение: Агрегация по ключу распределения vs произвольному полю
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 3A: GROUP BY по distribution key (account_id) на HASH таблице'
\echo 'ОЖИДАНИЕ: Локальная агрегация на каждом сегменте, минимальный Redistribute'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    account_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction,
    COUNT(DISTINCT merchant_category) as category_count
FROM transactions_hash
WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY account_id
HAVING COUNT(*) >= 10
ORDER BY total_amount DESC
LIMIT 5000;

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 3B: GROUP BY по account_id на RANDOM таблице'
\echo 'ОЖИДАНИЕ: Массивный Redistribute Motion всех данных'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    account_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction,
    COUNT(DISTINCT merchant_category) as category_count
FROM transactions_random
WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY account_id
HAVING COUNT(*) >= 10
ORDER BY total_amount DESC
LIMIT 5000;

-- ================================================================================
-- ТЕСТ 4: GROUP BY ПО НЕ-DISTRIBUTION KEY
-- ================================================================================
-- Демонстрация: Когда HASH и RANDOM показывают одинаковую производительность
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 4A: GROUP BY по merchant_category (НЕ distribution key) на HASH'
\echo 'ОЖИДАНИЕ: Redistribute Motion требуется в обоих случаях'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    merchant_category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT account_id) as unique_accounts
FROM transactions_hash
WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
  AND amount > 0
GROUP BY merchant_category
ORDER BY total_amount DESC;

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 4B: GROUP BY по merchant_category на RANDOM'
\echo 'ОЖИДАНИЕ: Аналогичный план с Redistribute Motion'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    merchant_category,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT account_id) as unique_accounts
FROM transactions_random
WHERE transaction_date >= CURRENT_DATE - INTERVAL '90 days'
  AND amount > 0
GROUP BY merchant_category
ORDER BY total_amount DESC;

-- ================================================================================
-- ТЕСТ 5: COMPLEX MULTI-JOIN QUERY
-- ================================================================================
-- Демонстрация: Влияние на сложные запросы с несколькими JOIN
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 5A: Сложный запрос с тремя JOIN (все HASH + REPLICATED)'
\echo 'ОЖИДАНИЕ: Минимальное перемещение данных'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    c.customer_segment,
    DATE_TRUNC('month', o.order_date) as order_month,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(DISTINCT p.payment_id) as payment_count,
    SUM(o.total_amount) as total_orders,
    SUM(p.amount) as total_payments,
    SUM(CASE WHEN p.status = 'successful' THEN p.amount ELSE 0 END) as successful_payments,
    ROUND(AVG(o.total_amount), 2) as avg_order_value
FROM orders_hash o
INNER JOIN payments_hash p ON o.customer_id = p.customer_id
INNER JOIN customers_dim c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '180 days'
  AND p.payment_date >= CURRENT_TIMESTAMP - INTERVAL '180 days'
  AND o.status IN ('completed', 'shipped')
GROUP BY c.customer_segment, DATE_TRUNC('month', o.order_date)
HAVING COUNT(DISTINCT o.order_id) > 100
ORDER BY order_month DESC, total_orders DESC;

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 5B: Тот же запрос с RANDOM таблицами'
\echo 'ОЖИДАНИЕ: Множественные Redistribute Motion операции'
\echo '==================================================================================='

EXPLAIN ANALYZE
SELECT 
    c.customer_segment,
    DATE_TRUNC('month', o.order_date) as order_month,
    COUNT(DISTINCT o.customer_id) as unique_customers,
    COUNT(DISTINCT o.order_id) as order_count,
    COUNT(DISTINCT p.payment_id) as payment_count,
    SUM(o.total_amount) as total_orders,
    SUM(p.amount) as total_payments,
    SUM(CASE WHEN p.status = 'successful' THEN p.amount ELSE 0 END) as successful_payments,
    ROUND(AVG(o.total_amount), 2) as avg_order_value
FROM orders_random o
INNER JOIN payments_random p ON o.customer_id = p.customer_id
INNER JOIN customers_dim c ON o.customer_id = c.customer_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '180 days'
  AND p.payment_date >= CURRENT_TIMESTAMP - INTERVAL '180 days'
  AND o.status IN ('completed', 'shipped')
GROUP BY c.customer_segment, DATE_TRUNC('month', o.order_date)
HAVING COUNT(DISTINCT o.order_id) > 100
ORDER BY order_month DESC, total_orders DESC;

-- ================================================================================
-- ТЕСТ 6: ПОДЗАПРОСЫ И CTE
-- ================================================================================
-- Демонстрация: Влияние на оптимизацию подзапросов
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 6A: CTE с агрегацией по distribution key (HASH)'
\echo 'ОЖИДАНИЕ: Эффективная оптимизация'
\echo '==================================================================================='

EXPLAIN ANALYZE
WITH customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total_amount) as total_spent,
        MAX(order_date) as last_order_date
    FROM orders_hash
    WHERE order_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY customer_id
),
high_value_customers AS (
    SELECT 
        customer_id,
        order_count,
        total_spent
    FROM customer_stats
    WHERE total_spent > 10000
)
SELECT 
    c.customer_segment,
    COUNT(*) as customer_count,
    SUM(hvc.total_spent) as segment_revenue,
    AVG(hvc.order_count) as avg_orders_per_customer
FROM high_value_customers hvc
JOIN customers_dim c ON hvc.customer_id = c.customer_id
GROUP BY c.customer_segment
ORDER BY segment_revenue DESC;

\echo ''
\echo '==================================================================================='
\echo 'ТЕСТ 6B: CTE с агрегацией (RANDOM)'
\echo 'ОЖИДАНИЕ: Менее эффективная оптимизация из-за Redistribute'
\echo '==================================================================================='

EXPLAIN ANALYZE
WITH customer_stats AS (
    SELECT 
        customer_id,
        COUNT(*) as order_count,
        SUM(total_amount) as total_spent,
        MAX(order_date) as last_order_date
    FROM orders_random
    WHERE order_date >= CURRENT_DATE - INTERVAL '365 days'
    GROUP BY customer_id
),
high_value_customers AS (
    SELECT 
        customer_id,
        order_count,
        total_spent
    FROM customer_stats
    WHERE total_spent > 10000
)
SELECT 
    c.customer_segment,
    COUNT(*) as customer_count,
    SUM(hvc.total_spent) as segment_revenue,
    AVG(hvc.order_count) as avg_orders_per_customer
FROM high_value_customers hvc
JOIN customers_dim c ON hvc.customer_id = c.customer_id
GROUP BY c.customer_segment
ORDER BY segment_revenue DESC;

-- ================================================================================
-- АНАЛИЗ РАСПРЕДЕЛЕНИЯ ДАННЫХ
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo 'АНАЛИЗ: Распределение данных по сегментам'
\echo '==================================================================================='

-- Проверка равномерности для HASH таблиц
SELECT 
    'orders_hash' as table_name,
    gp_segment_id,
    count(*) as row_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage,
    CASE 
        WHEN count(*) BETWEEN 
            (avg(count(*)) OVER () * 0.9) AND 
            (avg(count(*)) OVER () * 1.1) 
        THEN '✓ Balanced'
        ELSE '⚠ Skewed'
    END as distribution_status
FROM gp_dist_random('orders_hash')
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- Проверка для RANDOM таблиц
SELECT 
    'orders_random' as table_name,
    gp_segment_id,
    count(*) as row_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 2) as percentage,
    CASE 
        WHEN count(*) BETWEEN 
            (avg(count(*)) OVER () * 0.9) AND 
            (avg(count(*)) OVER () * 1.1) 
        THEN '✓ Balanced'
        ELSE '⚠ Skewed'
    END as distribution_status
FROM gp_dist_random('orders_random')
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- ================================================================================
-- СВОДНАЯ ТАБЛИЦА РЕЗУЛЬТАТОВ
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo 'СОЗДАНИЕ ТАБЛИЦЫ ДЛЯ СРАВНЕНИЯ РЕЗУЛЬТАТОВ'
\echo '==================================================================================='

DROP TABLE IF EXISTS performance_comparison;
CREATE TABLE performance_comparison (
    test_id SERIAL PRIMARY KEY,
    test_name VARCHAR(200),
    hash_execution_time_ms DECIMAL(10, 2),
    random_execution_time_ms DECIMAL(10, 2),
    performance_difference_pct DECIMAL(10, 2),
    hash_has_redistribute BOOLEAN,
    random_has_redistribute BOOLEAN,
    winner VARCHAR(20),
    notes TEXT
);

\echo ''
\echo '==================================================================================='
\echo 'ИНСТРУКЦИЯ ПО ЗАПОЛНЕНИЮ ТАБЛИЦЫ РЕЗУЛЬТАТОВ'
\echo '==================================================================================='
\echo 'Скопируйте Execution time из каждого EXPLAIN ANALYZE и вставьте в таблицу:'
\echo ''
\echo 'INSERT INTO performance_comparison (test_name, hash_execution_time_ms, random_execution_time_ms, notes)'
\echo 'VALUES '
\echo '  (''CO-LOCATED JOIN'', <время_1A>, <время_1B>, ''JOIN по distribution key''),'
\echo '  (''JOIN с REPLICATED'', <время_2A>, <время_2B>, ''JOIN со справочником''),'
\echo '  (''GROUP BY distribution key'', <время_3A>, <время_3B>, ''Агрегация по ключу''),'
\echo '  (''GROUP BY другое поле'', <время_4A>, <время_4B>, ''Агрегация НЕ по ключу''),'
\echo '  (''Complex multi-JOIN'', <время_5A>, <время_5B>, ''Сложный запрос с 3 JOIN''),'
\echo '  (''CTE с агрегацией'', <время_6A>, <время_6B>, ''Common Table Expression'');'
\echo ''
\echo 'Затем обновите рассчитанные поля:'
\echo ''
\echo 'UPDATE performance_comparison'
\echo 'SET '
\echo '    performance_difference_pct = ROUND(100.0 * (random_execution_time_ms - hash_execution_time_ms) / hash_execution_time_ms, 2),'
\echo '    winner = CASE '
\echo '        WHEN hash_execution_time_ms < random_execution_time_ms THEN ''HASH'''
\echo '        WHEN random_execution_time_ms < hash_execution_time_ms THEN ''RANDOM'''
\echo '        ELSE ''TIE'''
\echo '    END;'
\echo ''
\echo 'Просмотр результатов:'
\echo ''
\echo 'SELECT * FROM performance_comparison ORDER BY performance_difference_pct DESC;'

-- ================================================================================
-- ВЫВОДЫ И РЕКОМЕНДАЦИИ
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo '📊 КЛЮЧЕВЫЕ ВЫВОДЫ'
\echo '==================================================================================='
\echo ''
\echo '1. CO-LOCATED JOIN (ТЕСТ 1):'
\echo '   ✓ HASH: Локальный JOIN на каждом сегменте, БЕЗ Redistribute Motion'
\echo '   ✗ RANDOM: Redistribute Motion на ОБЕИХ таблицах (миллионы строк)'
\echo '   → Разница может достигать 2-5x в пользу HASH'
\echo ''
\echo '2. JOIN С REPLICATED (ТЕСТ 2):'
\echo '   ≈ HASH и RANDOM показывают схожую производительность'
\echo '   → REPLICATED нивелирует разницу в стратегиях'
\echo ''
\echo '3. GROUP BY ПО DISTRIBUTION KEY (ТЕСТ 3):'
\echo '   ✓ HASH: Локальная агрегация, минимальный Redistribute'
\echo '   ✗ RANDOM: Полный Redistribute всех данных'
\echo '   → Разница может достигать 3-10x в пользу HASH'
\echo ''
\echo '4. GROUP BY ПО ДРУГОМУ ПОЛЮ (ТЕСТ 4):'
\echo '   ≈ HASH и RANDOM показывают схожую производительность'
\echo '   → Оба требуют Redistribute Motion'
\echo ''
\echo '5. COMPLEX QUERIES (ТЕСТ 5-6):'
\echo '   ✓ HASH: Меньше операций перемещения данных'
\echo '   ✗ RANDOM: Множественные Redistribute в сложных запросах'
\echo '   → Кумулятивная разница может быть существенной'
\echo ''
\echo '==================================================================================='
\echo '💡 РЕКОМЕНДАЦИИ ПО ВЫБОРУ СТРАТЕГИИ'
\echo '==================================================================================='
\echo ''
\echo 'Используйте HASH DISTRIBUTION когда:'
\echo '  • Таблица большая (> 100K строк)'
\echo '  • Частые JOIN по одному и тому же ключу'
\echo '  • GROUP BY по конкретному полю'
\echo '  • Нужна предсказуемая производительность'
\echo ''
\echo 'Используйте RANDOM DISTRIBUTION когда:'
\echo '  • Staging/временные таблицы'
\echo '  • Данные вставляются быстро и удаляются'
\echo '  • Нет очевидного ключа для распределения'
\echo '  • Таблица относительно небольшая'
\echo ''
\echo 'Используйте REPLICATED DISTRIBUTION когда:'
\echo '  • Справочники и dimension таблицы'
\echo '  • Таблица маленькая (< 10-50K строк)'
\echo '  • Частые JOIN с большими таблицами'
\echo '  • Данные редко изменяются'
\echo ''
\echo '==================================================================================='

-- ================================================================================
-- ОЧИСТКА РЕСУРСОВ (ОПЦИОНАЛЬНО)
-- ================================================================================

\echo ''
\echo '==================================================================================='
\echo 'Для удаления всех тестовых данных выполните:'
\echo '==================================================================================='
\echo 'DROP SCHEMA lab1_advanced CASCADE;'
\echo ''

-- ================================================================================
-- КОНЕЦ ФАЙЛА
-- ================================================================================
