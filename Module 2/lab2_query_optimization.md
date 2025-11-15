# Лабораторная работа 2: Написание эффективных запросов и оптимизация

## Цель работы

Научиться писать оптимизированные SQL-запросы для GreenPlum, освоить анализ планов выполнения с помощью EXPLAIN ANALYZE, применить техники оптимизации на практике.

## Продолжительность

90 минут

## Предварительные требования

- Выполненная Лабораторная работа 1
- Подключение к Yandex MPP Analytics for PostgreSQL
- Установленные таблицы с тестовыми данными

## Часть 1: Создание партиционированных таблиц (20 минут)

### Шаг 1.1: Создание схемы для работы

```sql
-- Создаем новую схему
CREATE SCHEMA lab2;
SET search_path TO lab2, public;
```

### Шаг 1.2: Создание партиционированной таблицы по диапазону

```sql
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
    PRIMARY KEY (order_id, order_date,customer_id
```

### Шаг 1.3: Создание непартиционированной таблицы для сравнения

```sql
 -- Такая же таблица без партиционирования
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
```

### Шаг 1.4: Загрузка тестовых данных

```sql
-- Генерация 2 миллионов заказов за 2024 год
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
```

**Примечание:** Загрузка может занять 5-10 минут.

### Шаг 1.5: Просмотр информации о партициях

```sql
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

-- Количество записей в каждой партиции
SELECT 
    tablerelid::regclass AS partition_name,
    reltuples::bigint AS estimated_rows
FROM pg_partition_rule pr
JOIN pg_class c ON pr.parchildrelid = c.oid
WHERE pr.paroid IN (
    SELECT oid FROM pg_partition 
    WHERE parrelid = 'lab2.orders_partitioned'::regclass
)
ORDER BY partition_name;
```

## Часть 2: Анализ планов выполнения запросов (25 минут)

### Шаг 2.1: Основы EXPLAIN ANALYZE

```sql
-- Простой запрос с фильтром по дате
EXPLAIN ANALYZE
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM orders_partitioned
WHERE order_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY order_date
ORDER BY order_date;
```

**Что искать в плане выполнения:**

1. **Partition Pruning** - исключение ненужных партиций
2. **Seq Scan vs Index Scan** - тип сканирования
3. **Motion операции** - перемещение данных между сегментами
4. **Execution Time** - время выполнения

### Шаг 2.2: Сравнение с непартиционированной таблицей

```sql
-- Тот же запрос на непартиционированной таблице
EXPLAIN ANALYZE
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM orders_no_partition
WHERE order_date BETWEEN '2024-06-01' AND '2024-06-30'
GROUP BY order_date
ORDER BY order_date;
```

**Задание:** Сравните:

- Количество сканируемых строк
- Время выполнения
- Наличие partition pruning

### Шаг 2.3: Анализ JOIN операций

Создадим справочные таблицы:

```sql
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

-- Загрузка данных
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
```

Теперь проанализируем JOIN:

```sql
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
```

**Обратите внимание:**

- Отсутствие Redistribute Motion (благодаря REPLICATED)
- Эффективность Broadcast Join
- Применение partition pruning

## Часть 3: Оптимизация запросов (30 минут)

### Шаг 3.1: Проблема - неоптимальный JOIN

```sql
-- Плохой пример: JOIN не по distribution key
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

-- Неэффективный запрос
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
```

### Шаг 3.2: Решение - правильное распределение

```sql
-- Правильный подход: согласованное распределение
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

-- Оптимизированный запрос
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
```

**Задание:**

- Сравните планы выполнения двух запросов
- Измерьте время выполнения
- Найдите Redistribute Motion в первом плане

### Шаг 3.3: Оптимизация с помощью индексов

```sql
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
```

### Шаг 3.4: Использование WITH (CTE) для оптимизации

```sql
-- Без CTE - множественные сканирования
EXPLAIN ANALYZE
SELECT 
    c.country,
    monthly_revenue.month,
    monthly_revenue.revenue,
    total_revenue.total
FROM (
    SELECT 
        o.customer_id,
        DATE_TRUNC('month', o.order_date) as month,
        SUM(o.total_amount) as revenue
    FROM orders_partitioned o
    WHERE o.order_date >= '2024-01-01'
    GROUP BY o.customer_id, DATE_TRUNC('month', o.order_date)
) monthly_revenue
JOIN (
    SELECT 
        customer_id,
        SUM(total_amount) as total
    FROM orders_partitioned
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
) total_revenue ON monthly_revenue.customer_id = total_revenue.customer_id
JOIN customers c ON monthly_revenue.customer_id = c.customer_id
LIMIT 100;

-- С CTE - одно сканирование
EXPLAIN ANALYZE
WITH customer_orders AS (
    SELECT 
        customer_id,
        order_date,
        total_amount
    FROM orders_partitioned
    WHERE order_date >= '2024-01-01'
)
SELECT 
    c.country,
    DATE_TRUNC('month', co.order_date) as month,
    SUM(co.total_amount) as monthly_revenue,
    SUM(SUM(co.total_amount)) OVER (PARTITION BY co.customer_id) as total_revenue
FROM customer_orders co
JOIN customers c ON co.customer_id = c.customer_id
GROUP BY c.country, DATE_TRUNC('month', co.order_date), co.customer_id
LIMIT 100;
```

### Шаг 3.5: Оптимизация с помощью материализованных представлений

```sql
-- Создание материализованного представления для часто используемых агрегатов
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

-- Создание индекса на материализованном представлении
CREATE INDEX idx_monthly_sales_month ON monthly_sales_summary(month);
CREATE INDEX idx_monthly_sales_country ON monthly_sales_summary(country);

-- Использование материализованного представления
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

-- Сравнение с прямым запросом
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
```

## Часть 4: Практические задания (15 минут)

### Задание 1: Оптимизация медленного запроса

Дан неэффективный запрос:

```sql
-- Медленный запрос
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
```

**Ваша задача:**

1. Проанализируйте план выполнения
2. Определите узкие места
3. Оптимизируйте запрос (подсказка: используйте window functions)
4. Сравните производительность

<details>
<summary>Решение (нажмите, чтобы раскрыть)</summary>

```sql
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
```

</details>

### Задание 2: Написание эффективного отчета

Требуется создать отчет по продажам с следующими метриками:

- Выручка по странам и категориям за последние 3 месяца
- Рост по сравнению с предыдущим периодом
- Top-10 клиентов по выручке

**Требования:**

- Используйте партиционирование для ускорения
- Минимизируйте Redistribute Motion
- Оптимизируйте GROUP BY

```sql
-- Ваш код здесь
```

<details>
<summary>Примерное решение</summary>

```sql
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
SELECT 
    tc.customer_id,
    c.customer_name,
    tc.country,
    tc.customer_revenue
FROM top_customers tc
JOIN customers c ON tc.customer_id = c.customer_id
WHERE tc.rank <= 10
ORDER BY tc.rank;
```

</details>

### Задание 3: Отладка проблемы с производительностью

Запрос выполняется очень медленно:

```sql
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
```

**Задачи:**

1. Используйте EXPLAIN ANALYZE
2. Определите причины медленной работы
3. Предложите и реализуйте оптимизации
4. Измерьте улучшение производительности

## Часть 5: Создание таблицы результатов и анализ (опционально)

```sql
-- Таблица для хранения результатов тестов
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
```

## Контрольные вопросы

1. **Как partition pruning влияет на производительность запросов?**
2. **Почему JOIN по distribution key быстрее?**
3. **В каких случаях CTE (WITH) эффективнее подзапросов?**
4. **Когда стоит использовать материализованные представления?**
5. **Как интерпретировать план EXPLAIN ANALYZE?**

## Лучшие практики (Best Practices)

### ✓ Делайте:

1. **Всегда используйте EXPLAIN ANALYZE** перед оптимизацией
2. **Фильтруйте данные как можно раньше** (WHERE до JOIN)
3. **Используйте партиционирование** для больших таблиц с временными данными
4. **JOIN по distribution key** когда это возможно
5. **Используйте REPLICATED** для малых справочников
6. **Выбирайте только нужные колонки** (избегайте SELECT *)
7. **Обновляйте статистику** после загрузки данных (ANALYZE)

### ✗ Избегайте:

1. **Подзапросов в SELECT** (используйте JOIN или CTE)
2. **DISTINCT без необходимости** (дорогая операция)
3. **Функций в WHERE** на индексированных колонках
4. **Больших IN списков** (используйте временные таблицы)
5. **ORDER BY без LIMIT** на больших результатах
6. **Нескольких сканирований** одной таблицы (используйте CTE)

## Очистка ресурсов

```sql
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
```

## Дополнительные материалы

- [Query Performance Tuning](https://gpdb.docs.pivotal.io/6-latest/admin_guide/query/topics/query-piv-opt-overview.html)
- [Best Practices](https://gpdb.docs.pivotal.io/6-latest/best_practices/summary.html)
- [EXPLAIN Documentation](https://www.postgresql.org/docs/current/sql-explain.html)

## Домашнее задание

1. Создайте сложный аналитический запрос для ваших данных
2. Проведите полный цикл оптимизации
3. Задокументируйте результаты в отчете
4. Сравните производительность до и после оптимизации
