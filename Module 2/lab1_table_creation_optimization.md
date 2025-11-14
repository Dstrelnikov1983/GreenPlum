# Лабораторная работа 1: Создание и оптимизация таблиц в Yandex MPP Analytics

## Цель работы

Познакомиться с созданием таблиц с различными стратегиями распределения данных, научиться выбирать оптимальные параметры для таблиц и измерять влияние на производительность.

## Продолжительность

90 минут

## Предварительные требования

- Доступ к Yandex Cloud
- Установленный клиент PostgreSQL (psql) или DBeaver
- Базовые знания SQL

## Часть 1: Подключение к Yandex MPP Analytics for PostgreSQL (15 минут)

### Шаг 1.1: Получение параметров подключения

1. Откройте консоль Yandex Cloud: https://console.cloud.yandex.ru
2. Перейдите в раздел **Managed Service for Greenplum**
3. Выберите ваш кластер из списка
4. На странице кластера найдите раздел **Подключение**
5. Скопируйте следующие параметры:
   - Хост (Master hostname)
   - Порт (обычно 6432)
   - Имя базы данных
   - Имя пользователя

### Шаг 1.2: Подключение через psql

```bash
# Синтаксис подключения
psql "host=<MASTER_HOST> \
      port=6432 \
      sslmode=verify-full \
      dbname=<DB_NAME> \
      user=<USERNAME> \
      target_session_attrs=read-write"

# Пример
psql "host=c-c9qm1234567890ab.rw.mdb.yandexcloud.net \
      port=6432 \
      sslmode=verify-full \
      dbname=postgres \
      user=admin \
      target_session_attrs=read-write"
```

**Примечание:** При первом подключении потребуется ввести пароль.

### Шаг 1.3: Подключение через DBeaver

1. Создайте новое подключение: **База данных → Новое подключение**
2. Выберите **PostgreSQL**
3. Заполните параметры:
   - **Host:** скопируйте из консоли Yandex Cloud
   - **Port:** 6432
   - **Database:** postgres (или ваша БД)
   - **Username:** admin (или ваш пользователь)
   - **Password:** ваш пароль
4. На вкладке **SSL**:
   - Отметьте **Use SSL**
   - SSL mode: **verify-full**
5. Нажмите **Test Connection**, затем **Finish**

### Шаг 1.4: Проверка подключения

Выполните тестовый запрос:

```sql
-- Проверка версии GreenPlum
SELECT version();

-- Просмотр конфигурации кластера
SELECT * FROM gp_segment_configuration;

-- Количество сегментов
SELECT count(*) as segment_count 
FROM gp_segment_configuration 
WHERE role = 'p';
```

**Ожидаемый результат:** Вы должны увидеть информацию о версии GreenPlum и конфигурации сегментов.

## Часть 2: Создание схемы и таблиц с разными стратегиями распределения (30 минут)

### Шаг 2.1: Создание рабочей схемы

```sql
-- Создаем схему для лабораторной работы
CREATE SCHEMA lab1;

-- Устанавливаем схему как текущую
SET search_path TO lab1, public;
```

### Шаг 2.2: Создание таблиц с HASH распределением

Создадим таблицу фактов продаж с распределением по customer_id:

```sql
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
```

**Вопросы для обсуждения:**
1. Почему мы выбрали customer_id в качестве ключа распределения?
2. Какие альтернативные варианты могли бы быть?

### Шаг 2.3: Создание реплицируемых таблиц-справочников

```sql
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
SELECT 
    gp_segment_id,
    count(*) as row_count
FROM products_replicated
GROUP BY gp_segment_id
ORDER BY gp_segment_id;
```

### Шаг 2.4: Сравнение с RANDOM распределением

Создадим аналогичную таблицу с RANDOM распределением для сравнения:

```sql
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
```

### Шаг 2.5: Просмотр информации о таблицах

```sql
-- Информация о распределении таблиц
SELECT 
    schemaname,
    tablename,
    CASE 
        WHEN distkey IS NULL THEN 'RANDOMLY'
        WHEN distkey = '' THEN 'REPLICATED'
        ELSE 'HASH(' || distkey || ')'
    END as distribution_strategy
FROM pg_tables pt
LEFT JOIN (
    SELECT 
        n.nspname as schemaname,
        c.relname as tablename,
        array_to_string(array_agg(a.attname), ', ') as distkey
    FROM pg_class c
    JOIN pg_namespace n ON c.relnamespace = n.oid
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE a.attnum = ANY(c.attdistkey)
    AND c.relkind = 'r'
    GROUP BY n.nspname, c.relname
) dist USING (schemaname, tablename)
WHERE schemaname = 'lab1'
ORDER BY tablename;
```

## Часть 3: Загрузка тестовых данных (20 минут)

### Шаг 3.1: Загрузка данных в справочники

```sql
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
```

### Шаг 3.2: Генерация больших объемов данных для таблиц продаж

```sql
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
```

**Примечание:** Загрузка 1 млн записей может занять 2-5 минут. Следите за прогрессом.

### Шаг 3.3: Проверка распределения данных

```sql
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
```

**Задание:** Сравните распределение данных между двумя таблицами. Какая из них более равномерно распределена?

## Часть 4: Сравнение производительности запросов (25 минут)

### Шаг 4.1: Запросы с JOIN по distribution key

```sql
-- Тест 1: JOIN sales_hash с customers_replicated
-- (JOIN невозможен по distribution key, т.к. customers реплицирована)
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
```

**Обратите внимание на:**
- Время выполнения
- Наличие/отсутствие Redistribute Motion в плане

### Шаг 4.2: Запросы со случайным распределением

```sql
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
```

### Шаг 4.3: Агрегация по distribution key

```sql
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
```

### Шаг 4.4: Сравнение с JOIN маленькой таблицы

Создадим версию customers с HASH распределением:

```sql
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
```

### Шаг 4.5: Заполнение таблицы результатов

Создайте таблицу для записи результатов экспериментов:

```sql
CREATE TABLE performance_results (
    test_id SERIAL PRIMARY KEY,
    test_name VARCHAR(200),
    table_config VARCHAR(100),
    execution_time_ms DECIMAL(10, 2),
    rows_processed INTEGER,
    redistribute_motion BOOLEAN,
    notes TEXT
);
```

Заполните таблицу результатами ваших тестов:

```sql
INSERT INTO performance_results (test_name, table_config, execution_time_ms, rows_processed, redistribute_motion, notes)
VALUES 
    ('JOIN с реплицированной таблицей', 'sales_hash + customers_replicated', 1234.56, 1000000, false, 'Ваши заметки'),
    ('JOIN с реплицированной таблицей', 'sales_random + customers_replicated', 2345.67, 1000000, false, 'Ваши заметки');
-- Добавьте остальные результаты
```

## Контрольные вопросы

1. **Какая стратегия распределения показала лучшую производительность для больших таблиц фактов?**

2. **Почему реплицированные таблицы эффективны для справочников?**

3. **В каких случаях RANDOM распределение может быть предпочтительнее?**

4. **Как распределение данных влияет на необходимость перемещения данных между сегментами?**

5. **Что вы заметили в планах выполнения (EXPLAIN ANALYZE)?**

## Дополнительные задания (опционально)

### Задание 1: Экспериментируйте с размером данных

Попробуйте:
- Увеличить объем данных до 5-10 млн записей
- Измерить, как изменяется производительность
- Проверить равномерность распределения

### Задание 2: Разные ключи распределения

Создайте копию sales_hash с распределением по product_id:

```sql
CREATE TABLE sales_by_product (
    LIKE sales_hash
) DISTRIBUTED BY (product_id);

INSERT INTO sales_by_product SELECT * FROM sales_hash;
```

Сравните производительность запросов, агрегирующих данные по продуктам.

### Задание 3: Анализ skew (неравномерности)

Проверьте, есть ли неравномерность в распределении:

```sql
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
```

## Очистка ресурсов

После завершения работы:

```sql
-- Удаление таблиц
DROP TABLE IF EXISTS performance_results;
DROP TABLE IF EXISTS customers_hash;
DROP TABLE IF EXISTS sales_random;
DROP TABLE IF EXISTS sales_hash;
DROP TABLE IF EXISTS customers_replicated;
DROP TABLE IF EXISTS products_replicated;

-- Удаление схемы
DROP SCHEMA IF EXISTS lab1 CASCADE;
```

## Выводы

Запишите основные выводы из лабораторной работы:

1. Какая стратегия распределения наиболее эффективна для различных типов таблиц?
2. Как выбор distribution key влияет на производительность JOIN?
3. В каких сценариях реплицированные таблицы дают максимальное преимущество?

## Материалы для самостоятельного изучения

- [Официальная документация Yandex MPP Analytics](https://cloud.yandex.ru/docs/managed-greenplum/)
- [GreenPlum Database Documentation](https://docs.vmware.com/en/VMware-Greenplum/index.html)
- [Best Practices for GreenPlum](https://gpdb.docs.pivotal.io/6-latest/best_practices/summary.html)
