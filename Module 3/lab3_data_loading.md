# Лабораторная работа №3: Загрузка и обработка данных в GreenPlum

## Цель работы

Освоить различные методы загрузки данных в GreenPlum (Yandex MPP Analytics for PostgreSQL), научиться работать с разными форматами данных (CSV, JSON, XML) и оптимизировать процессы загрузки.

## Продолжительность

**60 минут** (рекомендуется)

## Предварительные требования

- Кластер GreenPlum в Yandex Cloud (из Лабораторной №1)
- PostgreSQL клиент (psql)
- Доступ к Yandex Object Storage
- Базовые знания SQL и командной строки

## Подготовка данных

### Создание тестовых данных

Перед началом работы создадим тестовые файлы с данными. Все файлы будут размещены в Yandex Object Storage.

---

## Часть 1: Загрузка CSV данных (20 минут)

### Задание 1.1: Подготовка окружения

**Шаг 1:** Подключитесь к вашему кластеру GreenPlum

```bash
# Получите endpoint кластера
yc managed-greenplum cluster get <cluster-id> --format json | jq -r '.host_master_name'

# Подключение
psql -h <master-host> -U <username> -d postgres
```

**Шаг 2:** Создайте базу данных для лабораторной работы

```sql
-- Создание базы данных
CREATE DATABASE lab3_data_loading;

-- Подключение к новой базе
\c lab3_data_loading
```

**Шаг 3:** Создайте схему для таблиц

```sql
-- Создание схемы
CREATE SCHEMA staging;
CREATE SCHEMA production;

-- Установка search path
SET search_path = staging, production, public;
```

### Задание 1.2: Загрузка через COPY команду

**Шаг 1:** Создайте таблицу для данных о продажах

```sql
CREATE TABLE staging.sales_data (
    sale_id INTEGER,
    product_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    amount DECIMAL(10,2),
    sale_date DATE,
    region TEXT,
    payment_method TEXT
) DISTRIBUTED BY (customer_id);
```

**Шаг 2:** Создайте тестовый CSV файл на локальной машине

Создайте файл `sales_sample.csv`:

```csv
sale_id,product_id,customer_id,quantity,amount,sale_date,region,payment_method
1,101,1001,2,49.98,2024-11-01,North,Credit Card
2,102,1002,1,29.99,2024-11-01,South,PayPal
3,103,1003,3,89.97,2024-11-02,East,Credit Card
4,104,1004,1,19.99,2024-11-02,West,Cash
5,105,1005,2,59.98,2024-11-03,North,Debit Card
6,106,1001,1,39.99,2024-11-03,South,Credit Card
7,107,1002,4,119.96,2024-11-04,East,PayPal
8,108,1003,2,79.98,2024-11-04,West,Credit Card
9,109,1004,1,24.99,2024-11-05,North,Cash
10,110,1005,3,89.97,2024-11-05,South,Debit Card
```

**Шаг 3:** Загрузите данные используя COPY

```sql
-- Включение измерения времени
\timing on

-- Загрузка через COPY (из локального файла на клиенте)
\copy staging.sales_data FROM 'sales_sample.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',');

-- Проверка загруженных данных
SELECT count(*) FROM staging.sales_data;
SELECT * FROM staging.sales_data LIMIT 5;
```

**Ожидаемый результат:** 10 записей загружено успешно.

### Задание 1.3: Загрузка через External Table с Object Storage

**Шаг 1:** Создайте бакет в Yandex Object Storage (если еще не создан)

```bash
# Через Yandex Cloud CLI
yc storage bucket create --name mpp-lab3-data --default-storage-class standard

# Получите статические ключи доступа
yc iam access-key create --service-account-name <service-account-name>
```

**Шаг 2:** Загрузите тестовый файл в Object Storage

Создайте файл `sales_large.csv` с большим количеством данных (для демонстрации можно использовать генератор).

```bash
# Загрузка файла в S3
aws --endpoint-url=https://storage.yandexcloud.net \
    s3 cp sales_large.csv s3://mpp-lab3-data/sales/sales_large.csv
```

**Шаг 3:** Создайте External Table

```sql
-- Создание external table для чтения из S3
CREATE EXTERNAL TABLE staging.ext_sales_from_s3 (
    sale_id INTEGER,
    product_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    amount DECIMAL(10,2),
    sale_date DATE,
    region TEXT,
    payment_method TEXT
)
LOCATION (
    's3://mpp-lab3-data/sales/sales_large.csv 
    config=/path/to/s3.conf'
)
FORMAT 'CSV' (
    HEADER 
    DELIMITER ',' 
    NULL 'NULL'
    QUOTE '"'
);

-- Примечание: для Yandex MPP Analytics используйте специфичный синтаксис
-- Смотрите документацию: https://cloud.yandex.ru/docs/managed-greenplum/operations/external-tables
```

**Конфигурационный файл s3.conf** (разместите на мастер-ноде):

```ini
[default]
accessid = YOUR_ACCESS_KEY_ID
secret = YOUR_SECRET_ACCESS_KEY
threadnum = 4
chunksize = 67108864
protocol = https
version = 2
gpcheckcloud_newline = LF
```

**Шаг 4:** Загрузите данные из External Table в постоянную таблицу

```sql
-- Создание production таблицы
CREATE TABLE production.sales (
    sale_id INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    quantity INTEGER CHECK (quantity > 0),
    amount DECIMAL(10,2) CHECK (amount >= 0),
    sale_date DATE NOT NULL,
    region TEXT,
    payment_method TEXT
) DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (sale_date)
(
    START (DATE '2024-01-01') INCLUSIVE
    END (DATE '2025-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);

-- Загрузка с измерением времени
\timing on

INSERT INTO production.sales
SELECT * FROM staging.ext_sales_from_s3;

-- Проверка
SELECT count(*) FROM production.sales;
```

### Задание 1.4: Обработка ошибок при загрузке

**Шаг 1:** Создайте файл с ошибками `sales_with_errors.csv`:

```csv
sale_id,product_id,customer_id,quantity,amount,sale_date,region,payment_method
11,111,1006,2,49.98,2024-11-06,North,Credit Card
12,INVALID,1007,1,29.99,2024-11-06,South,PayPal
13,113,1008,3,89.97,INVALID_DATE,East,Credit Card
14,114,NULL,1,19.99,2024-11-07,West,Cash
15,115,1010,-5,59.98,2024-11-07,North,Debit Card
```

**Шаг 2:** Создайте таблицу для загрузки с обработкой ошибок

```sql
CREATE TABLE staging.sales_with_validation (
    sale_id INTEGER,
    product_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    amount DECIMAL(10,2),
    sale_date DATE,
    region TEXT,
    payment_method TEXT
) DISTRIBUTED BY (customer_id);
```

**Шаг 3:** Загрузите данные с логированием ошибок

```sql
-- Очистка старых логов ошибок
SELECT gp_truncate_error_log('staging.sales_with_validation');

-- Загрузка с обработкой ошибок
\copy staging.sales_with_validation FROM 'sales_with_errors.csv' WITH (FORMAT CSV, HEADER true, DELIMITER ',', NULL 'NULL')

-- В случае ошибок, используйте параметры:
-- Для таблиц (не работает с \copy):
COPY staging.sales_with_validation
FROM '/path/to/sales_with_errors.csv'
WITH (
    FORMAT CSV,
    HEADER true,
    DELIMITER ',',
    NULL 'NULL',
    LOG_ERRORS true,  -- Логирование ошибок
    SEGMENT REJECT LIMIT 10 ROWS  -- Пропуск до 10 строк с ошибками
);
```

**Шаг 4:** Просмотр ошибок загрузки

```sql
-- Просмотр логов ошибок
SELECT 
    linenum,
    errmsg,
    rawdata
FROM gp_read_error_log('staging.sales_with_validation')
ORDER BY linenum;

-- Статистика ошибок
SELECT 
    count(*) as error_count,
    errmsg
FROM gp_read_error_log('staging.sales_with_validation')
GROUP BY errmsg;
```

### Задание 1.5: Сравнение производительности методов загрузки

**Задача:** Сравните время загрузки одного и того же файла разными методами.

```sql
-- Метод 1: INSERT ... VALUES (построчно) - МЕДЛЕННО
CREATE TABLE staging.test_insert (LIKE staging.sales_data);
\timing on
INSERT INTO staging.test_insert VALUES 
    (1, 101, 1001, 2, 49.98, '2024-11-01', 'North', 'Credit Card'),
    (2, 102, 1002, 1, 29.99, '2024-11-01', 'South', 'PayPal');
-- ... для 100 строк
DROP TABLE staging.test_insert;

-- Метод 2: COPY - БЫСТРО
CREATE TABLE staging.test_copy (LIKE staging.sales_data);
\timing on
\copy staging.test_copy FROM 'sales_sample.csv' WITH CSV HEADER
DROP TABLE staging.test_copy;

-- Метод 3: External Table + INSERT SELECT - ОЧЕНЬ БЫСТРО для больших файлов
CREATE TABLE staging.test_external (LIKE staging.sales_data);
\timing on
INSERT INTO staging.test_external SELECT * FROM staging.ext_sales_from_s3;
DROP TABLE staging.test_external;
```

**Результаты:** Запишите время выполнения для каждого метода.

| Метод | Время загрузки | Преимущества | Недостатки |
|-------|----------------|--------------|------------|
| INSERT VALUES | ? мс | Простота | Очень медленно |
| COPY | ? мс | Средняя скорость | Файл на клиенте |
| External Table | ? мс | Параллельная загрузка | Настройка S3 |

---

## Часть 2: Работа с JSON данными (15 минут)

### Задание 2.1: Создание таблиц для JSON данных

```sql
-- Таблица для хранения событий в JSON формате
CREATE TABLE staging.user_events (
    event_id SERIAL PRIMARY KEY,
    event_data JSONB,  -- JSONB для быстрых запросов
    created_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (event_id);

-- Таблица для нормализованных данных
CREATE TABLE production.events (
    event_id SERIAL PRIMARY KEY,
    user_id INTEGER,
    event_type TEXT,
    event_timestamp TIMESTAMP,
    properties JSONB
) DISTRIBUTED BY (user_id);
```

### Задание 2.2: Загрузка JSON данных

**Шаг 1:** Создайте файл `events.json` (NDJSON - каждая строка = JSON объект):

```json
{"user_id": 1001, "event_type": "page_view", "timestamp": "2024-11-14T10:00:00", "page": "/home", "duration": 45}
{"user_id": 1002, "event_type": "click", "timestamp": "2024-11-14T10:01:00", "element": "button", "page": "/products"}
{"user_id": 1003, "event_type": "purchase", "timestamp": "2024-11-14T10:05:00", "amount": 99.99, "items": ["A", "B", "C"]}
{"user_id": 1001, "event_type": "logout", "timestamp": "2024-11-14T10:10:00", "session_duration": 600}
{"user_id": 1004, "event_type": "search", "timestamp": "2024-11-14T10:15:00", "query": "laptop", "results": 42}
```

**Шаг 2:** Загрузите JSON данные

```sql
-- Вариант 1: Загрузка через COPY (для NDJSON)
-- Временная таблица для raw JSON
CREATE TEMP TABLE temp_json_import (json_line TEXT);

\copy temp_json_import FROM 'events.json'

-- Парсинг и вставка
INSERT INTO staging.user_events (event_data)
SELECT json_line::JSONB
FROM temp_json_import
WHERE json_line IS NOT NULL AND json_line != '';

-- Проверка
SELECT count(*) FROM staging.user_events;
SELECT * FROM staging.user_events LIMIT 2;
```

### Задание 2.3: Извлечение данных из JSON

```sql
-- Извлечение полей из JSON
SELECT 
    event_id,
    event_data->>'user_id' as user_id_text,
    (event_data->>'user_id')::INTEGER as user_id,
    event_data->>'event_type' as event_type,
    (event_data->>'timestamp')::TIMESTAMP as event_timestamp,
    event_data->'amount' as amount_json,
    (event_data->>'amount')::DECIMAL as amount
FROM staging.user_events;

-- Фильтрация по JSON полям
SELECT 
    event_id,
    event_data->>'user_id' as user_id,
    event_data->>'event_type' as event_type
FROM staging.user_events
WHERE event_data->>'event_type' = 'purchase';

-- Проверка существования ключа
SELECT 
    event_id,
    event_data->>'event_type' as event_type,
    CASE 
        WHEN event_data ? 'amount' THEN 'Has amount'
        ELSE 'No amount'
    END as has_amount
FROM staging.user_events;

-- Работа с JSON массивами
SELECT 
    event_id,
    jsonb_array_elements_text(event_data->'items') as item
FROM staging.user_events
WHERE event_data ? 'items';
```

### Задание 2.4: Трансформация JSON в реляционные таблицы

```sql
-- Нормализация: извлечение данных из JSON в структурированную таблицу
INSERT INTO production.events (user_id, event_type, event_timestamp, properties)
SELECT 
    (event_data->>'user_id')::INTEGER as user_id,
    event_data->>'event_type' as event_type,
    (event_data->>'timestamp')::TIMESTAMP as event_timestamp,
    event_data - 'user_id' - 'event_type' - 'timestamp' as properties  -- Остальные поля
FROM staging.user_events;

-- Проверка результата
SELECT * FROM production.events;

-- Агрегация по данным из JSON
SELECT 
    event_type,
    count(*) as event_count,
    count(DISTINCT user_id) as unique_users
FROM production.events
GROUP BY event_type
ORDER BY event_count DESC;
```

### Задание 2.5: Создание индекса на JSON данные

```sql
-- GIN индекс для быстрого поиска по JSONB
CREATE INDEX idx_events_data_gin ON staging.user_events USING GIN (event_data);

-- Поиск с использованием индекса
EXPLAIN ANALYZE
SELECT * FROM staging.user_events
WHERE event_data @> '{"event_type": "purchase"}';

-- Индекс на конкретное поле в JSON
CREATE INDEX idx_events_user_id ON staging.user_events ((event_data->>'user_id'));

EXPLAIN ANALYZE
SELECT * FROM staging.user_events
WHERE event_data->>'user_id' = '1001';
```

---

## Часть 3: Работа с XML данными (15 минут)

### Задание 3.1: Создание таблиц для XML данных

```sql
-- Таблица для хранения XML документов
CREATE TABLE staging.orders_xml (
    doc_id SERIAL PRIMARY KEY,
    xml_content XML,
    loaded_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (doc_id);

-- Таблицы для нормализованных данных
CREATE TABLE production.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    order_date DATE,
    total_amount DECIMAL(10,2),
    status TEXT
) DISTRIBUTED BY (customer_id);

CREATE TABLE production.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INTEGER,
    product_name TEXT,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES production.orders(order_id)
) DISTRIBUTED BY (order_id);
```

### Задание 3.2: Загрузка XML данных

**Шаг 1:** Создайте файл `orders.xml`:

```xml
<order>
    <order_id>1001</order_id>
    <customer_id>5001</customer_id>
    <order_date>2024-11-10</order_date>
    <total>250.00</total>
    <status>completed</status>
    <items>
        <item>
            <product_name>Laptop</product_name>
            <quantity>1</quantity>
            <unit_price>200.00</unit_price>
        </item>
        <item>
            <product_name>Mouse</product_name>
            <quantity>2</quantity>
            <unit_price>25.00</unit_price>
        </item>
    </items>
</order>
```

**Шаг 2:** Вставьте XML данные

```sql
-- Вставка XML вручную для демонстрации
INSERT INTO staging.orders_xml (xml_content) VALUES 
('<order>
    <order_id>1001</order_id>
    <customer_id>5001</customer_id>
    <order_date>2024-11-10</order_date>
    <total>250.00</total>
    <status>completed</status>
    <items>
        <item>
            <product_name>Laptop</product_name>
            <quantity>1</quantity>
            <unit_price>200.00</unit_price>
        </item>
        <item>
            <product_name>Mouse</product_name>
            <quantity>2</quantity>
            <unit_price>25.00</unit_price>
        </item>
    </items>
</order>');

INSERT INTO staging.orders_xml (xml_content) VALUES 
('<order>
    <order_id>1002</order_id>
    <customer_id>5002</customer_id>
    <order_date>2024-11-11</order_date>
    <total>150.00</total>
    <status>pending</status>
    <items>
        <item>
            <product_name>Keyboard</product_name>
            <quantity>1</quantity>
            <unit_price>150.00</unit_price>
        </item>
    </items>
</order>');

-- Проверка
SELECT * FROM staging.orders_xml;
```

### Задание 3.3: Извлечение данных из XML с помощью XPath

```sql
-- Базовые XPath запросы
SELECT 
    doc_id,
    xpath('/order/order_id/text()', xml_content) as order_id_array,
    xpath('/order/customer_id/text()', xml_content) as customer_id_array,
    xpath('/order/total/text()', xml_content) as total_array
FROM staging.orders_xml;

-- Извлечение и приведение типов (xpath возвращает массив)
SELECT 
    doc_id,
    (xpath('/order/order_id/text()', xml_content))[1]::TEXT::INTEGER as order_id,
    (xpath('/order/customer_id/text()', xml_content))[1]::TEXT::INTEGER as customer_id,
    (xpath('/order/order_date/text()', xml_content))[1]::TEXT::DATE as order_date,
    (xpath('/order/total/text()', xml_content))[1]::TEXT::DECIMAL as total,
    (xpath('/order/status/text()', xml_content))[1]::TEXT as status
FROM staging.orders_xml;
```

### Задание 3.4: Парсинг вложенных XML элементов

```sql
-- Извлечение элементов items (массив)
SELECT 
    doc_id,
    unnest(xpath('/order/items/item', xml_content)) as item_xml
FROM staging.orders_xml;

-- Извлечение данных из каждого item
WITH items_expanded AS (
    SELECT 
        doc_id,
        (xpath('/order/order_id/text()', xml_content))[1]::TEXT::INTEGER as order_id,
        unnest(xpath('/order/items/item', xml_content)) as item_xml
    FROM staging.orders_xml
)
SELECT 
    doc_id,
    order_id,
    (xpath('/item/product_name/text()', item_xml))[1]::TEXT as product_name,
    (xpath('/item/quantity/text()', item_xml))[1]::TEXT::INTEGER as quantity,
    (xpath('/item/unit_price/text()', item_xml))[1]::TEXT::DECIMAL as unit_price
FROM items_expanded;
```

### Задание 3.5: Нормализация XML данных в таблицы

```sql
-- Загрузка заказов
INSERT INTO production.orders (order_id, customer_id, order_date, total_amount, status)
SELECT 
    (xpath('/order/order_id/text()', xml_content))[1]::TEXT::INTEGER,
    (xpath('/order/customer_id/text()', xml_content))[1]::TEXT::INTEGER,
    (xpath('/order/order_date/text()', xml_content))[1]::TEXT::DATE,
    (xpath('/order/total/text()', xml_content))[1]::TEXT::DECIMAL,
    (xpath('/order/status/text()', xml_content))[1]::TEXT
FROM staging.orders_xml;

-- Загрузка позиций заказов
WITH items_data AS (
    SELECT 
        (xpath('/order/order_id/text()', xml_content))[1]::TEXT::INTEGER as order_id,
        unnest(xpath('/order/items/item', xml_content)) as item_xml
    FROM staging.orders_xml
)
INSERT INTO production.order_items (order_id, product_name, quantity, unit_price)
SELECT 
    order_id,
    (xpath('/item/product_name/text()', item_xml))[1]::TEXT,
    (xpath('/item/quantity/text()', item_xml))[1]::TEXT::INTEGER,
    (xpath('/item/unit_price/text()', item_xml))[1]::TEXT::DECIMAL
FROM items_data;

-- Проверка результатов
SELECT * FROM production.orders;
SELECT * FROM production.order_items;

-- Объединенный запрос
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    oi.product_name,
    oi.quantity,
    oi.unit_price
FROM production.orders o
JOIN production.order_items oi ON o.order_id = oi.order_id
ORDER BY o.order_id, oi.item_id;
```

---

## Часть 4: Оптимизация и мониторинг загрузки (10 минут)

### Задание 4.1: Анализ распределения данных

```sql
-- Проверка data skew в таблице sales
SELECT 
    gp_segment_id,
    count(*) as rows_per_segment,
    pg_size_pretty(pg_relation_size('production.sales')) as size_per_segment
FROM production.sales
GROUP BY gp_segment_id
ORDER BY rows_per_segment DESC;

-- Вычисление коэффициента перекоса
WITH segment_stats AS (
    SELECT 
        gp_segment_id,
        count(*) as row_count
    FROM production.sales
    GROUP BY gp_segment_id
)
SELECT 
    max(row_count) as max_rows,
    min(row_count) as min_rows,
    avg(row_count) as avg_rows,
    round(100.0 * (max(row_count) - min(row_count)) / avg(row_count), 2) as skew_percentage
FROM segment_stats;

-- Идеальное значение: skew_percentage < 10%
```

### Задание 4.2: Оптимизация производительности загрузки

```sql
-- Сравнение различных методов вставки

-- 1. Создание тестовой таблицы без индексов
CREATE TABLE staging.test_load_no_index (
    id INTEGER,
    data TEXT,
    value DECIMAL
) DISTRIBUTED BY (id);

\timing on
-- Загрузка данных
INSERT INTO staging.test_load_no_index 
SELECT 
    generate_series(1, 100000) as id,
    md5(random()::TEXT) as data,
    random() * 1000 as value;
-- Запишите время: _____ мс

-- 2. Создание таблицы с индексом перед загрузкой
CREATE TABLE staging.test_load_with_index (
    id INTEGER,
    data TEXT,
    value DECIMAL
) DISTRIBUTED BY (id);

CREATE INDEX idx_test_value ON staging.test_load_with_index(value);

\timing on
INSERT INTO staging.test_load_with_index 
SELECT 
    generate_series(1, 100000) as id,
    md5(random()::TEXT) as data,
    random() * 1000 as value;
-- Запишите время: _____ мс

-- 3. Создание индекса после загрузки
CREATE TABLE staging.test_load_index_after (
    id INTEGER,
    data TEXT,
    value DECIMAL
) DISTRIBUTED BY (id);

\timing on
INSERT INTO staging.test_load_index_after 
SELECT 
    generate_series(1, 100000) as id,
    md5(random()::TEXT) as data,
    random() * 1000 as value;

CREATE INDEX idx_test_value_after ON staging.test_load_index_after(value);
-- Запишите общее время: _____ мс

-- Вывод: Создание индексов ПОСЛЕ загрузки быстрее!
```

### Задание 4.3: Использование VACUUM и ANALYZE

```sql
-- После массовой загрузки данных необходимо обновить статистику

-- ANALYZE обновляет статистику для оптимизатора запросов
ANALYZE production.sales;

-- Проверка статистики
SELECT 
    schemaname,
    tablename,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows
FROM pg_stat_user_tables
WHERE tablename IN ('sales', 'orders', 'order_items');

-- VACUUM освобождает пространство от удаленных строк
VACUUM production.sales;

-- VACUUM ANALYZE - комбинация обоих операций
VACUUM ANALYZE production.sales;
```

### Задание 4.4: Мониторинг активных загрузок

```sql
-- Просмотр текущих активных запросов
SELECT 
    pid,
    usename,
    application_name,
    client_addr,
    state,
    query_start,
    now() - query_start as duration,
    substring(query, 1, 100) as query_preview
FROM pg_stat_activity
WHERE state = 'active'
    AND query NOT ILIKE '%pg_stat_activity%'
ORDER BY query_start;

-- Мониторинг долгих запросов (> 5 минут)
SELECT 
    pid,
    usename,
    state,
    now() - query_start as duration,
    query
FROM pg_stat_activity
WHERE state = 'active'
    AND now() - query_start > interval '5 minutes'
ORDER BY duration DESC;

-- При необходимости остановить долгий запрос:
-- SELECT pg_cancel_backend(<pid>);  -- Мягкая остановка
-- SELECT pg_terminate_backend(<pid>);  -- Принудительная остановка
```

### Задание 4.5: Проверка качества данных

```sql
-- 1. Проверка NULL значений в критических полях
SELECT 
    count(*) as total_rows,
    count(*) FILTER (WHERE customer_id IS NULL) as null_customer_id,
    count(*) FILTER (WHERE amount IS NULL) as null_amount,
    count(*) FILTER (WHERE sale_date IS NULL) as null_sale_date
FROM production.sales;

-- 2. Проверка дубликатов
SELECT 
    sale_id,
    count(*) as duplicate_count
FROM production.sales
GROUP BY sale_id
HAVING count(*) > 1;

-- 3. Проверка диапазонов значений
SELECT 
    count(*) FILTER (WHERE quantity < 0) as negative_quantity,
    count(*) FILTER (WHERE amount < 0) as negative_amount,
    count(*) FILTER (WHERE amount > 10000) as suspiciously_high_amount
FROM production.sales;

-- 4. Проверка аномалий по датам
SELECT 
    count(*) FILTER (WHERE sale_date < '2020-01-01') as old_dates,
    count(*) FILTER (WHERE sale_date > current_date) as future_dates
FROM production.sales;

-- 5. Комплексный отчет о качестве данных
WITH quality_checks AS (
    SELECT 
        'Total Rows' as check_name,
        count(*)::TEXT as result
    FROM production.sales
    
    UNION ALL
    
    SELECT 
        'NULL customer_id',
        count(*)::TEXT
    FROM production.sales
    WHERE customer_id IS NULL
    
    UNION ALL
    
    SELECT 
        'NULL amount',
        count(*)::TEXT
    FROM production.sales
    WHERE amount IS NULL
    
    UNION ALL
    
    SELECT 
        'Negative amounts',
        count(*)::TEXT
    FROM production.sales
    WHERE amount < 0
    
    UNION ALL
    
    SELECT 
        'Duplicates',
        count(*)::TEXT
    FROM (
        SELECT sale_id
        FROM production.sales
        GROUP BY sale_id
        HAVING count(*) > 1
    ) dups
)
SELECT * FROM quality_checks;
```

---

## Контрольные вопросы

После выполнения лабораторной работы ответьте на следующие вопросы:

### Теоретические вопросы:

1. **В чем разница между COPY и External Tables?** Когда использовать каждый из них?

2. **Почему JSONB предпочтительнее JSON?** Какие преимущества он дает?

3. **Что такое data skew и как его избежать?**

4. **Зачем нужен ANALYZE после массовой загрузки данных?**

5. **Какие стратегии обработки ошибок при загрузке данных вы знаете?**

### Практические вопросы:

6. **Сколько времени заняла загрузка данных через COPY vs External Table?** Почему есть разница?

7. **Какой процент data skew у вашей таблицы sales?** Это приемлемо?

8. **Сколько ошибок было в файле sales_with_errors.csv?** Какие типы ошибок?

9. **Что сложнее парсить: JSON или XML?** Почему?

10. **Какой метод загрузки вы бы выбрали для 500 ГБ CSV файлов?** Обоснуйте.

---

## Дополнительные задания (опционально)

### Задание A: Автоматизация загрузки

Напишите SQL скрипт, который:
1. Создает staging таблицу
2. Загружает данные из external table
3. Выполняет data quality checks
4. При успехе — переносит в production
5. Логирует результаты в таблицу audit_log

### Задание B: Оптимизация большой загрузки

Загрузите файл размером 1 ГБ и оптимизируйте процесс:
- Измерьте baseline производительность
- Примените compression
- Попробуйте разные distribution keys
- Отключите индексы перед загрузкой
- Сравните результаты

### Задание C: Работа со сложным JSON

Загрузите JSON с глубоко вложенной структурой (3+ уровня):
- Распарсьте все уровни
- Создайте нормализованные таблицы
- Постройте запросы для аналитики

---

## Очистка ресурсов

После завершения лабораторной работы:

```sql
-- Удаление тестовых данных (опционально)
DROP SCHEMA staging CASCADE;
-- DROP SCHEMA production CASCADE;  -- Оставьте для следующих модулей

-- Удаление базы данных (опционально)
-- \c postgres
-- DROP DATABASE lab3_data_loading;
```

**Важно:** Если вы не планируете использовать кластер до следующего занятия, остановите его:

```bash
yc managed-greenplum cluster stop <cluster-id>
```

Или удалите полностью:

```bash
yc managed-greenplum cluster delete <cluster-id>
```

---

## Полезные ссылки

- [GreenPlum External Tables](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/admin_guide-external-g-working-with-file-based-ext-tables.html)
- [PostgreSQL COPY](https://www.postgresql.org/docs/9.4/sql-copy.html)
- [PostgreSQL JSON Functions](https://www.postgresql.org/docs/9.4/functions-json.html)
- [PostgreSQL XML Functions](https://www.postgresql.org/docs/9.4/functions-xml.html)
- [Yandex Object Storage](https://cloud.yandex.ru/docs/storage/)
- [Yandex MPP Analytics](https://cloud.yandex.ru/docs/managed-greenplum/)

---

## Заключение

В этой лабораторной работе вы освоили:
- ✅ Различные методы загрузки данных в GreenPlum
- ✅ Работу с CSV, JSON и XML форматами
- ✅ Обработку ошибок при загрузке
- ✅ Оптимизацию производительности загрузки
- ✅ Мониторинг и проверку качества данных

**Следующий шаг:** Модуль 4 — ETL/ELT процессы и продвинутое использование GreenPlum.

---

**Время выполнения:** Фактическое — _____ минут  
**Статус:** ☐ Выполнено ☐ Требует доработки  
**Комментарии:**