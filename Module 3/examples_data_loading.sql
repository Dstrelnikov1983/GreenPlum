-- ============================================
-- Модуль 3: Примеры загрузки данных
-- GreenPlum Data Loading Examples
-- ============================================

-- ============================================
-- 1. ПОДГОТОВКА ОКРУЖЕНИЯ
-- ============================================

-- Создание базы данных для примеров
CREATE DATABASE data_loading_examples;
\c data_loading_examples

-- Создание схем
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS examples;

-- Установка search path
SET search_path = staging, production, examples, public;

-- ============================================
-- 2. ПРИМЕРЫ ЗАГРУЗКИ CSV ДАННЫХ
-- ============================================

-- Пример 2.1: Базовая загрузка через COPY
-- ----------------------------------------

CREATE TABLE examples.customers (
    customer_id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE,
    phone TEXT,
    registration_date DATE,
    country TEXT
) DISTRIBUTED BY (customer_id);

-- COPY из локального файла (на стороне клиента)
-- \copy examples.customers FROM '/path/to/customers.csv' WITH (FORMAT CSV, HEADER true);

-- COPY из файла на сервере
-- COPY examples.customers FROM '/data/customers.csv' WITH (FORMAT CSV, HEADER true);

-- Пример 2.2: COPY с различными опциями
-- ----------------------------------------

CREATE TABLE examples.products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price DECIMAL(10,2),
    stock_quantity INTEGER
) DISTRIBUTED BY (product_id);

-- COPY с кастомным разделителем и NULL обработкой
/*
COPY examples.products 
FROM '/data/products.txt'
WITH (
    FORMAT TEXT,
    DELIMITER '|',
    NULL 'N/A',
    QUOTE '"',
    ESCAPE '\\'
);
*/

-- Пример 2.3: Обработка ошибок при загрузке
-- ------------------------------------------

CREATE TABLE staging.sales_import (
    sale_id INTEGER,
    product_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    amount DECIMAL(10,2),
    sale_date DATE
) DISTRIBUTED BY (customer_id);

-- Загрузка с логированием ошибок (только для COPY с сервера)
/*
COPY staging.sales_import
FROM '/data/sales.csv'
WITH (
    FORMAT CSV,
    HEADER true,
    DELIMITER ',',
    NULL '',
    LOG_ERRORS true,             -- Включить логирование ошибок
    SEGMENT REJECT LIMIT 100 ROWS -- Допустить до 100 ошибочных строк
);
*/

-- Просмотр ошибок загрузки
SELECT 
    cmdtime as error_time,
    relname as table_name,
    filename,
    linenum as line_number,
    errmsg as error_message,
    rawdata as raw_data
FROM gp_read_error_log('staging.sales_import')
ORDER BY cmdtime DESC
LIMIT 20;

-- Очистка логов ошибок
SELECT gp_truncate_error_log('staging.sales_import');

-- ============================================
-- 3. EXTERNAL TABLES ДЛЯ ФАЙЛОВ
-- ============================================

-- Пример 3.1: Readable External Table (FILE protocol)
-- ----------------------------------------------------

-- External table для чтения CSV файлов
CREATE EXTERNAL TABLE staging.ext_transactions_file (
    transaction_id INTEGER,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    transaction_date DATE,
    status TEXT
)
LOCATION ('file://host/data/transactions*.csv')
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
    NULL 'NULL'
    QUOTE '"'
)
ENCODING 'UTF8';

-- Загрузка данных из external table в обычную таблицу
CREATE TABLE production.transactions (
    transaction_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount DECIMAL(10,2) CHECK (amount >= 0),
    transaction_date DATE NOT NULL,
    status TEXT,
    loaded_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (transaction_date)
(
    START (DATE '2024-01-01') INCLUSIVE
    END (DATE '2025-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);

INSERT INTO production.transactions 
    (transaction_id, customer_id, amount, transaction_date, status)
SELECT 
    transaction_id,
    customer_id,
    amount,
    transaction_date,
    status
FROM staging.ext_transactions_file
WHERE transaction_date >= '2024-01-01';

-- Пример 3.2: Readable External Table (S3/Object Storage)
-- --------------------------------------------------------

-- Конфигурация для Yandex Object Storage
-- Файл s3.conf должен содержать:
-- [default]
-- accessid = YOUR_ACCESS_KEY_ID
-- secret = YOUR_SECRET_ACCESS_KEY
-- threadnum = 4
-- chunksize = 67108864

CREATE EXTERNAL TABLE staging.ext_logs_s3 (
    log_id BIGINT,
    timestamp TIMESTAMP,
    user_id INTEGER,
    action TEXT,
    ip_address INET,
    user_agent TEXT
)
LOCATION (
    's3://bucket-name/logs/2024/11/*.csv 
    config=/path/to/s3.conf'
)
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
);

-- Пример 3.3: Writable External Table (для экспорта)
-- ---------------------------------------------------

CREATE WRITABLE EXTERNAL TABLE staging.ext_export_customers (
    customer_id INTEGER,
    full_name TEXT,
    email TEXT,
    total_purchases DECIMAL(10,2)
)
LOCATION ('file://host/exports/customers_export.csv')
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
    QUOTE '"'
)
ENCODING 'UTF8';

-- Экспорт данных
INSERT INTO staging.ext_export_customers
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name as full_name,
    c.email,
    COALESCE(SUM(t.amount), 0) as total_purchases
FROM examples.customers c
LEFT JOIN production.transactions t ON c.customer_id = t.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

-- ============================================
-- 4. РАБОТА С JSON ДАННЫМИ
-- ============================================

-- Пример 4.1: Создание таблиц с JSON колонками
-- ---------------------------------------------

CREATE TABLE examples.api_responses (
    response_id SERIAL PRIMARY KEY,
    endpoint TEXT,
    response_data JSONB,  -- JSONB вместо JSON для лучшей производительности
    status_code INTEGER,
    created_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (response_id);

-- Вставка JSON данных
INSERT INTO examples.api_responses (endpoint, response_data, status_code) VALUES
('/api/users/123', 
 '{"user_id": 123, "name": "John Doe", "email": "john@example.com", "settings": {"theme": "dark", "notifications": true}}',
 200),
('/api/orders/456',
 '{"order_id": 456, "customer_id": 123, "items": [{"id": 1, "name": "Product A", "qty": 2}, {"id": 2, "name": "Product B", "qty": 1}], "total": 99.99}',
 200),
('/api/search',
 '{"query": "laptop", "results": 42, "filters": {"price_min": 500, "price_max": 2000, "brands": ["Dell", "HP", "Lenovo"]}}',
 200);

-- Пример 4.2: Извлечение данных из JSON
-- --------------------------------------

-- Базовое извлечение значений
SELECT 
    response_id,
    endpoint,
    response_data->>'user_id' as user_id_text,
    (response_data->>'user_id')::INTEGER as user_id,
    response_data->>'name' as user_name,
    response_data->'settings'->>'theme' as theme
FROM examples.api_responses
WHERE endpoint = '/api/users/123';

-- Работа с JSON массивами
SELECT 
    response_id,
    response_data->>'order_id' as order_id,
    jsonb_array_elements(response_data->'items') as item
FROM examples.api_responses
WHERE endpoint = '/api/orders/456';

-- Извлечение элементов из массива
SELECT 
    response_id,
    item->>'id' as product_id,
    item->>'name' as product_name,
    (item->>'qty')::INTEGER as quantity
FROM examples.api_responses,
    jsonb_array_elements(response_data->'items') as item
WHERE endpoint = '/api/orders/456';

-- Пример 4.3: Фильтрация и поиск в JSON
-- --------------------------------------

-- Поиск по значению в JSON
SELECT * FROM examples.api_responses
WHERE response_data->>'user_id' = '123';

-- Проверка существования ключа
SELECT * FROM examples.api_responses
WHERE response_data ? 'order_id';

-- Проверка существования любого из ключей
SELECT * FROM examples.api_responses
WHERE response_data ?| array['order_id', 'user_id'];

-- Проверка существования всех ключей
SELECT * FROM examples.api_responses
WHERE response_data ?& array['query', 'results'];

-- Containment оператор (@>)
SELECT * FROM examples.api_responses
WHERE response_data @> '{"user_id": 123}';

-- Пример 4.4: Загрузка JSON из файлов
-- ------------------------------------

-- Создание таблицы для событий
CREATE TABLE examples.events (
    event_id SERIAL PRIMARY KEY,
    event_data JSONB,
    processed BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (event_id);

-- Временная таблица для импорта (NDJSON format)
CREATE TEMP TABLE temp_json_lines (json_line TEXT);

-- Загрузка построчного JSON
-- \copy temp_json_lines FROM '/data/events.json'

-- Парсинг и вставка
INSERT INTO examples.events (event_data)
SELECT json_line::JSONB
FROM temp_json_lines
WHERE json_line IS NOT NULL 
    AND json_line != ''
    AND json_line != 'null';

-- Пример 4.5: JSON индексирование
-- --------------------------------

-- GIN индекс для быстрого поиска
CREATE INDEX idx_api_responses_data ON examples.api_responses USING GIN (response_data);

-- Индекс на конкретное поле
CREATE INDEX idx_api_responses_user_id ON examples.api_responses ((response_data->>'user_id'));

-- Использование индексов
EXPLAIN ANALYZE
SELECT * FROM examples.api_responses
WHERE response_data @> '{"user_id": 123}';

-- Пример 4.6: Агрегация JSON данных
-- ----------------------------------

-- Подсчет по типам событий
SELECT 
    event_data->>'event_type' as event_type,
    count(*) as event_count,
    min(created_at) as first_occurrence,
    max(created_at) as last_occurrence
FROM examples.events
GROUP BY event_data->>'event_type'
ORDER BY event_count DESC;

-- Пример 4.7: Трансформация JSON в реляционные таблицы
-- -----------------------------------------------------

-- Создание нормализованных таблиц
CREATE TABLE production.users (
    user_id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    theme TEXT,
    notifications BOOLEAN
) DISTRIBUTED BY (user_id);

CREATE TABLE production.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    total DECIMAL(10,2)
) DISTRIBUTED BY (customer_id);

CREATE TABLE production.order_details (
    detail_id SERIAL PRIMARY KEY,
    order_id INTEGER,
    product_id INTEGER,
    product_name TEXT,
    quantity INTEGER
) DISTRIBUTED BY (order_id);

-- Загрузка пользователей из JSON
INSERT INTO production.users (user_id, name, email, theme, notifications)
SELECT 
    (response_data->>'user_id')::INTEGER,
    response_data->>'name',
    response_data->>'email',
    response_data->'settings'->>'theme',
    (response_data->'settings'->>'notifications')::BOOLEAN
FROM examples.api_responses
WHERE endpoint LIKE '/api/users/%';

-- Загрузка заказов из JSON
INSERT INTO production.orders (order_id, customer_id, total)
SELECT 
    (response_data->>'order_id')::INTEGER,
    (response_data->>'customer_id')::INTEGER,
    (response_data->>'total')::DECIMAL
FROM examples.api_responses
WHERE endpoint LIKE '/api/orders/%';

-- Загрузка деталей заказа из JSON массивов
INSERT INTO production.order_details (order_id, product_id, product_name, quantity)
SELECT 
    (response_data->>'order_id')::INTEGER as order_id,
    (item->>'id')::INTEGER as product_id,
    item->>'name' as product_name,
    (item->>'qty')::INTEGER as quantity
FROM examples.api_responses,
    jsonb_array_elements(response_data->'items') as item
WHERE endpoint LIKE '/api/orders/%';

-- ============================================
-- 5. РАБОТА С XML ДАННЫМИ
-- ============================================

-- Пример 5.1: Создание таблиц с XML
-- ----------------------------------

CREATE TABLE examples.xml_documents (
    doc_id SERIAL PRIMARY KEY,
    doc_name TEXT,
    xml_content XML,
    created_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (doc_id);

-- Вставка XML данных
INSERT INTO examples.xml_documents (doc_name, xml_content) VALUES
('invoice_001',
'<invoice>
    <invoice_id>INV-001</invoice_id>
    <customer>
        <id>5001</id>
        <name>Acme Corp</name>
        <email>contact@acme.com</email>
    </customer>
    <date>2024-11-14</date>
    <total>1500.00</total>
    <items>
        <item>
            <product>Laptop</product>
            <quantity>2</quantity>
            <price>600.00</price>
        </item>
        <item>
            <product>Mouse</product>
            <quantity>5</quantity>
            <price>60.00</price>
        </item>
    </items>
</invoice>');

-- Пример 5.2: XPath запросы
-- --------------------------

-- Базовое извлечение значений
SELECT 
    doc_id,
    xpath('/invoice/invoice_id/text()', xml_content) as invoice_id,
    xpath('/invoice/customer/name/text()', xml_content) as customer_name,
    xpath('/invoice/total/text()', xml_content) as total
FROM examples.xml_documents;

-- Извлечение с приведением типов
SELECT 
    doc_id,
    (xpath('/invoice/invoice_id/text()', xml_content))[1]::TEXT as invoice_id,
    (xpath('/invoice/customer/id/text()', xml_content))[1]::TEXT::INTEGER as customer_id,
    (xpath('/invoice/customer/name/text()', xml_content))[1]::TEXT as customer_name,
    (xpath('/invoice/date/text()', xml_content))[1]::TEXT::DATE as invoice_date,
    (xpath('/invoice/total/text()', xml_content))[1]::TEXT::DECIMAL as total
FROM examples.xml_documents;

-- Пример 5.3: Работа с вложенными элементами
-- -------------------------------------------

-- Извлечение элементов items (возвращает массив XML элементов)
SELECT 
    doc_id,
    unnest(xpath('/invoice/items/item', xml_content)) as item_xml
FROM examples.xml_documents;

-- Парсинг каждого item
WITH items_parsed AS (
    SELECT 
        doc_id,
        (xpath('/invoice/invoice_id/text()', xml_content))[1]::TEXT as invoice_id,
        unnest(xpath('/invoice/items/item', xml_content)) as item_xml
    FROM examples.xml_documents
)
SELECT 
    doc_id,
    invoice_id,
    (xpath('/item/product/text()', item_xml))[1]::TEXT as product,
    (xpath('/item/quantity/text()', item_xml))[1]::TEXT::INTEGER as quantity,
    (xpath('/item/price/text()', item_xml))[1]::TEXT::DECIMAL as price
FROM items_parsed;

-- Пример 5.4: XML функции
-- ------------------------

-- Проверка well-formed XML
SELECT 
    doc_id,
    doc_name,
    xml_is_well_formed(xml_content::TEXT) as is_valid_xml
FROM examples.xml_documents;

-- Извлечение всех текстовых узлов
SELECT 
    doc_id,
    unnest(xpath('//text()', xml_content)) as text_nodes
FROM examples.xml_documents;

-- Подсчет элементов
SELECT 
    doc_id,
    array_length(xpath('/invoice/items/item', xml_content), 1) as items_count
FROM examples.xml_documents;

-- Пример 5.5: Нормализация XML данных
-- ------------------------------------

-- Создание таблиц для нормализованных данных
CREATE TABLE production.invoices (
    invoice_id TEXT PRIMARY KEY,
    customer_id INTEGER,
    customer_name TEXT,
    customer_email TEXT,
    invoice_date DATE,
    total_amount DECIMAL(10,2)
) DISTRIBUTED BY (customer_id);

CREATE TABLE production.invoice_items (
    item_id SERIAL PRIMARY KEY,
    invoice_id TEXT,
    product_name TEXT,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    FOREIGN KEY (invoice_id) REFERENCES production.invoices(invoice_id)
) DISTRIBUTED BY (invoice_id);

-- Загрузка invoices
INSERT INTO production.invoices 
    (invoice_id, customer_id, customer_name, customer_email, invoice_date, total_amount)
SELECT 
    (xpath('/invoice/invoice_id/text()', xml_content))[1]::TEXT,
    (xpath('/invoice/customer/id/text()', xml_content))[1]::TEXT::INTEGER,
    (xpath('/invoice/customer/name/text()', xml_content))[1]::TEXT,
    (xpath('/invoice/customer/email/text()', xml_content))[1]::TEXT,
    (xpath('/invoice/date/text()', xml_content))[1]::TEXT::DATE,
    (xpath('/invoice/total/text()', xml_content))[1]::TEXT::DECIMAL
FROM examples.xml_documents;

-- Загрузка invoice items
WITH items_data AS (
    SELECT 
        (xpath('/invoice/invoice_id/text()', xml_content))[1]::TEXT as invoice_id,
        unnest(xpath('/invoice/items/item', xml_content)) as item_xml
    FROM examples.xml_documents
)
INSERT INTO production.invoice_items (invoice_id, product_name, quantity, unit_price)
SELECT 
    invoice_id,
    (xpath('/item/product/text()', item_xml))[1]::TEXT,
    (xpath('/item/quantity/text()', item_xml))[1]::TEXT::INTEGER,
    (xpath('/item/price/text()', item_xml))[1]::TEXT::DECIMAL
FROM items_data;

-- ============================================
-- 6. ОПТИМИЗАЦИЯ ЗАГРУЗКИ ДАННЫХ
-- ============================================

-- Пример 6.1: Staging таблицы
-- ----------------------------

-- Best Practice: Загрузка через staging таблицу

-- Шаг 1: Создание staging таблицы (без constraints, индексов)
CREATE TABLE staging.sales_staging (
    sale_id INTEGER,
    product_id INTEGER,
    customer_id INTEGER,
    quantity INTEGER,
    amount DECIMAL(10,2),
    sale_date DATE,
    region TEXT
) DISTRIBUTED BY (customer_id);

-- Шаг 2: Быстрая загрузка в staging
-- INSERT INTO staging.sales_staging SELECT * FROM ext_source;

-- Шаг 3: Data Quality Checks
DO $$
DECLARE
    null_count INTEGER;
    duplicate_count INTEGER;
    negative_amount_count INTEGER;
BEGIN
    -- Проверка NULL значений
    SELECT count(*) INTO null_count
    FROM staging.sales_staging
    WHERE customer_id IS NULL OR amount IS NULL;
    
    IF null_count > 0 THEN
        RAISE NOTICE 'Found % rows with NULL values', null_count;
    END IF;
    
    -- Проверка дубликатов
    SELECT count(*) INTO duplicate_count
    FROM (
        SELECT sale_id, count(*)
        FROM staging.sales_staging
        GROUP BY sale_id
        HAVING count(*) > 1
    ) dups;
    
    IF duplicate_count > 0 THEN
        RAISE WARNING 'Found % duplicate sale_ids', duplicate_count;
    END IF;
    
    -- Проверка бизнес-правил
    SELECT count(*) INTO negative_amount_count
    FROM staging.sales_staging
    WHERE amount < 0;
    
    IF negative_amount_count > 0 THEN
        RAISE WARNING 'Found % rows with negative amounts', negative_amount_count;
    END IF;
END $$;

-- Шаг 4: Очистка данных
DELETE FROM staging.sales_staging 
WHERE customer_id IS NULL OR amount IS NULL;

-- Обновление некорректных значений
UPDATE staging.sales_staging
SET amount = 0
WHERE amount < 0;

-- Шаг 5: Перенос в production
INSERT INTO production.sales
SELECT * FROM staging.sales_staging
ON CONFLICT (sale_id) DO NOTHING;  -- Пропускать дубликаты

-- Шаг 6: Обновление статистики
ANALYZE production.sales;

-- Шаг 7: Очистка staging
TRUNCATE staging.sales_staging;

-- Пример 6.2: Bulk Insert оптимизация
-- ------------------------------------

-- Отключение автостатистики для bulk операций
SET gp_autostats_mode = NONE;

-- Массовая загрузка
INSERT INTO production.large_table
SELECT * FROM staging.large_staging;

-- Ручное обновление статистики после загрузки
ANALYZE production.large_table;

-- Возврат автостатистики
SET gp_autostats_mode = ON_CHANGE;

-- Пример 6.3: Параллельная загрузка с несколькими файлами
-- --------------------------------------------------------

-- External table с несколькими файлами (wildcard)
CREATE EXTERNAL TABLE staging.ext_multi_files (
    col1 INTEGER,
    col2 TEXT,
    col3 DATE
)
LOCATION (
    'file://host/data/part1.csv',
    'file://host/data/part2.csv',
    'file://host/data/part3.csv'
)
FORMAT 'CSV' (HEADER DELIMITER ',');

-- Или с wildcard
CREATE EXTERNAL TABLE staging.ext_wildcard_files (
    col1 INTEGER,
    col2 TEXT,
    col3 DATE
)
LOCATION ('file://host/data/part*.csv')
FORMAT 'CSV' (HEADER DELIMITER ',');

-- Каждый сегмент будет обрабатывать свои файлы параллельно
INSERT INTO production.target_table
SELECT * FROM staging.ext_wildcard_files;

-- ============================================
-- 7. МОНИТОРИНГ И ДИАГНОСТИКА ЗАГРУЗКИ
-- ============================================

-- Пример 7.1: Проверка Data Skew
-- -------------------------------

SELECT 
    gp_segment_id,
    count(*) as row_count,
    pg_size_pretty(pg_relation_size('production.sales')) as segment_size
FROM production.sales
GROUP BY gp_segment_id
ORDER BY row_count DESC;

-- Расчет коэффициента перекоса
WITH segment_stats AS (
    SELECT 
        gp_segment_id,
        count(*) as row_count
    FROM production.sales
    GROUP BY gp_segment_id
)
SELECT 
    min(row_count) as min_rows,
    max(row_count) as max_rows,
    avg(row_count)::INTEGER as avg_rows,
    stddev(row_count)::INTEGER as stddev_rows,
    round(100.0 * (max(row_count) - min(row_count)) / NULLIF(avg(row_count), 0), 2) as skew_percent
FROM segment_stats;

-- Пример 7.2: Мониторинг активных загрузок
-- -----------------------------------------

SELECT 
    pid,
    usename as username,
    application_name,
    client_addr,
    backend_start,
    query_start,
    state_change,
    wait_event_type,
    wait_event,
    state,
    now() - query_start as duration,
    substring(query, 1, 100) as query_preview
FROM pg_stat_activity
WHERE query ILIKE '%INSERT%' 
    OR query ILIKE '%COPY%'
    OR state = 'active'
ORDER BY query_start;

-- Пример 7.3: Статистика по ошибкам загрузки
-- -------------------------------------------

-- Просмотр всех ошибок для таблицы
SELECT 
    cmdtime,
    relname,
    filename,
    linenum,
    errmsg,
    rawdata
FROM gp_read_error_log('staging.sales_import')
ORDER BY cmdtime DESC;

-- Группировка ошибок по типам
SELECT 
    errmsg,
    count(*) as error_count,
    min(linenum) as first_line,
    max(linenum) as last_line
FROM gp_read_error_log('staging.sales_import')
GROUP BY errmsg
ORDER BY error_count DESC;

-- ============================================
-- 8. УТИЛИТЫ И ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
-- ============================================

-- Функция для логирования загрузок
CREATE TABLE IF NOT EXISTS staging.load_log (
    log_id SERIAL PRIMARY KEY,
    table_name TEXT,
    rows_loaded INTEGER,
    rows_rejected INTEGER,
    load_duration INTERVAL,
    load_status TEXT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (log_id);

-- Функция для логирования
CREATE OR REPLACE FUNCTION staging.log_load_operation(
    p_table_name TEXT,
    p_rows_loaded INTEGER,
    p_rows_rejected INTEGER,
    p_duration INTERVAL,
    p_status TEXT,
    p_error TEXT DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO staging.load_log 
        (table_name, rows_loaded, rows_rejected, load_duration, load_status, error_message)
    VALUES 
        (p_table_name, p_rows_loaded, p_rows_rejected, p_duration, p_status, p_error);
END;
$$ LANGUAGE plpgsql;

-- Использование функции логирования
DO $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_count INTEGER;
BEGIN
    v_start_time := clock_timestamp();
    
    -- Ваша операция загрузки
    INSERT INTO production.target_table SELECT * FROM staging.source_table;
    GET DIAGNOSTICS v_rows_count = ROW_COUNT;
    
    v_end_time := clock_timestamp();
    
    -- Логирование
    PERFORM staging.log_load_operation(
        'production.target_table',
        v_rows_count,
        0,
        v_end_time - v_start_time,
        'SUCCESS'
    );
    
    RAISE NOTICE 'Loaded % rows in %', v_rows_count, v_end_time - v_start_time;
END $$;

-- Просмотр истории загрузок
SELECT 
    table_name,
    rows_loaded,
    rows_rejected,
    load_duration,
    load_status,
    created_at
FROM staging.load_log
ORDER BY created_at DESC
LIMIT 20;

-- ============================================
-- КОНЕЦ ПРИМЕРОВ
-- ============================================