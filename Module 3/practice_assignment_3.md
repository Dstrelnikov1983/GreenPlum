# Практическое задание №3: Проект загрузки и обработки данных

## Описание задания

В этом практическом задании вам предстоит разработать полноценный процесс загрузки и обработки данных для вымышленной e-commerce компании "TechStore".

## Бизнес-контекст

**TechStore** - интернет-магазин электроники, который получает данные из различных источников:
- Логи веб-сервера (CSV)
- События от мобильного приложения (JSON)
- Заказы из ERP системы (XML)

Ваша задача - разработать процесс загрузки, валидации и трансформации этих данных в GreenPlum для последующего анализа.

## Продолжительность

**2-3 часа** (рекомендуется разделить на несколько сессий)

## Требования к выполнению

### Обязательная часть (70% оценки):

1. ✅ Реализовать загрузку CSV, JSON и XML данных
2. ✅ Создать staging и production таблицы
3. ✅ Реализовать проверки качества данных
4. ✅ Обработать ошибки загрузки
5. ✅ Оптимизировать распределение данных

### Дополнительная часть (30% оценки):

6. ⭐ Автоматизировать процесс с помощью SQL функций
7. ⭐ Реализовать инкрементальную загрузку
8. ⭐ Создать dashboard для мониторинга
9. ⭐ Оптимизировать производительность
10. ⭐ Документировать решение

---

## Задание 1: Подготовка инфраструктуры (15 минут)

### 1.1 Создание базы данных и схем

```sql
-- Создайте базу данных для проекта
CREATE DATABASE techstore_dwh;
\c techstore_dwh

-- Создайте необходимые схемы
CREATE SCHEMA staging;     -- Для временных/сырых данных
CREATE SCHEMA production;  -- Для финальных данных
CREATE SCHEMA audit;       -- Для логов и мониторинга

-- Установите search path
SET search_path = staging, production, audit, public;
```

### 1.2 Создание audit таблиц

```sql
-- Таблица для логирования загрузок
CREATE TABLE audit.load_history (
    load_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    source_file TEXT,
    rows_loaded INTEGER,
    rows_rejected INTEGER,
    load_start TIMESTAMP,
    load_end TIMESTAMP,
    load_duration INTERVAL,
    status TEXT CHECK (status IN ('SUCCESS', 'FAILED', 'PARTIAL')),
    error_message TEXT
) DISTRIBUTED BY (load_id);

-- Таблица для data quality метрик
CREATE TABLE audit.data_quality_checks (
    check_id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    check_name TEXT NOT NULL,
    check_result TEXT CHECK (check_result IN ('PASS', 'FAIL', 'WARNING')),
    issue_count INTEGER,
    check_details TEXT,
    checked_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (check_id);
```

**Ожидаемый результат:** Созданы база данных, схемы и audit таблицы.

---

## Задание 2: Загрузка логов веб-сервера (CSV) (25 минут)

### 2.1 Дизайн таблиц

Создайте таблицы для хранения логов веб-сервера:

**Поля логов:**
- log_id (уникальный ID)
- timestamp (время запроса)
- user_id (ID пользователя, может быть NULL для анонимных)
- session_id (ID сессии)
- ip_address (IP адрес)
- request_method (GET, POST, etc.)
- request_url (URL запроса)
- response_code (HTTP статус код)
- response_time_ms (время ответа в миллисекундах)
- user_agent (браузер/устройство)
- referrer (откуда пришел пользователь)

```sql
-- Staging таблица (без constraints)
CREATE TABLE staging.web_logs_staging (
    -- РЕАЛИЗУЙТЕ СТРУКТУРУ САМОСТОЯТЕЛЬНО
    -- Подсказка: используйте подходящие типы данных
    -- Подсказка: пока не добавляйте PRIMARY KEY и FOREIGN KEY
) DISTRIBUTED BY (?);  -- Выберите подходящий ключ распределения

-- Production таблица (с constraints и партиционированием)
CREATE TABLE production.web_logs (
    -- РЕАЛИЗУЙТЕ СТРУКТУРУ САМОСТОЯТЕЛЬНО
    -- Требования:
    -- 1. PRIMARY KEY на log_id
    -- 2. NOT NULL на обязательные поля
    -- 3. CHECK constraints на response_code (100-599)
    -- 4. CHECK constraint на response_time_ms (>= 0)
    -- 5. Партиционирование по timestamp (по дням)
) DISTRIBUTED BY (?)
PARTITION BY RANGE (timestamp)
(
    -- РЕАЛИЗУЙТЕ ПАРТИЦИИ НА 7 ДНЕЙ
);
```

**Задание:** 
- Определите оптимальный ключ распределения (DISTRIBUTED BY)
- Обоснуйте свой выбор в комментарии
- Создайте партиции на 7 дней

### 2.2 Подготовка тестовых данных

Создайте CSV файл `web_logs_sample.csv` с минимум 100 строками:

```csv
log_id,timestamp,user_id,session_id,ip_address,request_method,request_url,response_code,response_time_ms,user_agent,referrer
1,2024-11-14 10:00:00,1001,sess_abc123,192.168.1.1,GET,/home,200,45,Mozilla/5.0,https://google.com
2,2024-11-14 10:00:15,NULL,sess_xyz789,192.168.1.2,GET,/products,200,120,Chrome/119.0,,
3,2024-11-14 10:01:00,1002,sess_def456,192.168.1.3,POST,/api/cart,201,89,Safari/17.0,https://site.com/products
...
```

**Подсказка:** Используйте Python/Excel для генерации 100+ строк или расширьте данные вручную.

### 2.3 Реализация загрузки

```sql
-- Создайте External Table для чтения из CSV
CREATE EXTERNAL TABLE staging.ext_web_logs (
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
) LOCATION (?)  -- Укажите путь к файлу или S3
FORMAT 'CSV' (?);  -- Настройте параметры

-- Функция для загрузки с логированием
CREATE OR REPLACE FUNCTION staging.load_web_logs(
    p_source_file TEXT
) RETURNS TABLE(
    rows_loaded INTEGER,
    rows_rejected INTEGER,
    status TEXT
) AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_count INTEGER;
    v_reject_count INTEGER;
BEGIN
    v_start_time := clock_timestamp();
    
    -- 1. ОЧИСТКА STAGING
    TRUNCATE staging.web_logs_staging;
    
    -- 2. ЗАГРУЗКА ИЗ EXTERNAL TABLE
    -- РЕАЛИЗУЙТЕ: INSERT INTO staging.web_logs_staging SELECT * FROM ...
    
    GET DIAGNOSTICS v_rows_count = ROW_COUNT;
    
    -- 3. ПОДСЧЕТ ОТКЛОНЕННЫХ СТРОК
    -- РЕАЛИЗУЙТЕ: Проверьте gp_read_error_log
    
    v_end_time := clock_timestamp();
    
    -- 4. ЛОГИРОВАНИЕ
    INSERT INTO audit.load_history 
        (table_name, source_file, rows_loaded, rows_rejected, 
         load_start, load_end, load_duration, status)
    VALUES 
        ('staging.web_logs_staging', p_source_file, v_rows_count, v_reject_count,
         v_start_time, v_end_time, v_end_time - v_start_time, 
         CASE WHEN v_reject_count = 0 THEN 'SUCCESS' ELSE 'PARTIAL' END);
    
    RETURN QUERY SELECT v_rows_count, v_reject_count, 'SUCCESS'::TEXT;
END;
$$ LANGUAGE plpgsql;
```

### 2.4 Data Quality Checks

Реализуйте проверки качества данных:

```sql
CREATE OR REPLACE FUNCTION staging.validate_web_logs()
RETURNS TABLE(
    check_name TEXT,
    check_result TEXT,
    issue_count INTEGER,
    details TEXT
) AS $$
BEGIN
    -- Проверка 1: NULL в обязательных полях
    RETURN QUERY
    SELECT 
        'NULL values in required fields'::TEXT,
        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END::TEXT,
        count(*)::INTEGER,
        'Found ' || count(*) || ' rows with NULL in timestamp or response_code'
    FROM staging.web_logs_staging
    WHERE timestamp IS NULL OR response_code IS NULL;
    
    -- Проверка 2: Некорректные HTTP коды
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
    -- Проверка 3: Отрицательное время ответа
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
    -- Проверка 4: Дубликаты log_id
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
    -- Проверка 5: Аномально высокое время ответа (> 10000 ms)
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
END;
$$ LANGUAGE plpgsql;

-- Запуск проверок
SELECT * FROM staging.validate_web_logs();

-- Сохранение результатов в audit
INSERT INTO audit.data_quality_checks (table_name, check_name, check_result, issue_count, check_details)
SELECT 'staging.web_logs_staging', check_name, check_result, issue_count, details
FROM staging.validate_web_logs();
```

### 2.5 Перенос в Production

```sql
-- Функция для переноса данных после валидации
CREATE OR REPLACE FUNCTION staging.promote_web_logs_to_production()
RETURNS INTEGER AS $$
DECLARE
    v_rows_count INTEGER;
BEGIN
    -- Вставка валидных данных
    INSERT INTO production.web_logs
    SELECT * FROM staging.web_logs_staging
    WHERE timestamp IS NOT NULL
        AND response_code IS NOT NULL
        AND response_code BETWEEN 100 AND 599
        AND response_time_ms >= 0
    ON CONFLICT (log_id) DO NOTHING;  -- Пропуск дубликатов
    
    GET DIAGNOSTICS v_rows_count = ROW_COUNT;
    
    -- Обновление статистики
    ANALYZE production.web_logs;
    
    RETURN v_rows_count;
END;
$$ LANGUAGE plpgsql;

-- Выполнение
SELECT staging.promote_web_logs_to_production();
```

**Контрольные вопросы:**
1. Сколько строк было загружено успешно?
2. Сколько строк было отклонено? Какие ошибки?
3. Какое время заняла загрузка?
4. Какой процент data skew у таблицы?

---

## Задание 3: Загрузка событий из приложения (JSON) (25 минут)

### 3.1 Дизайн таблиц

События мобильного приложения приходят в JSON формате со следующей структурой:

```json
{
    "event_id": "evt_123456",
    "user_id": 1001,
    "event_type": "screen_view",
    "timestamp": "2024-11-14T10:00:00Z",
    "device": {
        "platform": "iOS",
        "version": "17.0",
        "model": "iPhone 14"
    },
    "properties": {
        "screen_name": "ProductDetails",
        "product_id": 456,
        "time_spent": 45
    }
}
```

**Задание:** Создайте таблицы для хранения событий:

```sql
-- Staging: сырой JSON
CREATE TABLE staging.app_events_staging (
    event_id TEXT,
    event_json JSONB,
    loaded_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (event_id);

-- Production: нормализованная структура
CREATE TABLE production.app_events (
    -- РЕАЛИЗУЙТЕ СТРУКТУРУ САМОСТОЯТЕЛЬНО
    -- Поля: event_id, user_id, event_type, timestamp, 
    --        platform, device_version, device_model,
    --        properties (JSONB для гибкости)
) DISTRIBUTED BY (?)
PARTITION BY RANGE (timestamp)
(
    -- Партиции на 7 дней
);

-- Индексы для быстрого поиска
-- СОЗДАЙТЕ GIN индекс на properties
-- СОЗДАЙТЕ B-tree индексы на часто используемые поля
```

### 3.2 Загрузка и парсинг JSON

```sql
-- Загрузка из NDJSON файла
CREATE OR REPLACE FUNCTION staging.load_app_events(
    p_source_file TEXT
) RETURNS INTEGER AS $$
DECLARE
    v_rows_count INTEGER;
BEGIN
    -- Временная таблица для raw JSON
    CREATE TEMP TABLE temp_json_import (json_line TEXT);
    
    -- РЕАЛИЗУЙТЕ: загрузку из файла в temp_json_import
    
    -- Парсинг JSON и вставка в staging
    INSERT INTO staging.app_events_staging (event_id, event_json)
    SELECT 
        json_line::JSONB->>'event_id' as event_id,
        json_line::JSONB
    FROM temp_json_import
    WHERE json_line IS NOT NULL;
    
    GET DIAGNOSTICS v_rows_count = ROW_COUNT;
    
    DROP TABLE temp_json_import;
    
    RETURN v_rows_count;
END;
$$ LANGUAGE plpgsql;
```

### 3.3 Трансформация в Production

```sql
-- Извлечение данных из JSON и загрузка в production
INSERT INTO production.app_events 
    (event_id, user_id, event_type, timestamp, 
     platform, device_version, device_model, properties)
SELECT 
    event_json->>'event_id',
    (event_json->>'user_id')::INTEGER,
    event_json->>'event_type',
    (event_json->>'timestamp')::TIMESTAMP,
    event_json->'device'->>'platform',
    event_json->'device'->>'version',
    event_json->'device'->>'model',
    event_json->'properties'  -- Сохраняем properties как JSONB
FROM staging.app_events_staging
ON CONFLICT (event_id) DO NOTHING;
```

### 3.4 Аналитические запросы

Напишите запросы для анализа:

```sql
-- 1. Топ-10 экранов по просмотрам
-- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО

-- 2. Среднее время на экране по типу устройства
-- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО

-- 3. Воронка: screen_view -> add_to_cart -> purchase
-- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО

-- 4. Активные пользователи по платформам
-- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
```

---

## Задание 4: Загрузка заказов из ERP (XML) (25 минут)

### 4.1 Дизайн таблиц

XML файлы содержат информацию о заказах:

```xml
<order>
    <order_id>ORD-1001</order_id>
    <customer>
        <id>5001</id>
        <name>John Smith</name>
        <email>john@example.com</email>
    </customer>
    <order_date>2024-11-14</order_date>
    <total>350.00</total>
    <items>
        <item>
            <sku>PROD-123</sku>
            <name>Laptop</name>
            <quantity>1</quantity>
            <price>300.00</price>
        </item>
        <item>...</item>
    </items>
</order>
```

**Задание:** Создайте normalized таблицы:

```sql
-- Таблица заказов
CREATE TABLE production.orders (
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
) DISTRIBUTED BY (customer_id);

-- Таблица позиций заказов
CREATE TABLE production.order_items (
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
) DISTRIBUTED BY (order_id);
```

### 4.2 Парсинг XML и загрузка

```sql
-- Staging для XML
CREATE TABLE staging.orders_xml (
    doc_id SERIAL PRIMARY KEY,
    xml_content XML,
    loaded_at TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (doc_id);

-- Функция для парсинга и загрузки заказов
CREATE OR REPLACE FUNCTION staging.parse_and_load_orders()
RETURNS TABLE(orders_loaded INTEGER, items_loaded INTEGER) AS $$
DECLARE
    v_orders_count INTEGER;
    v_items_count INTEGER;
BEGIN
    -- Загрузка заказов
    INSERT INTO production.orders (order_id, customer_id, customer_name, ...)
    SELECT 
        -- РЕАЛИЗУЙТЕ: XPath запросы для извлечения данных
        (xpath('/order/order_id/text()', xml_content))[1]::TEXT,
        ...
    FROM staging.orders_xml;
    
    GET DIAGNOSTICS v_orders_count = ROW_COUNT;
    
    -- Загрузка items
    WITH items_data AS (
        -- РЕАЛИЗУЙТЕ: Извлечение вложенных элементов
    )
    INSERT INTO production.order_items (order_id, sku, ...)
    SELECT ...
    FROM items_data;
    
    GET DIAGNOSTICS v_items_count = ROW_COUNT;
    
    RETURN QUERY SELECT v_orders_count, v_items_count;
END;
$$ LANGUAGE plpgsql;
```

---

## Задание 5: Интеграция и аналитика (30 минут)

### 5.1 Создание consolidated view

Объедините все источники данных:

```sql
CREATE VIEW production.customer_360 AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    -- РЕАЛИЗУЙТЕ: статистики из web_logs
    -- РЕАЛИЗУЙТЕ: статистики из app_events
    -- РЕАЛИЗУЙТЕ: статистики из orders
FROM ...;
```

### 5.2 Создание аналитических отчетов

**Отчет 1: Daily Sales Dashboard**
```sql
CREATE VIEW production.daily_sales_dashboard AS
-- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
-- Должен включать: дата, количество заказов, сумма продаж, 
-- средний чек, количество уникальных клиентов
;
```

**Отчет 2: User Engagement Metrics**
```sql
CREATE VIEW production.user_engagement_metrics AS
-- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
-- Должен включать: user_id, количество визитов, количество событий,
-- время в приложении, последняя активность, RFM сегмент
;
```

### 5.3 Оптимизация производительности

**Задание:** Проанализируйте и оптимизируйте:

1. **Data Skew:**
```sql
-- Проверьте все production таблицы на data skew
-- Если skew > 10%, предложите решение
```

2. **Индексы:**
```sql
-- Создайте необходимые индексы на основе:
-- - Часто используемых WHERE conditions
-- - JOIN ключей
-- - Колонок для сортировки
```

3. **Compression:**
```sql
-- Примените column-oriented compression
ALTER TABLE production.web_logs SET WITH (appendonly=true, compresstype=zstd);
-- Повторите для других больших таблиц
```

---

## Задание 6: Автоматизация (Дополнительное, 30 минут)

### 6.1 Создание master загрузочной процедуры

```sql
CREATE OR REPLACE FUNCTION staging.run_daily_load()
RETURNS TABLE(
    step TEXT,
    status TEXT,
    details TEXT
) AS $$
BEGIN
    -- Шаг 1: Web Logs
    BEGIN
        PERFORM staging.load_web_logs('web_logs_daily.csv');
        RETURN QUERY SELECT 'Web Logs', 'SUCCESS', 'Loaded';
    EXCEPTION WHEN OTHERS THEN
        RETURN QUERY SELECT 'Web Logs', 'FAILED', SQLERRM;
    END;
    
    -- Шаг 2: App Events
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
    -- Шаг 3: Orders
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
    -- Шаг 4: Data Quality Checks
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
    -- Шаг 5: Promotion to Production
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
END;
$$ LANGUAGE plpgsql;
```

### 6.2 Инкрементальная загрузка

```sql
-- Таблица для отслеживания загрузок
CREATE TABLE audit.incremental_load_state (
    table_name TEXT PRIMARY KEY,
    last_loaded_timestamp TIMESTAMP,
    last_loaded_id BIGINT
) DISTRIBUTED RANDOMLY;

-- Функция для инкрементальной загрузки
CREATE OR REPLACE FUNCTION staging.incremental_load_web_logs()
RETURNS INTEGER AS $$
-- РЕАЛИЗУЙТЕ: Загружайте только новые записи с timestamp > last_loaded_timestamp
$$ LANGUAGE plpgsql;
```

---

## Задание 7: Мониторинг и отчетность (Дополнительное, 20 минут)

### 7.1 Dashboard для мониторинга загрузок

```sql
CREATE VIEW audit.load_dashboard AS
SELECT 
    table_name,
    count(*) as total_loads,
    count(*) FILTER (WHERE status = 'SUCCESS') as successful_loads,
    count(*) FILTER (WHERE status = 'FAILED') as failed_loads,
    sum(rows_loaded) as total_rows_loaded,
    sum(rows_rejected) as total_rows_rejected,
    avg(EXTRACT(EPOCH FROM load_duration)) as avg_load_time_seconds,
    max(load_end) as last_load_time
FROM audit.load_history
WHERE load_start >= current_date - interval '7 days'
GROUP BY table_name;
```

### 7.2 Alerts для проблем

```sql
-- Функция для проверки аномалий
CREATE OR REPLACE FUNCTION audit.check_load_anomalies()
RETURNS TABLE(
    alert_type TEXT,
    table_name TEXT,
    issue TEXT
) AS $$
BEGIN
    -- Алерт 1: Загрузка не выполнялась более 24 часов
    RETURN QUERY
    SELECT 
        'MISSING_LOAD'::TEXT,
        t.table_name,
        'Last load was ' || (now() - max(lh.load_end))::TEXT || ' ago'
    FROM (VALUES ('production.web_logs'), ('production.app_events'), ('production.orders')) t(table_name)
    LEFT JOIN audit.load_history lh ON lh.table_name = t.table_name
    GROUP BY t.table_name
    HAVING max(lh.load_end) < now() - interval '24 hours' OR max(lh.load_end) IS NULL;
    
    -- Алерт 2: Высокий процент rejected строк
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
    -- Алерт 3: Время загрузки значительно увеличилось
    -- РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
    
END;
$$ LANGUAGE plpgsql;

-- Запуск проверки
SELECT * FROM audit.check_load_anomalies();
```

---

## Критерии оценки

### Обязательная часть (70 баллов):

| Критерий | Баллы | Описание |
|----------|-------|----------|
| Таблицы созданы корректно | 15 | Правильные типы данных, constraints, партиционирование |
| Загрузка CSV работает | 15 | Данные загружаются без ошибок |
| Загрузка JSON работает | 15 | JSON корректно парсится и загружается |
| Загрузка XML работает | 15 | XML корректно парсится и загружается |
| Data Quality проверки | 10 | Реализованы валидации и логирование |

### Дополнительная часть (30 баллов):

| Критерий | Баллы | Описание |
|----------|-------|----------|
| Автоматизация | 10 | Master процедура для загрузки |
| Инкрементальная загрузка | 5 | Загружаются только новые данные |
| Оптимизация | 10 | Data skew < 10%, индексы, compression |
| Мониторинг | 5 | Dashboard и alerts |

### Бонусные баллы (до 20):

- Документация решения (+10)
- Юнит-тесты для функций (+5)
- Обработка edge cases (+5)

---

## Deliverables (Что нужно сдать)

### 1. SQL скрипты:
- `setup.sql` - создание всех таблиц и функций
- `load_data.sql` - примеры запуска загрузок
- `analytics.sql` - аналитические запросы и views

### 2. Отчет в Markdown:
- Описание архитектуры решения
- Обоснование выбора distribution keys
- Результаты data quality проверок
- Метрики производительности (время загрузки, data skew)
- Проблемы и их решения

### 3. Тестовые данные:
- Минимум 3 файла (CSV, JSON, XML)
- Общий объем данных: 1000+ записей

### 4. Скриншоты (опционально):
- Результаты выполнения загрузок
- Dashboard мониторинга
- EXPLAIN ANALYZE для сложных запросов

---

## Полезные подсказки

### Генерация тестовых данных:

**Python скрипт для CSV:**
```python
import csv
import random
from datetime import datetime, timedelta

# Генерация 1000 строк web_logs
# РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
```

**Python скрипт для JSON:**
```python
import json
from datetime import datetime

# Генерация 1000 событий
# РЕАЛИЗУЙТЕ САМОСТОЯТЕЛЬНО
```

### Типичные ошибки:

1. **Неправильный distribution key** → data skew
2. **Отсутствие партиционирования** → медленные запросы
3. **Загрузка напрямую в production** → нет валидации
4. **Забыли ANALYZE после загрузки** → плохие планы запросов

---

## Дополнительные ресурсы

- [GreenPlum Best Practices](https://docs.vmware.com/en/VMware-Greenplum/index.html)
- [PostgreSQL JSON Functions](https://www.postgresql.org/docs/9.4/functions-json.html)
- [XPath Tutorial](https://www.w3schools.com/xml/xpath_intro.asp)

---

## Сроки сдачи

**Deadline:** 7 дней с момента получения задания

**Формат сдачи:** 
- Pull request в репозиторий курса
- Или zip архив с файлами на email преподавателя

---

## Вопросы?

Если у вас возникли вопросы:
1. Проверьте FAQ в README курса
2. Спросите в чате курса
3. Напишите преподавателю

**Удачи! 🚀**