# Практическое задание №4: ETL для аналитики e-commerce

## Описание задания

Вы - Data Engineer в интернет-магазине "TechStore Pro". Ваша задача - создать полноценный ETL-процесс для загрузки данных из различных источников в хранилище данных (GreenPlum) для последующей аналитики.

## Бизнес-требования

Аналитический отдел нуждается в ежедневных обновлениях данных для:
- Анализа продаж по регионам и категориям
- Расчета RFM-сегментации клиентов
- Мониторинга остатков товаров на складах
- Анализа эффективности маркетинговых кампаний

## Продолжительность

**120 минут** (рекомендуется)

## Исходные данные

Данные поступают из трех источников:
1. **Транзакции** - CSV файлы в S3 (обновляются каждый день)
2. **Каталог товаров** - JSON API endpoint
3. **Маркетинговые кампании** - CSV файлы в S3
4. **События веб-сайта** - JSON логи в S3

## Часть 1: Подготовка данных (20 минут)

### Задание 1.1: Генерация тестовых данных

Создайте следующие файлы данных:

#### Файл 1: `transactions_2025_01.csv`

```bash
cat > transactions_2025_01.csv << 'EOF'
transaction_id,customer_id,product_id,quantity,unit_price,discount_percent,transaction_date,payment_method,store_id
T2025010001,C10001,P5001,2,299.99,10,2025-01-15 14:23:15,card,ST01
T2025010002,C10002,P5003,1,149.99,0,2025-01-15 15:45:22,cash,ST01
T2025010003,C10001,P5002,3,89.99,15,2025-01-16 10:12:45,card,ST02
T2025010004,C10003,P5004,1,599.99,5,2025-01-16 11:34:12,card,ST01
T2025010005,C10004,P5001,1,299.99,0,2025-01-17 09:23:45,transfer,ST03
T2025010006,C10002,P5005,2,45.99,20,2025-01-17 16:45:33,card,ST02
T2025010007,C10005,P5003,1,149.99,10,2025-01-18 12:56:22,cash,ST01
T2025010008,C10001,P5006,4,29.99,0,2025-01-18 14:23:11,card,ST01
T2025010009,C10006,P5002,2,89.99,15,2025-01-19 10:45:55,card,ST03
T2025010010,C10003,P5004,1,599.99,10,2025-01-19 15:34:22,transfer,ST02
T2025010011,C10007,P5007,3,199.99,5,2025-01-20 11:22:33,card,ST01
T2025010012,C10004,P5001,2,299.99,10,2025-01-20 13:45:12,card,ST02
T2025010013,C10008,P5008,1,899.99,0,2025-01-21 09:15:44,card,ST01
T2025010014,C10005,P5003,2,149.99,15,2025-01-21 16:23:56,cash,ST03
T2025010015,C10002,P5005,5,45.99,25,2025-01-22 10:34:21,card,ST02
EOF
```

#### Файл 2: `products_catalog.json`

```bash
cat > products_catalog.json << 'EOF'
{
  "products": [
    {
      "product_id": "P5001",
      "name": "UltraBook Pro 15",
      "category": "Laptops",
      "subcategory": "Premium",
      "brand": "TechBrand",
      "cost_price": 180.00,
      "retail_price": 299.99,
      "weight_kg": 1.8,
      "warranty_months": 24,
      "supplier_id": "SUP001"
    },
    {
      "product_id": "P5002",
      "name": "Wireless Mouse X200",
      "category": "Accessories",
      "subcategory": "Peripherals",
      "brand": "InputMaster",
      "cost_price": 45.00,
      "retail_price": 89.99,
      "weight_kg": 0.15,
      "warranty_months": 12,
      "supplier_id": "SUP002"
    },
    {
      "product_id": "P5003",
      "name": "Mechanical Keyboard RGB",
      "category": "Accessories",
      "subcategory": "Peripherals",
      "brand": "KeyTech",
      "cost_price": 75.00,
      "retail_price": 149.99,
      "weight_kg": 0.9,
      "warranty_months": 24,
      "supplier_id": "SUP002"
    },
    {
      "product_id": "P5004",
      "name": "4K Monitor 27 inch",
      "category": "Monitors",
      "subcategory": "Premium",
      "brand": "ViewPro",
      "cost_price": 350.00,
      "retail_price": 599.99,
      "weight_kg": 5.5,
      "warranty_months": 36,
      "supplier_id": "SUP003"
    },
    {
      "product_id": "P5005",
      "name": "USB-C Cable 2m",
      "category": "Accessories",
      "subcategory": "Cables",
      "brand": "ConnectPlus",
      "cost_price": 15.00,
      "retail_price": 45.99,
      "weight_kg": 0.05,
      "warranty_months": 6,
      "supplier_id": "SUP004"
    },
    {
      "product_id": "P5006",
      "name": "Laptop Stand Aluminum",
      "category": "Accessories",
      "subcategory": "Stands",
      "brand": "ErgoDes",
      "cost_price": 18.00,
      "retail_price": 29.99,
      "weight_kg": 0.8,
      "warranty_months": 12,
      "supplier_id": "SUP004"
    },
    {
      "product_id": "P5007",
      "name": "External SSD 1TB",
      "category": "Storage",
      "subcategory": "External",
      "brand": "DataFast",
      "cost_price": 120.00,
      "retail_price": 199.99,
      "weight_kg": 0.12,
      "warranty_months": 60,
      "supplier_id": "SUP001"
    },
    {
      "product_id": "P5008",
      "name": "Webcam Pro 4K",
      "category": "Accessories",
      "subcategory": "Video",
      "brand": "CamTech",
      "cost_price": 450.00,
      "retail_price": 899.99,
      "weight_kg": 0.3,
      "warranty_months": 24,
      "supplier_id": "SUP003"
    }
  ],
  "last_updated": "2025-01-22T10:00:00Z"
}
EOF
```

#### Файл 3: `marketing_campaigns.csv`

```bash
cat > marketing_campaigns.csv << 'EOF'
campaign_id,campaign_name,start_date,end_date,budget,channel,target_category
CAMP001,Winter Sale 2025,2025-01-10,2025-01-31,50000,email,Laptops
CAMP002,Accessories Promotion,2025-01-15,2025-02-15,25000,social_media,Accessories
CAMP003,New Year Tech Deals,2025-01-01,2025-01-15,75000,display_ads,all
CAMP004,Premium Monitor Sale,2025-01-20,2025-02-28,30000,email,Monitors
EOF
```

#### Файл 4: `web_events_log.json`

```bash
cat > web_events_log.json << 'EOF'
[
  {
    "event_id": "E001",
    "event_type": "page_view",
    "customer_id": "C10001",
    "product_id": "P5001",
    "timestamp": "2025-01-15T14:20:00Z",
    "session_id": "SES12345",
    "device": "desktop",
    "campaign_id": "CAMP001"
  },
  {
    "event_id": "E002",
    "event_type": "add_to_cart",
    "customer_id": "C10001",
    "product_id": "P5001",
    "timestamp": "2025-01-15T14:22:00Z",
    "session_id": "SES12345",
    "device": "desktop",
    "campaign_id": "CAMP001"
  },
  {
    "event_id": "E003",
    "event_type": "page_view",
    "customer_id": "C10002",
    "product_id": "P5003",
    "timestamp": "2025-01-15T15:40:00Z",
    "session_id": "SES12346",
    "device": "mobile",
    "campaign_id": "CAMP002"
  },
  {
    "event_id": "E004",
    "event_type": "add_to_cart",
    "customer_id": "C10002",
    "product_id": "P5003",
    "timestamp": "2025-01-15T15:43:00Z",
    "session_id": "SES12346",
    "device": "mobile",
    "campaign_id": "CAMP002"
  },
  {
    "event_id": "E005",
    "event_type": "page_view",
    "customer_id": "C10003",
    "product_id": "P5004",
    "timestamp": "2025-01-16T11:30:00Z",
    "session_id": "SES12347",
    "device": "desktop",
    "campaign_id": "CAMP003"
  }
]
EOF
```

#### Файл 5: `stores.csv`

```bash
cat > stores.csv << 'EOF'
store_id,store_name,city,region,country,open_date,manager_name
ST01,TechStore Downtown,New York,Northeast,USA,2020-05-15,John Smith
ST02,TechStore Mall,Los Angeles,West,USA,2021-03-20,Jane Doe
ST03,TechStore Plaza,Chicago,Midwest,USA,2019-11-10,Bob Johnson
EOF
```

### Задание 1.2: Загрузка данных в S3

Загрузите все созданные файлы в Object Storage:

```bash
# Создайте структуру папок
aws s3 cp transactions_2025_01.csv s3://$BUCKET_NAME/input/transactions/2025/01/ \
  --endpoint-url=https://storage.yandexcloud.net

aws s3 cp products_catalog.json s3://$BUCKET_NAME/input/products/ \
  --endpoint-url=https://storage.yandexcloud.net

aws s3 cp marketing_campaigns.csv s3://$BUCKET_NAME/input/campaigns/ \
  --endpoint-url=https://storage.yandexcloud.net

aws s3 cp web_events_log.json s3://$BUCKET_NAME/input/events/ \
  --endpoint-url=https://storage.yandexcloud.net

aws s3 cp stores.csv s3://$BUCKET_NAME/input/stores/ \
  --endpoint-url=https://storage.yandexcloud.net

# Проверка
aws s3 ls s3://$BUCKET_NAME/input/ --recursive \
  --endpoint-url=https://storage.yandexcloud.net
```

## Часть 2: Проектирование схемы данных в GreenPlum (20 минут)

### Задание 2.1: Создание dimensional модели

Спроектируйте и создайте следующие таблицы:

```sql
-- Подключение к GreenPlum
-- psql "host=<GP_MASTER_FQDN> port=6432 dbname=postgres user=admin sslmode=require"

-- Схема для DWH
CREATE SCHEMA IF NOT EXISTS dwh;

-- Dimension: Продукты
CREATE TABLE dwh.dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20) UNIQUE NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    cost_price NUMERIC(10,2),
    retail_price NUMERIC(10,2),
    weight_kg NUMERIC(8,3),
    warranty_months INTEGER,
    supplier_id VARCHAR(20),
    valid_from TIMESTAMP DEFAULT now(),
    valid_to TIMESTAMP DEFAULT '9999-12-31'::timestamp,
    is_current BOOLEAN DEFAULT true
) DISTRIBUTED REPLICATED;

-- Dimension: Магазины
CREATE TABLE dwh.dim_stores (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(20) UNIQUE NOT NULL,
    store_name VARCHAR(200),
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    open_date DATE,
    manager_name VARCHAR(200)
) DISTRIBUTED REPLICATED;

-- Dimension: Маркетинговые кампании
CREATE TABLE dwh.dim_campaigns (
    campaign_key SERIAL PRIMARY KEY,
    campaign_id VARCHAR(20) UNIQUE NOT NULL,
    campaign_name VARCHAR(200),
    start_date DATE,
    end_date DATE,
    budget NUMERIC(12,2),
    channel VARCHAR(50),
    target_category VARCHAR(100)
) DISTRIBUTED REPLICATED;

-- Dimension: Дата (генерируется)
CREATE TABLE dwh.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
) DISTRIBUTED REPLICATED;

-- Fact: Транзакции
CREATE TABLE dwh.fact_transactions (
    transaction_key BIGSERIAL,
    transaction_id VARCHAR(50) NOT NULL,
    product_key INTEGER REFERENCES dwh.dim_products(product_key),
    store_key INTEGER REFERENCES dwh.dim_stores(store_key),
    date_key INTEGER REFERENCES dwh.dim_date(date_key),
    customer_id VARCHAR(20),
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_percent NUMERIC(5,2),
    discount_amount NUMERIC(10,2),
    total_amount NUMERIC(12,2),
    payment_method VARCHAR(50),
    transaction_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (transaction_timestamp)
(
    START (TIMESTAMP '2025-01-01 00:00:00') INCLUSIVE
    END (TIMESTAMP '2025-12-31 23:59:59') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);

-- Fact: События веб-сайта
CREATE TABLE dwh.fact_web_events (
    event_key BIGSERIAL,
    event_id VARCHAR(50),
    event_type VARCHAR(50),
    customer_id VARCHAR(20),
    product_key INTEGER REFERENCES dwh.dim_products(product_key),
    campaign_key INTEGER REFERENCES dwh.dim_campaigns(campaign_key),
    session_id VARCHAR(100),
    device VARCHAR(50),
    event_timestamp TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (event_timestamp)
(
    START (TIMESTAMP '2025-01-01 00:00:00') INCLUSIVE
    END (TIMESTAMP '2025-12-31 23:59:59') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);

-- Staging таблицы
CREATE TABLE dwh.stg_transactions (
    transaction_id VARCHAR(50),
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_percent NUMERIC(5,2),
    transaction_date TIMESTAMP,
    payment_method VARCHAR(50),
    store_id VARCHAR(20)
) DISTRIBUTED RANDOMLY;

CREATE TABLE dwh.stg_web_events (
    event_id VARCHAR(50),
    event_type VARCHAR(50),
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    timestamp TIMESTAMP,
    session_id VARCHAR(100),
    device VARCHAR(50),
    campaign_id VARCHAR(20)
) DISTRIBUTED RANDOMLY;
```

### Задание 2.2: Заполнение dimension таблицы дат

```sql
-- Генерация календаря на 2025 год
INSERT INTO dwh.dim_date (date_key, full_date, year, quarter, month, month_name, 
                          week, day_of_month, day_of_week, day_name, is_weekend, is_holiday)
SELECT 
    TO_CHAR(date_series, 'YYYYMMDD')::INTEGER as date_key,
    date_series::DATE as full_date,
    EXTRACT(YEAR FROM date_series)::INTEGER as year,
    EXTRACT(QUARTER FROM date_series)::INTEGER as quarter,
    EXTRACT(MONTH FROM date_series)::INTEGER as month,
    TO_CHAR(date_series, 'Month') as month_name,
    EXTRACT(WEEK FROM date_series)::INTEGER as week,
    EXTRACT(DAY FROM date_series)::INTEGER as day_of_month,
    EXTRACT(DOW FROM date_series)::INTEGER as day_of_week,
    TO_CHAR(date_series, 'Day') as day_name,
    CASE WHEN EXTRACT(DOW FROM date_series) IN (0,6) THEN true ELSE false END as is_weekend,
    false as is_holiday  -- Можно расширить логикой праздников
FROM generate_series(
    '2025-01-01'::timestamp,
    '2025-12-31'::timestamp,
    '1 day'::interval
) as date_series;

-- Проверка
SELECT * FROM dwh.dim_date LIMIT 10;
```

## Часть 3: Создание ETL DAG в Airflow (40 минут)

### Задание 3.1: Создание комплексного DAG

Создайте файл `etl_ecommerce_full.py` со следующей структурой:

**Требования к DAG:**
1. Загрузка dimensions (products, stores, campaigns)
2. Загрузка facts (transactions, web_events)
3. Валидация данных на каждом этапе
4. Обработка инкрементальных обновлений
5. Генерация отчета о загрузке
6. Обработка ошибок с retry logic

**Пример структуры DAG:**

```python
"""
Комплексный ETL для e-commerce аналитики
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

dag = DAG(
    'etl_ecommerce_full',
    default_args=default_args,
    description='Full ETL pipeline for e-commerce analytics',
    schedule_interval='0 2 * * *',  # Каждый день в 2:00
    catchup=False,
    tags=['etl', 'ecommerce', 'dwh'],
)

# Ваш код DAG здесь...
# 
# Структура задач:
# 1. start -> check_new_data
# 2. check_new_data -> [load_dimensions_group, skip_load]
# 3. load_dimensions_group -> load_facts_group
# 4. load_facts_group -> validate_data
# 5. validate_data -> generate_metrics
# 6. generate_metrics -> end
```

### Задание 3.2: Реализация задач загрузки

Реализуйте следующие функции:

#### 1. Загрузка продуктов из JSON

```python
def load_products_from_s3(**context):
    """
    Загрузка продуктов из JSON с обработкой SCD Type 2
    (Slowly Changing Dimensions)
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import json
    
    # ВАША РЕАЛИЗАЦИЯ
    # Требования:
    # 1. Прочитать JSON из S3
    # 2. Сравнить с существующими записями
    # 3. Закрыть старые версии (is_current=false, valid_to=now())
    # 4. Вставить новые версии
    
    pass
```

#### 2. Загрузка транзакций с вычислением метрик

```python
def load_transactions_from_s3(**context):
    """
    Загрузка транзакций с расчетом derived metrics
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import csv
    from io import StringIO
    
    # ВАША РЕАЛИЗАЦИЯ
    # Требования:
    # 1. Загрузить CSV из S3 в staging
    # 2. Обогатить данными из dimensions (получить surrogate keys)
    # 3. Вычислить: discount_amount = unit_price * quantity * discount_percent / 100
    # 4. Вычислить: total_amount = unit_price * quantity - discount_amount
    # 5. Загрузить в fact таблицу
    
    pass
```

#### 3. Валидация качества данных

```python
def validate_data_quality(**context):
    """
    Комплексная проверка качества данных
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='greenplum_default')
    
    # Проверки:
    # 1. Отсутствие NULL в критичных полях
    # 2. Referential integrity (все foreign keys существуют)
    # 3. Бизнес-правила (total_amount >= 0, quantity > 0)
    # 4. Дубликаты
    # 5. Orphan records
    
    validation_queries = [
        {
            'name': 'NULL_check',
            'query': """
                SELECT COUNT(*) as null_count
                FROM dwh.fact_transactions
                WHERE transaction_id IS NULL 
                   OR product_key IS NULL
                   OR store_key IS NULL
                   OR date_key IS NULL
            """,
            'threshold': 0
        },
        # Добавьте остальные проверки
    ]
    
    # ВАША РЕАЛИЗАЦИЯ
    pass
```

## Часть 4: Создание аналитических витрин (20 минут)

### Задание 4.1: Витрина продаж по категориям

```sql
-- Создайте материализованное представление
CREATE MATERIALIZED VIEW dwh.mart_sales_by_category AS
SELECT 
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    p.category,
    p.subcategory,
    s.region,
    COUNT(DISTINCT f.transaction_id) as transaction_count,
    SUM(f.quantity) as total_quantity,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_transaction_amount,
    SUM(f.discount_amount) as total_discount
FROM dwh.fact_transactions f
JOIN dwh.dim_products p ON f.product_key = p.product_key
JOIN dwh.dim_stores s ON f.store_key = s.store_key
JOIN dwh.dim_date d ON f.date_key = d.date_key
WHERE p.is_current = true
GROUP BY d.full_date, d.year, d.month, d.month_name, 
         p.category, p.subcategory, s.region
DISTRIBUTED BY (full_date, category);

-- Проверка
SELECT * FROM dwh.mart_sales_by_category 
ORDER BY full_date DESC, total_revenue DESC
LIMIT 20;
```

### Задание 4.2: Витрина эффективности кампаний

```sql
-- ВАША ЗАДАЧА: Создать витрину для анализа ROI маркетинговых кампаний
-- Требования:
-- 1. Связать web_events с транзакциями по customer_id и времени (±1 час)
-- 2. Рассчитать конверсию: (покупки / просмотры) * 100
-- 3. Рассчитать среднюю сумму заказа для каждой кампании
-- 4. Сравнить с бюджетом кампании
-- 5. Вычислить ROI: (revenue - budget) / budget * 100

CREATE MATERIALIZED VIEW dwh.mart_campaign_performance AS
-- ВАШ КОД ЗДЕСЬ
;
```

### Задание 4.3: RFM сегментация клиентов

```sql
-- Создайте представление для RFM анализа
CREATE OR REPLACE VIEW dwh.mart_customer_rfm AS
WITH customer_metrics AS (
    SELECT 
        customer_id,
        MAX(transaction_timestamp) as last_purchase_date,
        COUNT(DISTINCT transaction_id) as frequency,
        SUM(total_amount) as monetary
    FROM dwh.fact_transactions
    GROUP BY customer_id
),
rfm_scores AS (
    SELECT 
        customer_id,
        CURRENT_DATE - last_purchase_date::date as recency_days,
        frequency,
        monetary,
        -- RFM scores (1-5)
        NTILE(5) OVER (ORDER BY last_purchase_date DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) as m_score
    FROM customer_metrics
)
SELECT 
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    -- Комбинированный RFM score
    (r_score + f_score + m_score) as rfm_total,
    -- Сегментация
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
        ELSE 'Regular'
    END as customer_segment
FROM rfm_scores;

-- Проверка распределения по сегментам
SELECT 
    customer_segment,
    COUNT(*) as customer_count,
    AVG(monetary) as avg_lifetime_value,
    AVG(frequency) as avg_purchase_frequency
FROM dwh.mart_customer_rfm
GROUP BY customer_segment
ORDER BY customer_count DESC;
```

## Часть 5: Мониторинг и оптимизация (20 минут)

### Задание 5.1: Создание дашборда мониторинга ETL

```sql
-- Представление для мониторинга загрузок
CREATE OR REPLACE VIEW dwh.v_etl_monitoring AS
SELECT 
    'Transactions' as table_name,
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE load_timestamp::date = CURRENT_DATE) as loaded_today,
    MAX(load_timestamp) as last_load_time,
    MAX(transaction_timestamp) as latest_data_timestamp
FROM dwh.fact_transactions
UNION ALL
SELECT 
    'Web Events' as table_name,
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE load_timestamp::date = CURRENT_DATE) as loaded_today,
    MAX(load_timestamp) as last_load_time,
    MAX(event_timestamp) as latest_data_timestamp
FROM dwh.fact_web_events
UNION ALL
SELECT 
    'Products' as table_name,
    COUNT(*) as total_rows,
    COUNT(*) FILTER (WHERE valid_from::date = CURRENT_DATE) as loaded_today,
    MAX(valid_from) as last_load_time,
    NULL as latest_data_timestamp
FROM dwh.dim_products
WHERE is_current = true;

-- Просмотр статуса
SELECT * FROM dwh.v_etl_monitoring;
```

### Задание 5.2: Анализ производительности запросов

```sql
-- Найдите самые медленные запросы
SELECT 
    query,
    total_time,
    mean_time,
    calls,
    rows
FROM pg_stat_statements
WHERE query LIKE '%dwh.%'
ORDER BY mean_time DESC
LIMIT 10;

-- Анализ использования индексов
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname = 'dwh'
ORDER BY idx_scan;
```

### Задание 5.3: Оптимизация запросов

Оптимизируйте медленный запрос:

```sql
-- Исходный запрос (медленный)
EXPLAIN ANALYZE
SELECT 
    p.category,
    s.region,
    COUNT(*) as transaction_count,
    SUM(f.total_amount) as total_revenue
FROM dwh.fact_transactions f
JOIN dwh.dim_products p ON f.product_key = p.product_key
JOIN dwh.dim_stores s ON f.store_key = s.store_key
WHERE f.transaction_timestamp >= '2025-01-01'
GROUP BY p.category, s.region;

-- ЗАДАЧА: Оптимизируйте этот запрос используя:
-- 1. Материализованное представление
-- 2. Партиционирование
-- 3. Индексы
-- 4. DISTRIBUTED BY оптимизацию
```

## Критерии оценки

### Отлично (90-100 баллов):
- ✅ Все таблицы созданы с правильными distribution keys
- ✅ DAG работает без ошибок и обрабатывает все файлы
- ✅ Реализована обработка ошибок и retry logic
- ✅ Созданы все витрины данных
- ✅ Проведена оптимизация производительности
- ✅ Реализован мониторинг ETL процесса
- ✅ Код хорошо документирован

### Хорошо (75-89 баллов):
- ✅ Основные таблицы созданы
- ✅ DAG загружает большую часть данных
- ✅ Созданы основные витрины
- ✅ Проведен базовый мониторинг

### Удовлетворительно (60-74 балла):
- ✅ Схема данных создана (даже если не оптимально)
- ✅ Часть данных загружена
- ✅ Основные запросы работают

## Бонусные задания

### Бонус 1: Инкрементальная загрузка (+15 баллов)

Модифицируйте DAG для загрузки только новых/измененных данных:
- Отслеживание последней загруженной даты
- Загрузка только delta
- Обработка late arriving data

### Бонус 2: Data Quality Framework (+15 баллов)

Создайте фреймворк для автоматической проверки качества:
- Таблица с правилами валидации
- Автоматическое выполнение проверок
- Алертинг при нарушениях
- История проверок

### Бонус 3: Real-time метрики (+10 баллов)

Создайте представления для real-time мониторинга:
- Продажи за последний час
- Топ продуктов текущего дня
- Активность по сайту в реальном времени

## Отчет о выполнении

Подготовьте отчет (2-3 страницы) с:

1. **Архитектура решения:**
   - Схема data flow
   - Обоснование выбора distribution keys
   - Стратегия партиционирования

2. **ETL процесс:**
   - Описание каждого шага DAG
   - Обработка ошибок
   - Время выполнения

3. **Аналитические витрины:**
   - Описание каждой витрины
   - Примеры использования
   - Производительность запросов

4. **Оптимизация:**
   - Проблемы производительности
   - Примененные оптимизации
   - Результаты (до/после)

5. **Выводы и рекомендации:**
   - Что работает хорошо
   - Что можно улучшить
   - Рекомендации для production

## Полезные команды

```bash
# Просмотр логов Airflow
yc airflow cluster get etl-airflow-cluster --format json | jq -r '.webserver.url'

# Проверка файлов в S3
aws s3 ls s3://$BUCKET_NAME/ --recursive --endpoint-url=https://storage.yandexcloud.net

# Подключение к GreenPlum
psql "host=$GP_MASTER_FQDN port=6432 dbname=postgres user=admin sslmode=require"

# Мониторинг размера таблиц
psql -c "SELECT schemaname, tablename, pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) FROM pg_tables WHERE schemaname = 'dwh';"
```

## Сдача работы

Предоставьте:
1. ✅ DAG файл (`etl_ecommerce_full.py`)
2. ✅ SQL скрипты создания таблиц и витрин
3. ✅ Отчет в формате PDF или Markdown
4. ✅ Скриншоты Airflow Web UI (успешное выполнение DAG)
5. ✅ Результаты запросов к витринам данных

---

**Удачи в выполнении задания!** 🚀

Это задание приближено к реальным production сценариям и даст вам ценный опыт работы с комплексными ETL процессами.
