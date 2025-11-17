# Практический пример: Запуск хранимой процедуры GreenPlum из Apache Airflow

## 📋 Описание сценария

В этом примере мы создадим полный рабочий процесс, где Apache Airflow на удаленном сервере будет вызывать хранимые процедуры в GreenPlum для выполнения ETL операций.

**Сценарий:** Ежедневная обработка заказов e-commerce магазина
- Загрузка новых заказов из staging
- Расчет дневных агрегатов
- Обновление витрин данных для аналитики

## 🏗️ Архитектура

```
┌─────────────────────────────────────────────────────┐
│         Yandex Cloud Infrastructure                 │
│                                                      │
│  ┌──────────────────┐         ┌─────────────────┐  │
│  │  Apache Airflow  │ ──────► │   GreenPlum     │  │
│  │  (Remote Server) │  SSL    │   Database      │  │
│  │                  │         │                 │  │
│  │  - Scheduler     │         │  - Master Node  │  │
│  │  - Workers       │         │  - Segments     │  │
│  │  - Web UI        │         │  - Procedures   │  │
│  └──────────────────┘         └─────────────────┘  │
│                                                      │
└─────────────────────────────────────────────────────┘
```

## 🎯 Что мы создадим

1. **Хранимые процедуры в GreenPlum** - для бизнес-логики
2. **DAG в Airflow** - для оркестрации
3. **Connection настройки** - для безопасного подключения
4. **Мониторинг и логирование** - для отслеживания выполнения

---

## Часть 1: Подготовка GreenPlum (30 минут)

### Шаг 1.1: Создание схемы и таблиц

Подключитесь к GreenPlum и создайте необходимые структуры:

```bash
# Подключение к GreenPlum
psql "host=c-xxxxx.rw.mdb.yandexcloud.net port=6432 dbname=postgres user=admin sslmode=require"
```

```sql
-- ==========================================
-- 1. СОЗДАНИЕ СХЕМЫ ДЛЯ ETL ПРОЦЕССОВ
-- ==========================================

CREATE SCHEMA IF NOT EXISTS ecommerce;

-- ==========================================
-- 2. STAGING ТАБЛИЦА ДЛЯ ЗАГРУЗКИ ЗАКАЗОВ
-- ==========================================

CREATE TABLE ecommerce.orders_staging (
    order_id BIGINT,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_percent NUMERIC(5,2),
    order_date DATE,
    order_time TIME,
    status VARCHAR(20),
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED RANDOMLY;

-- Комментарий к таблице
COMMENT ON TABLE ecommerce.orders_staging IS 
'Временная таблица для загрузки заказов перед обработкой';

-- ==========================================
-- 3. PRODUCTION ТАБЛИЦА ЗАКАЗОВ
-- ==========================================

CREATE TABLE ecommerce.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price NUMERIC(10,2),
    discount_amount NUMERIC(10,2),
    total_amount NUMERIC(10,2),
    order_date DATE,
    order_time TIME,
    status VARCHAR(20),
    region VARCHAR(50),
    processed_at TIMESTAMP,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED BY (order_id)
PARTITION BY RANGE (order_date)
(
    START (DATE '2025-01-01') INCLUSIVE
    END (DATE '2026-01-01') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);

COMMENT ON TABLE ecommerce.orders IS 
'Основная таблица заказов с партиционированием по месяцам';

-- ==========================================
-- 4. ТАБЛИЦА ДНЕВНЫХ АГРЕГАТОВ
-- ==========================================

CREATE TABLE ecommerce.daily_sales_summary (
    summary_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue NUMERIC(15,2),
    total_discount NUMERIC(15,2),
    avg_order_value NUMERIC(10,2),
    unique_customers INTEGER,
    unique_products INTEGER,
    top_region VARCHAR(50),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED REPLICATED;

COMMENT ON TABLE ecommerce.daily_sales_summary IS 
'Ежедневные агрегаты продаж для быстрого доступа';

-- ==========================================
-- 5. ТАБЛИЦА ЛОГОВ ВЫПОЛНЕНИЯ ETL
-- ==========================================

CREATE TABLE ecommerce.etl_execution_log (
    execution_id SERIAL PRIMARY KEY,
    procedure_name VARCHAR(100),
    execution_date DATE,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    rows_processed INTEGER,
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED RANDOMLY;

COMMENT ON TABLE ecommerce.etl_execution_log IS 
'Лог выполнения ETL процедур для аудита и мониторинга';

-- Проверка созданных объектов
\dt ecommerce.*
```

### Шаг 1.2: Создание хранимой процедуры #1 - Обработка заказов

```sql
-- ==========================================
-- ПРОЦЕДУРА 1: ОБРАБОТКА ЗАКАЗОВ ИЗ STAGING
-- ==========================================

CREATE OR REPLACE PROCEDURE ecommerce.process_orders_from_staging()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_processed INTEGER := 0;
    v_execution_id INTEGER;
BEGIN
    -- Фиксируем время начала
    v_start_time := CLOCK_TIMESTAMP();
    
    -- Создаем запись о начале выполнения
    INSERT INTO ecommerce.etl_execution_log 
    (procedure_name, execution_date, start_time, status)
    VALUES 
    ('process_orders_from_staging', CURRENT_DATE, v_start_time, 'RUNNING')
    RETURNING execution_id INTO v_execution_id;
    
    RAISE NOTICE 'Starting order processing. Execution ID: %', v_execution_id;
    
    -- ==========================================
    -- ШАГ 1: ВАЛИДАЦИЯ ДАННЫХ
    -- ==========================================
    
    -- Удаляем записи с некорректными данными
    DELETE FROM ecommerce.orders_staging
    WHERE order_id IS NULL 
       OR customer_id IS NULL 
       OR product_id IS NULL
       OR quantity <= 0
       OR unit_price < 0;
    
    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    RAISE NOTICE 'Removed % invalid records from staging', v_rows_processed;
    
    -- ==========================================
    -- ШАГ 2: РАСЧЕТ ПРОИЗВОДНЫХ ПОЛЕЙ
    -- ==========================================
    
    -- Обновляем staging с вычисленными значениями
    UPDATE ecommerce.orders_staging
    SET 
        created_at = CURRENT_TIMESTAMP;
    
    -- ==========================================
    -- ШАГ 3: ЗАГРУЗКА В PRODUCTION (UPSERT)
    -- ==========================================
    
    -- Используем INSERT ... ON CONFLICT для обновления существующих записей
    WITH staged_orders AS (
        SELECT 
            order_id,
            customer_id,
            product_id,
            quantity,
            unit_price,
            -- Вычисляем discount_amount
            ROUND(unit_price * quantity * COALESCE(discount_percent, 0) / 100, 2) as discount_amount,
            -- Вычисляем total_amount
            ROUND(unit_price * quantity - 
                  (unit_price * quantity * COALESCE(discount_percent, 0) / 100), 2) as total_amount,
            order_date,
            order_time,
            status,
            region,
            CURRENT_TIMESTAMP as processed_at
        FROM ecommerce.orders_staging
    )
    INSERT INTO ecommerce.orders (
        order_id, customer_id, product_id, quantity, unit_price,
        discount_amount, total_amount, order_date, order_time,
        status, region, processed_at
    )
    SELECT * FROM staged_orders
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        product_id = EXCLUDED.product_id,
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        discount_amount = EXCLUDED.discount_amount,
        total_amount = EXCLUDED.total_amount,
        order_date = EXCLUDED.order_date,
        order_time = EXCLUDED.order_time,
        status = EXCLUDED.status,
        region = EXCLUDED.region,
        processed_at = EXCLUDED.processed_at,
        load_timestamp = CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    RAISE NOTICE 'Processed % orders into production table', v_rows_processed;
    
    -- ==========================================
    -- ШАГ 4: ОЧИСТКА STAGING
    -- ==========================================
    
    TRUNCATE TABLE ecommerce.orders_staging;
    RAISE NOTICE 'Staging table truncated';
    
    -- ==========================================
    -- ШАГ 5: ОБНОВЛЕНИЕ СТАТИСТИКИ
    -- ==========================================
    
    ANALYZE ecommerce.orders;
    RAISE NOTICE 'Statistics updated for orders table';
    
    -- Фиксируем успешное завершение
    v_end_time := CLOCK_TIMESTAMP();
    
    UPDATE ecommerce.etl_execution_log
    SET 
        end_time = v_end_time,
        duration_seconds = EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER,
        rows_processed = v_rows_processed,
        status = 'SUCCESS'
    WHERE execution_id = v_execution_id;
    
    RAISE NOTICE 'Order processing completed successfully. Duration: % seconds', 
                 EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER;
    
EXCEPTION
    WHEN OTHERS THEN
        -- Логирование ошибки
        UPDATE ecommerce.etl_execution_log
        SET 
            end_time = CLOCK_TIMESTAMP(),
            status = 'FAILED',
            error_message = SQLERRM
        WHERE execution_id = v_execution_id;
        
        RAISE NOTICE 'Error occurred: %', SQLERRM;
        RAISE;
END;
$$;

-- Комментарий к процедуре
COMMENT ON PROCEDURE ecommerce.process_orders_from_staging() IS
'Обрабатывает заказы из staging таблицы: валидирует, вычисляет метрики, загружает в production';
```

### Шаг 1.3: Создание хранимой процедуры #2 - Расчет агрегатов

```sql
-- ==========================================
-- ПРОЦЕДУРА 2: РАСЧЕТ ДНЕВНЫХ АГРЕГАТОВ
-- ==========================================

CREATE OR REPLACE PROCEDURE ecommerce.calculate_daily_summary(
    p_summary_date DATE DEFAULT CURRENT_DATE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_execution_id INTEGER;
    v_total_orders INTEGER;
BEGIN
    v_start_time := CLOCK_TIMESTAMP();
    
    -- Логирование начала выполнения
    INSERT INTO ecommerce.etl_execution_log 
    (procedure_name, execution_date, start_time, status)
    VALUES 
    ('calculate_daily_summary', p_summary_date, v_start_time, 'RUNNING')
    RETURNING execution_id INTO v_execution_id;
    
    RAISE NOTICE 'Calculating daily summary for date: %', p_summary_date;
    
    -- ==========================================
    -- РАСЧЕТ АГРЕГАТОВ
    -- ==========================================
    
    WITH daily_metrics AS (
        SELECT 
            p_summary_date as summary_date,
            COUNT(*) as total_orders,
            SUM(total_amount) as total_revenue,
            SUM(discount_amount) as total_discount,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers,
            COUNT(DISTINCT product_id) as unique_products
        FROM ecommerce.orders
        WHERE order_date = p_summary_date
    ),
    top_region_calc AS (
        SELECT region
        FROM ecommerce.orders
        WHERE order_date = p_summary_date
        GROUP BY region
        ORDER BY SUM(total_amount) DESC
        LIMIT 1
    )
    INSERT INTO ecommerce.daily_sales_summary (
        summary_date, total_orders, total_revenue, total_discount,
        avg_order_value, unique_customers, unique_products, top_region
    )
    SELECT 
        dm.summary_date,
        dm.total_orders,
        dm.total_revenue,
        dm.total_discount,
        dm.avg_order_value,
        dm.unique_customers,
        dm.unique_products,
        tr.region
    FROM daily_metrics dm
    CROSS JOIN top_region_calc tr
    ON CONFLICT (summary_date) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        total_discount = EXCLUDED.total_discount,
        avg_order_value = EXCLUDED.avg_order_value,
        unique_customers = EXCLUDED.unique_customers,
        unique_products = EXCLUDED.unique_products,
        top_region = EXCLUDED.top_region,
        calculated_at = CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS v_total_orders = ROW_COUNT;
    
    v_end_time := CLOCK_TIMESTAMP();
    
    -- Логирование успешного завершения
    UPDATE ecommerce.etl_execution_log
    SET 
        end_time = v_end_time,
        duration_seconds = EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER,
        rows_processed = v_total_orders,
        status = 'SUCCESS'
    WHERE execution_id = v_execution_id;
    
    RAISE NOTICE 'Daily summary calculated successfully for %', p_summary_date;
    
EXCEPTION
    WHEN OTHERS THEN
        UPDATE ecommerce.etl_execution_log
        SET 
            end_time = CLOCK_TIMESTAMP(),
            status = 'FAILED',
            error_message = SQLERRM
        WHERE execution_id = v_execution_id;
        
        RAISE;
END;
$$;

COMMENT ON PROCEDURE ecommerce.calculate_daily_summary(DATE) IS
'Вычисляет ежедневные агрегаты продаж для указанной даты';
```

### Шаг 1.4: Создание вспомогательной процедуры для очистки старых логов

```sql
-- ==========================================
-- ПРОЦЕДУРА 3: ОЧИСТКА СТАРЫХ ЛОГОВ
-- ==========================================

CREATE OR REPLACE PROCEDURE ecommerce.cleanup_old_logs(
    p_retention_days INTEGER DEFAULT 90
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_deleted_count INTEGER;
    v_cutoff_date DATE;
BEGIN
    v_cutoff_date := CURRENT_DATE - p_retention_days;
    
    RAISE NOTICE 'Cleaning up execution logs older than %', v_cutoff_date;
    
    DELETE FROM ecommerce.etl_execution_log
    WHERE execution_date < v_cutoff_date;
    
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    
    RAISE NOTICE 'Deleted % old log records', v_deleted_count;
    
    -- Обновляем статистику
    ANALYZE ecommerce.etl_execution_log;
END;
$$;

COMMENT ON PROCEDURE ecommerce.cleanup_old_logs(INTEGER) IS
'Удаляет записи логов старше указанного количества дней';
```

### Шаг 1.5: Создание тестовых данных

```sql
-- ==========================================
-- ЗАГРУЗКА ТЕСТОВЫХ ДАННЫХ
-- ==========================================

-- Очистка staging
TRUNCATE TABLE ecommerce.orders_staging;

-- Вставка тестовых заказов
INSERT INTO ecommerce.orders_staging 
(order_id, customer_id, product_id, quantity, unit_price, discount_percent, 
 order_date, order_time, status, region)
VALUES
    (1001, 101, 501, 2, 599.99, 10.00, CURRENT_DATE, '10:30:00', 'completed', 'North'),
    (1002, 102, 502, 1, 1299.99, 5.00, CURRENT_DATE, '11:15:00', 'completed', 'South'),
    (1003, 103, 503, 3, 49.99, 0.00, CURRENT_DATE, '12:00:00', 'completed', 'East'),
    (1004, 101, 504, 1, 299.99, 15.00, CURRENT_DATE, '13:30:00', 'pending', 'North'),
    (1005, 104, 501, 2, 599.99, 10.00, CURRENT_DATE, '14:00:00', 'completed', 'West'),
    (1006, 105, 505, 5, 19.99, 20.00, CURRENT_DATE, '15:45:00', 'completed', 'South'),
    (1007, 102, 502, 1, 1299.99, 5.00, CURRENT_DATE, '16:20:00', 'completed', 'East'),
    (1008, 106, 503, 4, 49.99, 0.00, CURRENT_DATE, '17:00:00', 'completed', 'North');

-- Проверка загруженных данных
SELECT COUNT(*) as staging_count FROM ecommerce.orders_staging;
```

### Шаг 1.6: Тестирование процедур вручную

```sql
-- ==========================================
-- РУЧНОЕ ТЕСТИРОВАНИЕ ПРОЦЕДУР
-- ==========================================

-- Тест 1: Обработка заказов
CALL ecommerce.process_orders_from_staging();

-- Проверка результатов
SELECT * FROM ecommerce.orders ORDER BY order_id;
SELECT * FROM ecommerce.etl_execution_log ORDER BY execution_id DESC LIMIT 5;

-- Тест 2: Расчет дневных агрегатов
CALL ecommerce.calculate_daily_summary(CURRENT_DATE);

-- Проверка результатов
SELECT * FROM ecommerce.daily_sales_summary ORDER BY summary_date DESC;

-- Тест 3: Очистка логов (безопасный тест с 0 дней)
CALL ecommerce.cleanup_old_logs(0);
```

---

## Часть 2: Настройка Apache Airflow (20 минут)

### Шаг 2.1: Создание Connection в Airflow

1. Откройте Airflow Web UI
2. Перейдите в **Admin → Connections**
3. Нажмите **+** (Add connection)
4. Заполните параметры:

```
Connection Id:    greenplum_prod
Connection Type:  Postgres
Host:             c-xxxxx.rw.mdb.yandexcloud.net
Schema:           postgres
Login:            admin
Password:         ********
Port:             6432
Extra:            {"sslmode": "require", "connect_timeout": 10}
```

5. Нажмите **Test** для проверки
6. Нажмите **Save**

### Шаг 2.2: Создание DAG файла

Создайте файл `dag_greenplum_procedures.py`:

```python
"""
DAG для запуска хранимых процедур GreenPlum
Ежедневная обработка заказов e-commerce магазина
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# ==========================================
# КОНФИГУРАЦИЯ DAG
# ==========================================

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'greenplum_daily_orders_etl',
    default_args=default_args,
    description='Daily ETL: Process orders and calculate sales summary',
    schedule_interval='0 2 * * *',  # Каждый день в 2:00 UTC
    catchup=False,
    tags=['greenplum', 'etl', 'procedures', 'ecommerce'],
    doc_md="""
    # Daily Orders ETL Pipeline
    
    Этот DAG выполняет ежедневную обработку заказов:
    1. Вызывает процедуру обработки заказов из staging
    2. Рассчитывает дневные агрегаты
    3. Очищает старые логи (раз в неделю)
    
    ## Расписание
    - Запуск: ежедневно в 02:00 UTC
    - Очистка логов: воскресенье в 03:00 UTC
    
    ## Мониторинг
    - Логи выполнения: ecommerce.etl_execution_log
    - Алерты: email при ошибках
    """
)

# ==========================================
# ЗАДАЧИ DAG
# ==========================================

# Задача 0: Начало выполнения
start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Задача 1: Проверка наличия данных в staging
check_staging_data = PostgresOperator(
    task_id='check_staging_has_data',
    postgres_conn_id='greenplum_prod',
    sql="""
        DO $$
        DECLARE
            v_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO v_count 
            FROM ecommerce.orders_staging;
            
            IF v_count = 0 THEN
                RAISE WARNING 'No data in staging table';
            ELSE
                RAISE NOTICE 'Found % records in staging', v_count;
            END IF;
        END $$;
    """,
    dag=dag,
)

# Задача 2: Вызов процедуры обработки заказов
process_orders = PostgresOperator(
    task_id='process_orders_from_staging',
    postgres_conn_id='greenplum_prod',
    sql="CALL ecommerce.process_orders_from_staging();",
    autocommit=True,
    dag=dag,
)

# Задача 3: Вызов процедуры расчета агрегатов
calculate_summary = PostgresOperator(
    task_id='calculate_daily_summary',
    postgres_conn_id='greenplum_prod',
    sql="CALL ecommerce.calculate_daily_summary(CURRENT_DATE);",
    autocommit=True,
    dag=dag,
)

# Задача 4: Проверка результатов
def verify_execution(**context):
    """
    Проверяет успешность выполнения через таблицу логов
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    hook = PostgresHook(postgres_conn_id='greenplum_prod')
    
    # Проверяем последние записи в логе
    query = """
        SELECT 
            procedure_name,
            status,
            rows_processed,
            duration_seconds,
            error_message
        FROM ecommerce.etl_execution_log
        WHERE execution_date = CURRENT_DATE
        ORDER BY execution_id DESC
        LIMIT 10;
    """
    
    results = hook.get_records(query)
    
    print("=" * 70)
    print("EXECUTION LOG - Last 10 records for today")
    print("=" * 70)
    
    failed_procedures = []
    for row in results:
        proc_name, status, rows, duration, error = row
        print(f"Procedure: {proc_name:<30} | Status: {status:<10}")
        print(f"  Rows processed: {rows or 0:<10} | Duration: {duration or 0} sec")
        if error:
            print(f"  Error: {error}")
            failed_procedures.append(proc_name)
        print("-" * 70)
    
    if failed_procedures:
        raise Exception(f"Failed procedures: {', '.join(failed_procedures)}")
    
    print("All procedures executed successfully!")

verify_results = PythonOperator(
    task_id='verify_execution_results',
    python_callable=verify_execution,
    dag=dag,
)

# Задача 5: Генерация отчета
def generate_daily_report(**context):
    """
    Генерирует отчет о ежедневных продажах
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    hook = PostgresHook(postgres_conn_id='greenplum_prod')
    
    query = """
        SELECT 
            summary_date,
            total_orders,
            total_revenue,
            total_discount,
            avg_order_value,
            unique_customers,
            unique_products,
            top_region
        FROM ecommerce.daily_sales_summary
        WHERE summary_date = CURRENT_DATE;
    """
    
    result = hook.get_first(query)
    
    if result:
        date, orders, revenue, discount, avg_val, customers, products, region = result
        
        print("\n" + "=" * 70)
        print(f"DAILY SALES REPORT - {date}")
        print("=" * 70)
        print(f"Total Orders:        {orders:>10}")
        print(f"Total Revenue:       ${revenue:>10,.2f}")
        print(f"Total Discount:      ${discount:>10,.2f}")
        print(f"Average Order Value: ${avg_val:>10,.2f}")
        print(f"Unique Customers:    {customers:>10}")
        print(f"Unique Products:     {products:>10}")
        print(f"Top Region:          {region:>10}")
        print("=" * 70 + "\n")
        
        # Можно отправить email или сохранить в файл
        context['ti'].xcom_push(key='daily_revenue', value=float(revenue))
        context['ti'].xcom_push(key='daily_orders', value=int(orders))
    else:
        print("No summary data found for today")

generate_report = PythonOperator(
    task_id='generate_daily_report',
    python_callable=generate_daily_report,
    dag=dag,
)

# Задача 6: Очистка старых логов (запускается только по воскресеньям)
cleanup_logs = PostgresOperator(
    task_id='cleanup_old_logs',
    postgres_conn_id='greenplum_prod',
    sql="CALL ecommerce.cleanup_old_logs(90);",  # Храним 90 дней
    autocommit=True,
    dag=dag,
)

# Задача 7: Завершение
end = DummyOperator(
    task_id='end',
    dag=dag,
)

# ==========================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# ==========================================

# Основной поток
start >> check_staging_data >> process_orders >> calculate_summary
calculate_summary >> verify_results >> generate_report

# Очистка логов (параллельно с основным потоком)
start >> cleanup_logs

# Все сходится в конце
[generate_report, cleanup_logs] >> end
```

### Шаг 2.3: Загрузка DAG в Airflow

```bash
# Если используете локальную установку Airflow
cp dag_greenplum_procedures.py ~/airflow/dags/

# Если используете Yandex Managed Airflow
aws s3 cp dag_greenplum_procedures.py s3://your-bucket/dags/ \
  --endpoint-url=https://storage.yandexcloud.net
```

---

## Часть 3: Запуск и мониторинг (15 минут)

### Шаг 3.1: Активация DAG

1. Откройте Airflow Web UI
2. Найдите DAG `greenplum_daily_orders_etl`
3. Включите DAG (toggle переключатель)
4. Нажмите **Trigger DAG** для ручного запуска

### Шаг 3.2: Мониторинг выполнения

**В Airflow UI:**

1. **Graph View** - визуализация задач и их статусов
2. **Gantt Chart** - время выполнения каждой задачи
3. **Task Logs** - детальные логи каждой задачи
4. **XCom** - данные, переданные между задачами

**В GreenPlum:**

```sql
-- Просмотр логов выполнения
SELECT 
    execution_id,
    procedure_name,
    execution_date,
    TO_CHAR(start_time, 'HH24:MI:SS') as start_time,
    TO_CHAR(end_time, 'HH24:MI:SS') as end_time,
    duration_seconds,
    rows_processed,
    status,
    CASE 
        WHEN error_message IS NOT NULL 
        THEN LEFT(error_message, 50) || '...'
        ELSE 'OK'
    END as error_summary
FROM ecommerce.etl_execution_log
WHERE execution_date >= CURRENT_DATE - 7
ORDER BY execution_id DESC;

-- Просмотр дневных агрегатов
SELECT 
    summary_date,
    total_orders,
    TO_CHAR(total_revenue, 'FM$999,999,990.00') as revenue,
    unique_customers,
    TO_CHAR(avg_order_value, 'FM$9,990.00') as avg_order,
    top_region
FROM ecommerce.daily_sales_summary
ORDER BY summary_date DESC
LIMIT 30;
```

### Шаг 3.3: Проверка результатов

```sql
-- Детальная статистика по обработанным заказам
SELECT 
    order_date,
    region,
    COUNT(*) as orders,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order
FROM ecommerce.orders
WHERE order_date >= CURRENT_DATE - 7
GROUP BY order_date, region
ORDER BY order_date DESC, revenue DESC;

-- Анализ производительности процедур
SELECT 
    procedure_name,
    COUNT(*) as executions,
    AVG(duration_seconds) as avg_duration,
    MAX(duration_seconds) as max_duration,
    MIN(duration_seconds) as min_duration,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as success_count,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count
FROM ecommerce.etl_execution_log
WHERE execution_date >= CURRENT_DATE - 30
GROUP BY procedure_name;
```

---

## Часть 4: Продвинутые сценарии (опционально)

### Вариант 1: Параметризованный вызов процедуры

```python
# В DAG добавьте задачу с параметрами
from airflow.models import Variable

process_date = "{{ ds }}"  # Дата выполнения DAG

calculate_custom_summary = PostgresOperator(
    task_id='calculate_summary_for_date',
    postgres_conn_id='greenplum_prod',
    sql=f"CALL ecommerce.calculate_daily_summary('{process_date}');",
    autocommit=True,
    dag=dag,
)
```

### Вариант 2: Условный запуск процедур

```python
from airflow.operators.python import BranchPythonOperator

def check_if_weekend(**context):
    """Определяет день недели"""
    from datetime import datetime
    execution_date = context['execution_date']
    
    # 5 = Saturday, 6 = Sunday
    if execution_date.weekday() in [5, 6]:
        return 'weekend_procedure'
    else:
        return 'weekday_procedure'

branch_task = BranchPythonOperator(
    task_id='check_day_of_week',
    python_callable=check_if_weekend,
    dag=dag,
)

weekend_proc = PostgresOperator(
    task_id='weekend_procedure',
    postgres_conn_id='greenplum_prod',
    sql="CALL ecommerce.weekend_special_processing();",
    dag=dag,
)

weekday_proc = PostgresOperator(
    task_id='weekday_procedure',
    postgres_conn_id='greenplum_prod',
    sql="CALL ecommerce.process_orders_from_staging();",
    dag=dag,
)

branch_task >> [weekend_proc, weekday_proc]
```

### Вариант 3: Динамический список процедур

```python
from airflow.operators.python import PythonOperator

def execute_procedure_list(**context):
    """Выполняет список процедур из конфигурации"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    procedures = [
        'ecommerce.process_orders_from_staging()',
        'ecommerce.calculate_daily_summary(CURRENT_DATE)',
        'ecommerce.update_customer_segments()',
        'ecommerce.refresh_product_recommendations()'
    ]
    
    hook = PostgresHook(postgres_conn_id='greenplum_prod')
    
    for proc in procedures:
        print(f"Executing: {proc}")
        hook.run(f"CALL {proc}")
        print(f"Completed: {proc}")

execute_all = PythonOperator(
    task_id='execute_procedure_list',
    python_callable=execute_procedure_list,
    dag=dag,
)
```

---

## 📊 Мониторинг и алертинг

### Dashboard в GreenPlum

```sql
-- Создание view для дашборда
CREATE OR REPLACE VIEW ecommerce.etl_monitoring_dashboard AS
SELECT 
    -- Общая статистика за сегодня
    (SELECT COUNT(*) FROM ecommerce.orders WHERE order_date = CURRENT_DATE) as todays_orders,
    (SELECT SUM(total_amount) FROM ecommerce.orders WHERE order_date = CURRENT_DATE) as todays_revenue,
    
    -- Статистика процедур за последние 24 часа
    (SELECT COUNT(*) FROM ecommerce.etl_execution_log 
     WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours' 
     AND status = 'SUCCESS') as successful_procedures_24h,
    
    (SELECT COUNT(*) FROM ecommerce.etl_execution_log 
     WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours' 
     AND status = 'FAILED') as failed_procedures_24h,
    
    -- Средняя длительность процедур
    (SELECT AVG(duration_seconds) FROM ecommerce.etl_execution_log 
     WHERE start_time >= CURRENT_TIMESTAMP - INTERVAL '24 hours') as avg_duration_24h,
    
    -- Последняя успешная обработка
    (SELECT MAX(end_time) FROM ecommerce.etl_execution_log 
     WHERE status = 'SUCCESS') as last_successful_run;

-- Использование
SELECT * FROM ecommerce.etl_monitoring_dashboard;
```

### Email алерты в Airflow

```python
from airflow.operators.email import EmailOperator

send_alert = EmailOperator(
    task_id='send_failure_alert',
    to='data-team@company.com',
    subject='[ALERT] GreenPlum ETL Failed',
    html_content="""
    <h3>ETL Execution Failed</h3>
    <p>Date: {{ ds }}</p>
    <p>DAG: {{ dag.dag_id }}</p>
    <p>Task: {{ task.task_id }}</p>
    <p>Check Airflow UI for details.</p>
    """,
    trigger_rule='one_failed',
    dag=dag,
)
```

---

## ✅ Проверочный список

### Для успешного запуска убедитесь:

- [ ] GreenPlum кластер доступен и работает
- [ ] Созданы все таблицы и процедуры
- [ ] Connection в Airflow настроен и проверен
- [ ] DAG файл загружен в Airflow
- [ ] Тестовые данные в staging таблице
- [ ] Процедуры протестированы вручную
- [ ] Security Groups разрешают подключение
- [ ] SSL сертификаты корректны

---

## 🎯 Ожидаемые результаты

После успешного выполнения DAG вы должны увидеть:

1. **В Airflow UI:**
   - Все задачи зеленые (success)
   - Логи показывают выполнение процедур
   - XCom содержит метрики (revenue, orders)

2. **В GreenPlum:**
   - Таблица `orders` содержит обработанные заказы
   - Таблица `daily_sales_summary` обновлена
   - Таблица `etl_execution_log` содержит записи SUCCESS

3. **Метрики производительности:**
   - process_orders: ~5-10 секунд для 1000 записей
   - calculate_summary: ~2-5 секунд
   - Весь DAG: ~1-2 минуты

---

## 🐛 Troubleshooting

### Проблема: Connection timeout

```bash
# Решение: Проверьте Security Groups
yc vpc security-group list-rules <SECURITY_GROUP_ID>

# Должны быть правила для:
# - Входящий трафик на порт 6432 (GreenPlum)
# - Исходящий трафик от Airflow
```

### Проблема: Process execution failed

```sql
-- Проверьте логи в GreenPlum
SELECT * FROM ecommerce.etl_execution_log 
WHERE status = 'FAILED' 
ORDER BY execution_id DESC 
LIMIT 5;

-- Проверьте наличие данных
SELECT COUNT(*) FROM ecommerce.orders_staging;
```

### Проблема: SSL connection error

```python
# В Connection добавьте в Extra:
{
    "sslmode": "require",
    "sslrootcert": "/path/to/ca.pem"  # если требуется
}
```

---

## 📚 Дополнительные ресурсы

- [PostgresOperator Documentation](https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/operators/postgres_operator_howto_guide.html)
- [GreenPlum Stored Procedures](https://docs.greenplum.org/6-24/admin_guide/query/topics/functions-operators.html)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

---

**Версия:** 1.0  
**Дата:** Ноябрь 2025  
**Совместимость:** Airflow 2.x, GreenPlum 6.x, Yandex Cloud
