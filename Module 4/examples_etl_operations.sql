-- ============================================
-- Модуль 4: Примеры SQL для ETL процессов
-- Тема: Комплексные ETL операции в GreenPlum
-- ============================================

-- ============================================
-- 1. STAGING LAYER: Временное хранение данных
-- ============================================

-- Пример 1: Создание staging таблицы для загрузки из CSV
CREATE TABLE staging.sales_raw (
    order_id VARCHAR(50),
    customer_id VARCHAR(20),
    product_id VARCHAR(20),
    order_date VARCHAR(30),  -- Сначала как string для обработки ошибок
    quantity VARCHAR(10),
    price VARCHAR(20),
    status VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT now(),
    load_batch_id VARCHAR(50)
) DISTRIBUTED RANDOMLY;  -- RANDOMLY для быстрой вставки

-- Пример 2: Staging с обработкой ошибок
CREATE TABLE staging.sales_errors (
    raw_data TEXT,
    error_message TEXT,
    error_timestamp TIMESTAMP DEFAULT now()
) DISTRIBUTED RANDOMLY;

-- ============================================
-- 2. DATA VALIDATION: Проверка качества данных
-- ============================================

-- Валидация и очистка данных
CREATE OR REPLACE FUNCTION staging.validate_and_clean_sales()
RETURNS TABLE(
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    order_date DATE,
    quantity INTEGER,
    price NUMERIC(10,2),
    status VARCHAR(50),
    validation_status VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        -- Конвертация с обработкой ошибок
        CASE 
            WHEN s.order_id ~ '^\d+$' THEN s.order_id::INTEGER 
            ELSE NULL 
        END as order_id,
        
        CASE 
            WHEN s.customer_id ~ '^\d+$' THEN s.customer_id::INTEGER 
            ELSE NULL 
        END as customer_id,
        
        CASE 
            WHEN s.product_id ~ '^\d+$' THEN s.product_id::INTEGER 
            ELSE NULL 
        END as product_id,
        
        -- Обработка различных форматов дат
        CASE 
            WHEN s.order_date ~ '^\d{4}-\d{2}-\d{2}$' THEN s.order_date::DATE
            WHEN s.order_date ~ '^\d{2}/\d{2}/\d{4}$' THEN 
                TO_DATE(s.order_date, 'MM/DD/YYYY')
            ELSE NULL 
        END as order_date,
        
        CASE 
            WHEN s.quantity ~ '^\d+$' AND s.quantity::INTEGER > 0 THEN s.quantity::INTEGER
            ELSE NULL 
        END as quantity,
        
        CASE 
            WHEN s.price ~ '^\d+\.?\d*$' AND s.price::NUMERIC >= 0 THEN s.price::NUMERIC(10,2)
            ELSE NULL 
        END as price,
        
        UPPER(TRIM(s.status)) as status,
        
        -- Статус валидации
        CASE 
            WHEN s.order_id IS NULL OR s.customer_id IS NULL OR 
                 s.product_id IS NULL OR s.order_date IS NULL OR 
                 s.quantity IS NULL OR s.price IS NULL 
            THEN 'INVALID'
            ELSE 'VALID'
        END as validation_status
    FROM staging.sales_raw s;
END;
$$ LANGUAGE plpgsql;

-- Использование функции валидации
WITH validated_data AS (
    SELECT * FROM staging.validate_and_clean_sales()
)
-- Загрузка валидных данных
INSERT INTO dwh.fact_sales (order_id, customer_id, product_id, order_date, quantity, price, status)
SELECT order_id, customer_id, product_id, order_date, quantity, price, status
FROM validated_data
WHERE validation_status = 'VALID';

-- Логирование ошибочных записей
INSERT INTO staging.sales_errors (raw_data, error_message)
SELECT 
    ROW(s.*)::TEXT as raw_data,
    'Validation failed' as error_message
FROM staging.sales_raw s
LEFT JOIN (
    SELECT order_id FROM staging.validate_and_clean_sales() WHERE validation_status = 'VALID'
) v ON s.order_id::INTEGER = v.order_id
WHERE v.order_id IS NULL;

-- ============================================
-- 3. SLOWLY CHANGING DIMENSIONS (SCD Type 2)
-- ============================================

-- Пример: Обработка изменений в справочнике продуктов
CREATE OR REPLACE PROCEDURE dwh.load_product_dimension(
    p_product_id INTEGER,
    p_product_name VARCHAR(200),
    p_category VARCHAR(100),
    p_price NUMERIC(10,2)
)
LANGUAGE plpgsql AS $$
DECLARE
    v_existing_record RECORD;
    v_changes_detected BOOLEAN := FALSE;
BEGIN
    -- Проверка существующей текущей записи
    SELECT * INTO v_existing_record
    FROM dwh.dim_products
    WHERE product_id = p_product_id
      AND is_current = TRUE;
    
    IF FOUND THEN
        -- Проверка изменений
        IF v_existing_record.product_name != p_product_name OR
           v_existing_record.category != p_category OR
           v_existing_record.price != p_price THEN
            v_changes_detected := TRUE;
        END IF;
        
        IF v_changes_detected THEN
            -- Закрытие старой записи
            UPDATE dwh.dim_products
            SET is_current = FALSE,
                valid_to = CURRENT_TIMESTAMP
            WHERE product_id = p_product_id
              AND is_current = TRUE;
            
            -- Вставка новой версии
            INSERT INTO dwh.dim_products (
                product_id, product_name, category, price,
                is_current, valid_from, valid_to
            ) VALUES (
                p_product_id, p_product_name, p_category, p_price,
                TRUE, CURRENT_TIMESTAMP, '9999-12-31'::TIMESTAMP
            );
            
            RAISE NOTICE 'Product % updated with new version', p_product_id;
        ELSE
            RAISE NOTICE 'No changes detected for product %', p_product_id;
        END IF;
    ELSE
        -- Вставка новой записи
        INSERT INTO dwh.dim_products (
            product_id, product_name, category, price,
            is_current, valid_from, valid_to
        ) VALUES (
            p_product_id, p_product_name, p_category, p_price,
            TRUE, CURRENT_TIMESTAMP, '9999-12-31'::TIMESTAMP
        );
        
        RAISE NOTICE 'New product % inserted', p_product_id;
    END IF;
END;
$$;

-- Массовая обработка SCD Type 2
CREATE OR REPLACE PROCEDURE dwh.bulk_load_products()
LANGUAGE plpgsql AS $$
BEGIN
    -- Закрытие записей, которые больше не существуют в источнике
    UPDATE dwh.dim_products d
    SET is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP
    WHERE d.is_current = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM staging.products_new s
          WHERE s.product_id = d.product_id
      );
    
    -- Обработка изменений
    WITH changed_products AS (
        SELECT 
            s.product_id,
            s.product_name,
            s.category,
            s.price
        FROM staging.products_new s
        LEFT JOIN dwh.dim_products d ON s.product_id = d.product_id AND d.is_current = TRUE
        WHERE d.product_id IS NULL  -- Новые продукты
           OR d.product_name != s.product_name  -- Изменения
           OR d.category != s.category
           OR d.price != s.price
    )
    -- Закрытие старых версий
    UPDATE dwh.dim_products d
    SET is_current = FALSE,
        valid_to = CURRENT_TIMESTAMP
    FROM changed_products c
    WHERE d.product_id = c.product_id
      AND d.is_current = TRUE;
    
    -- Вставка новых версий
    INSERT INTO dwh.dim_products (product_id, product_name, category, price, is_current, valid_from, valid_to)
    SELECT 
        product_id, 
        product_name, 
        category, 
        price,
        TRUE,
        CURRENT_TIMESTAMP,
        '9999-12-31'::TIMESTAMP
    FROM staging.products_new s
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.dim_products d
        WHERE d.product_id = s.product_id
          AND d.is_current = TRUE
          AND d.product_name = s.product_name
          AND d.category = s.category
          AND d.price = s.price
    );
    
    RAISE NOTICE 'Products dimension updated successfully';
END;
$$;

-- ============================================
-- 4. INCREMENTAL LOADING: Инкрементальная загрузка
-- ============================================

-- Таблица для отслеживания последней загрузки
CREATE TABLE dwh.etl_control (
    table_name VARCHAR(100) PRIMARY KEY,
    last_load_timestamp TIMESTAMP,
    last_load_date DATE,
    last_processed_id BIGINT,
    rows_loaded INTEGER,
    status VARCHAR(20),
    updated_at TIMESTAMP DEFAULT now()
) DISTRIBUTED REPLICATED;

-- Функция инкрементальной загрузки
CREATE OR REPLACE FUNCTION dwh.incremental_load_transactions()
RETURNS INTEGER AS $$
DECLARE
    v_last_timestamp TIMESTAMP;
    v_rows_loaded INTEGER;
BEGIN
    -- Получение последней загруженной даты
    SELECT last_load_timestamp INTO v_last_timestamp
    FROM dwh.etl_control
    WHERE table_name = 'fact_transactions';
    
    -- Если первая загрузка, используем начальную дату
    IF v_last_timestamp IS NULL THEN
        v_last_timestamp := '2000-01-01'::TIMESTAMP;
    END IF;
    
    -- Загрузка только новых данных
    INSERT INTO dwh.fact_transactions (
        transaction_id, customer_id, product_key, store_key, 
        date_key, quantity, unit_price, total_amount, transaction_timestamp
    )
    SELECT 
        s.transaction_id,
        s.customer_id,
        p.product_key,
        st.store_key,
        TO_CHAR(s.transaction_date, 'YYYYMMDD')::INTEGER as date_key,
        s.quantity,
        s.unit_price,
        s.quantity * s.unit_price * (1 - s.discount_percent / 100.0) as total_amount,
        s.transaction_date
    FROM staging.transactions_raw s
    JOIN dwh.dim_products p ON s.product_id = p.product_id AND p.is_current = TRUE
    JOIN dwh.dim_stores st ON s.store_id = st.store_id
    WHERE s.transaction_date > v_last_timestamp
      AND s.transaction_date <= CURRENT_TIMESTAMP;
    
    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;
    
    -- Обновление control таблицы
    INSERT INTO dwh.etl_control (table_name, last_load_timestamp, last_load_date, rows_loaded, status)
    VALUES ('fact_transactions', CURRENT_TIMESTAMP, CURRENT_DATE, v_rows_loaded, 'SUCCESS')
    ON CONFLICT (table_name) DO UPDATE SET
        last_load_timestamp = EXCLUDED.last_load_timestamp,
        last_load_date = EXCLUDED.last_load_date,
        rows_loaded = EXCLUDED.rows_loaded,
        status = EXCLUDED.status,
        updated_at = CURRENT_TIMESTAMP;
    
    RAISE NOTICE 'Loaded % new transactions', v_rows_loaded;
    
    RETURN v_rows_loaded;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- 5. DATA DEDUPLICATION: Удаление дубликатов
-- ============================================

-- Поиск дубликатов
SELECT 
    transaction_id,
    COUNT(*) as duplicate_count
FROM dwh.fact_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- Удаление дубликатов (оставляем запись с последним load_timestamp)
DELETE FROM dwh.fact_transactions t1
WHERE EXISTS (
    SELECT 1 
    FROM dwh.fact_transactions t2
    WHERE t1.transaction_id = t2.transaction_id
      AND t1.load_timestamp < t2.load_timestamp
);

-- Альтернатива: Использование window functions
WITH ranked_transactions AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id 
            ORDER BY load_timestamp DESC
        ) as rn
    FROM dwh.fact_transactions
)
DELETE FROM dwh.fact_transactions
WHERE transaction_key IN (
    SELECT transaction_key 
    FROM ranked_transactions 
    WHERE rn > 1
);

-- ============================================
-- 6. DATA AGGREGATION: Предагрегация данных
-- ============================================

-- Создание агрегированной таблицы для ускорения отчетов
CREATE TABLE dwh.agg_daily_sales (
    date_key INTEGER,
    product_key INTEGER,
    store_key INTEGER,
    transaction_count INTEGER,
    total_quantity INTEGER,
    total_revenue NUMERIC(12,2),
    avg_transaction_amount NUMERIC(10,2),
    unique_customers INTEGER,
    load_timestamp TIMESTAMP DEFAULT now(),
    PRIMARY KEY (date_key, product_key, store_key)
) DISTRIBUTED BY (date_key);

-- Процедура обновления агрегатов
CREATE OR REPLACE PROCEDURE dwh.refresh_daily_aggregates(p_date DATE)
LANGUAGE plpgsql AS $$
BEGIN
    -- Удаление старых данных за дату
    DELETE FROM dwh.agg_daily_sales
    WHERE date_key = TO_CHAR(p_date, 'YYYYMMDD')::INTEGER;
    
    -- Вычисление и вставка новых агрегатов
    INSERT INTO dwh.agg_daily_sales (
        date_key, product_key, store_key,
        transaction_count, total_quantity, total_revenue,
        avg_transaction_amount, unique_customers
    )
    SELECT 
        TO_CHAR(p_date, 'YYYYMMDD')::INTEGER as date_key,
        product_key,
        store_key,
        COUNT(DISTINCT transaction_id) as transaction_count,
        SUM(quantity) as total_quantity,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_transaction_amount,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM dwh.fact_transactions
    WHERE transaction_timestamp::DATE = p_date
    GROUP BY product_key, store_key;
    
    RAISE NOTICE 'Daily aggregates refreshed for date: %', p_date;
END;
$$;

-- ============================================
-- 7. ERROR HANDLING: Обработка ошибок в ETL
-- ============================================

-- Таблица для логирования ETL процессов
CREATE TABLE dwh.etl_log (
    log_id BIGSERIAL PRIMARY KEY,
    process_name VARCHAR(100),
    step_name VARCHAR(100),
    status VARCHAR(20),
    rows_affected INTEGER,
    error_message TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INTEGER
) DISTRIBUTED RANDOMLY;

-- Функция-обертка для безопасного выполнения ETL шагов
CREATE OR REPLACE FUNCTION dwh.execute_etl_step(
    p_process_name VARCHAR,
    p_step_name VARCHAR,
    p_sql_statement TEXT
) RETURNS BOOLEAN AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_end_time TIMESTAMP;
    v_rows_affected INTEGER;
    v_error_message TEXT;
    v_success BOOLEAN := TRUE;
BEGIN
    v_start_time := CLOCK_TIMESTAMP();
    
    BEGIN
        EXECUTE p_sql_statement;
        GET DIAGNOSTICS v_rows_affected = ROW_COUNT;
        
        v_end_time := CLOCK_TIMESTAMP();
        
        -- Логирование успешного выполнения
        INSERT INTO dwh.etl_log (
            process_name, step_name, status, rows_affected,
            start_time, end_time, duration_seconds
        ) VALUES (
            p_process_name, p_step_name, 'SUCCESS', v_rows_affected,
            v_start_time, v_end_time, 
            EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER
        );
        
        RAISE NOTICE 'Step % completed successfully. Rows affected: %', p_step_name, v_rows_affected;
        
    EXCEPTION WHEN OTHERS THEN
        v_success := FALSE;
        v_error_message := SQLERRM;
        v_end_time := CLOCK_TIMESTAMP();
        
        -- Логирование ошибки
        INSERT INTO dwh.etl_log (
            process_name, step_name, status, error_message,
            start_time, end_time, duration_seconds
        ) VALUES (
            p_process_name, p_step_name, 'ERROR', v_error_message,
            v_start_time, v_end_time,
            EXTRACT(EPOCH FROM (v_end_time - v_start_time))::INTEGER
        );
        
        RAISE WARNING 'Step % failed with error: %', p_step_name, v_error_message;
    END;
    
    RETURN v_success;
END;
$$ LANGUAGE plpgsql;

-- Использование error handling
DO $$
DECLARE
    v_success BOOLEAN;
BEGIN
    -- Шаг 1: Очистка staging
    v_success := dwh.execute_etl_step(
        'Daily Sales ETL',
        'Truncate Staging',
        'TRUNCATE TABLE staging.sales_raw'
    );
    
    IF NOT v_success THEN
        RAISE EXCEPTION 'ETL process failed at staging truncation';
    END IF;
    
    -- Шаг 2: Загрузка данных (пример)
    v_success := dwh.execute_etl_step(
        'Daily Sales ETL',
        'Load Transactions',
        'INSERT INTO dwh.fact_transactions SELECT * FROM staging.sales_raw'
    );
    
    IF NOT v_success THEN
        RAISE EXCEPTION 'ETL process failed at data loading';
    END IF;
    
    RAISE NOTICE 'ETL process completed successfully';
END;
$$;

-- ============================================
-- 8. MONITORING QUERIES: Запросы мониторинга ETL
-- ============================================

-- Мониторинг последних загрузок
SELECT 
    table_name,
    last_load_date,
    last_load_timestamp,
    rows_loaded,
    status,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - updated_at)) / 3600 as hours_since_update
FROM dwh.etl_control
ORDER BY updated_at DESC;

-- История выполнения ETL процессов
SELECT 
    process_name,
    step_name,
    status,
    COUNT(*) as execution_count,
    AVG(duration_seconds) as avg_duration_sec,
    MAX(duration_seconds) as max_duration_sec,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as error_count
FROM dwh.etl_log
WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY process_name, step_name, status
ORDER BY process_name, step_name;

-- Поиск проблемных ETL шагов
SELECT 
    process_name,
    step_name,
    error_message,
    start_time,
    duration_seconds
FROM dwh.etl_log
WHERE status = 'ERROR'
  AND start_time >= CURRENT_DATE - INTERVAL '24 hours'
ORDER BY start_time DESC
LIMIT 20;

-- Анализ производительности ETL
SELECT 
    process_name,
    step_name,
    AVG(rows_affected::NUMERIC / NULLIF(duration_seconds, 0)) as rows_per_second,
    AVG(duration_seconds) as avg_duration,
    COUNT(*) as run_count
FROM dwh.etl_log
WHERE status = 'SUCCESS'
  AND start_time >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY process_name, step_name
ORDER BY rows_per_second DESC;

-- ============================================
-- 9. PARTITION MANAGEMENT: Управление партициями
-- ============================================

-- Создание новой партиции для следующего месяца
CREATE OR REPLACE PROCEDURE dwh.add_partition_for_next_month(p_table_name VARCHAR)
LANGUAGE plpgsql AS $$
DECLARE
    v_next_month DATE;
    v_partition_name VARCHAR;
    v_start_date DATE;
    v_end_date DATE;
BEGIN
    v_next_month := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month');
    v_partition_name := p_table_name || '_' || TO_CHAR(v_next_month, 'YYYY_MM');
    v_start_date := v_next_month;
    v_end_date := v_next_month + INTERVAL '1 month';
    
    EXECUTE format('
        ALTER TABLE %I ADD PARTITION %I
        START (%L) INCLUSIVE
        END (%L) EXCLUSIVE
    ', p_table_name, v_partition_name, v_start_date, v_end_date);
    
    RAISE NOTICE 'Partition % created for table %', v_partition_name, p_table_name;
END;
$$;

-- Удаление старых партиций
CREATE OR REPLACE PROCEDURE dwh.drop_old_partitions(
    p_table_name VARCHAR,
    p_retention_months INTEGER
)
LANGUAGE plpgsql AS $$
DECLARE
    v_partition RECORD;
    v_cutoff_date DATE;
BEGIN
    v_cutoff_date := DATE_TRUNC('month', CURRENT_DATE - (p_retention_months || ' months')::INTERVAL);
    
    FOR v_partition IN
        SELECT partitiontablename, partitionrangestart
        FROM pg_partitions
        WHERE tablename = p_table_name
          AND partitionrangestart::DATE < v_cutoff_date
    LOOP
        EXECUTE format('ALTER TABLE %I DROP PARTITION %I', 
                      p_table_name, v_partition.partitiontablename);
        
        RAISE NOTICE 'Dropped partition % from table %', 
                     v_partition.partitiontablename, p_table_name;
    END LOOP;
END;
$$;

-- ============================================
-- 10. ПОЛНЫЙ ETL WORKFLOW: Пример комплексного процесса
-- ============================================

CREATE OR REPLACE PROCEDURE dwh.daily_etl_workflow()
LANGUAGE plpgsql AS $$
DECLARE
    v_start_time TIMESTAMP;
    v_process_name VARCHAR := 'Daily ETL Workflow';
BEGIN
    v_start_time := CLOCK_TIMESTAMP();
    RAISE NOTICE 'Starting % at %', v_process_name, v_start_time;
    
    -- Шаг 1: Очистка staging
    PERFORM dwh.execute_etl_step(v_process_name, 'Truncate Staging', 
        'TRUNCATE TABLE staging.sales_raw, staging.products_new, staging.customers_new');
    
    -- Шаг 2: Загрузка dimensions
    CALL dwh.bulk_load_products();
    
    -- Шаг 3: Инкрементальная загрузка фактов
    PERFORM dwh.incremental_load_transactions();
    
    -- Шаг 4: Удаление дубликатов
    PERFORM dwh.execute_etl_step(v_process_name, 'Remove Duplicates',
        'DELETE FROM dwh.fact_transactions WHERE transaction_key IN (
            SELECT transaction_key FROM (
                SELECT transaction_key, 
                       ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY load_timestamp DESC) as rn
                FROM dwh.fact_transactions
            ) sub WHERE rn > 1
        )');
    
    -- Шаг 5: Обновление агрегатов
    CALL dwh.refresh_daily_aggregates(CURRENT_DATE - 1);
    
    -- Шаг 6: Обновление статистики
    PERFORM dwh.execute_etl_step(v_process_name, 'Analyze Tables',
        'ANALYZE dwh.fact_transactions; 
         ANALYZE dwh.dim_products; 
         ANALYZE dwh.agg_daily_sales;');
    
    -- Шаг 7: Управление партициями
    CALL dwh.add_partition_for_next_month('fact_transactions');
    CALL dwh.drop_old_partitions('fact_transactions', 24);
    
    RAISE NOTICE '% completed in % seconds', 
                 v_process_name, 
                 EXTRACT(EPOCH FROM (CLOCK_TIMESTAMP() - v_start_time))::INTEGER;
END;
$$;

-- Запуск полного ETL workflow
CALL dwh.daily_etl_workflow();

-- ============================================
-- ЗАДАНИЯ ДЛЯ САМОСТОЯТЕЛЬНОЙ РАБОТЫ
-- ============================================

/*
Задание 1: Создайте процедуру для CDC (Change Data Capture)
- Отслеживание изменений в source таблицах
- Загрузка только измененных записей
- Логирование всех изменений

Задание 2: Реализуйте обработку late arriving facts
- Данные приходят с задержкой
- Обновление уже загруженных агрегатов
- Перерасчет зависимых витрин

Задание 3: Создайте систему data lineage
- Отслеживание источников данных
- Цепочка трансформаций
- Документирование потоков данных

Задание 4: Оптимизация ETL производительности
- Параллельная загрузка данных
- Batch processing
- Минимизация locks
*/
