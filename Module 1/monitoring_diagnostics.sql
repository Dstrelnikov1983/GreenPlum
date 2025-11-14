-- ============================================
-- Модуль 1: Скрипты мониторинга и диагностики
-- GreenPlum Cluster Monitoring
-- ============================================

-- ============================================
-- 1. ПРОВЕРКА СОСТОЯНИЯ КЛАСТЕРА
-- ============================================

-- Общая информация о кластере
SELECT version();

-- Конфигурация всех сегментов
SELECT 
    content as segment_id,
    role,
    preferred_role,
    mode,
    status,
    port,
    hostname,
    address,
    CASE 
        WHEN status = 'u' THEN '✓ UP'
        ELSE '✗ DOWN'
    END as health_status
FROM gp_segment_configuration
ORDER BY content, role;

-- Краткая сводка по здоровью кластера
SELECT 
    'Total Segments' as metric,
    count(*)::text as value
FROM gp_segment_configuration
WHERE content >= 0

UNION ALL

SELECT 
    'Primary Segments' as metric,
    count(*)::text as value
FROM gp_segment_configuration
WHERE content >= 0 AND role = 'p'

UNION ALL

SELECT 
    'Mirror Segments' as metric,
    count(*)::text as value
FROM gp_segment_configuration
WHERE content >= 0 AND role = 'm'

UNION ALL

SELECT 
    'Segments UP' as metric,
    count(*)::text as value
FROM gp_segment_configuration
WHERE content >= 0 AND status = 'u'

UNION ALL

SELECT 
    'Segments DOWN' as metric,
    count(*)::text as value
FROM gp_segment_configuration
WHERE content >= 0 AND status = 'd'

UNION ALL

SELECT 
    'Cluster Health' as metric,
    CASE 
        WHEN count(*) = sum(CASE WHEN status = 'u' THEN 1 ELSE 0 END)
        THEN '✓ HEALTHY'
        ELSE '⚠️  UNHEALTHY'
    END as value
FROM gp_segment_configuration
WHERE content >= 0;

-- ============================================
-- 2. МОНИТОРИНГ АКТИВНОСТИ
-- ============================================

-- Текущие активные запросы
SELECT 
    pid,
    usename as username,
    client_addr,
    client_port,
    application_name,
    backend_start,
    query_start,
    state_change,
    state,
    CASE 
        WHEN state = 'active' THEN now() - query_start
        ELSE NULL
    END as query_duration,
    CASE 
        WHEN length(query) > 100 THEN substring(query, 1, 100) || '...'
        ELSE query
    END as query
FROM pg_stat_activity
WHERE state != 'idle'
    AND pid != pg_backend_pid() -- Исключаем текущий запрос
ORDER BY query_start;

-- Количество подключений по базам данных
SELECT 
    datname as database,
    count(*) as connections,
    count(*) FILTER (WHERE state = 'active') as active,
    count(*) FILTER (WHERE state = 'idle') as idle,
    count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction
FROM pg_stat_activity
GROUP BY datname
ORDER BY connections DESC;

-- Количество подключений по пользователям
SELECT 
    usename as username,
    count(*) as total_connections,
    count(*) FILTER (WHERE state = 'active') as active_queries
FROM pg_stat_activity
GROUP BY usename
ORDER BY total_connections DESC;

-- Долгие запросы (выполняются более 1 минуты)
SELECT 
    pid,
    usename,
    now() - query_start as duration,
    state,
    substring(query, 1, 200) as query
FROM pg_stat_activity
WHERE state = 'active'
    AND now() - query_start > interval '1 minute'
ORDER BY duration DESC;

-- Заблокированные запросы
SELECT 
    blocked_locks.pid AS blocked_pid,
    blocked_activity.usename AS blocked_user,
    blocking_locks.pid AS blocking_pid,
    blocking_activity.usename AS blocking_user,
    blocked_activity.query AS blocked_statement,
    blocking_activity.query AS blocking_statement,
    blocked_activity.application_name AS blocked_application
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;

-- ============================================
-- 3. СТАТИСТИКА ПО ТАБЛИЦАМ
-- ============================================

-- Размер всех таблиц
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - 
                   pg_relation_size(schemaname||'.'||tablename)) as index_size,
    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema', 'gp_toolkit')
ORDER BY size_bytes DESC
LIMIT 20;

-- Статистика использования таблиц
SELECT 
    schemaname,
    tablename,
    n_live_tup as live_rows,
    n_dead_tup as dead_rows,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup + n_dead_tup, 0), 2) as dead_pct,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;

-- Таблицы, требующие VACUUM
SELECT 
    schemaname,
    tablename,
    n_dead_tup as dead_rows,
    n_live_tup as live_rows,
    round(100.0 * n_dead_tup / NULLIF(n_live_tup, 0), 2) as dead_pct,
    last_autovacuum,
    CASE 
        WHEN n_dead_tup > 10000 AND 
             (n_dead_tup::float / NULLIF(n_live_tup, 0)) > 0.1
        THEN '⚠️  VACUUM RECOMMENDED'
        ELSE '✓ OK'
    END as vacuum_status
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000
ORDER BY dead_pct DESC;

-- ============================================
-- 4. ИНДЕКСЫ И ПРОИЗВОДИТЕЛЬНОСТЬ
-- ============================================

-- Размер индексов
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) as index_size,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(schemaname||'.'||indexname) DESC
LIMIT 20;

-- Неиспользуемые индексы (кандидаты на удаление)
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) as index_size,
    idx_scan as times_used
FROM pg_stat_user_indexes
WHERE idx_scan = 0
    AND indexname NOT LIKE '%_pkey' -- Исключаем первичные ключи
ORDER BY pg_relation_size(schemaname||'.'||indexname) DESC;

-- ============================================
-- 5. ДИСКОВОЕ ПРОСТРАНСТВО
-- ============================================

-- Размер баз данных
SELECT 
    datname as database,
    pg_size_pretty(pg_database_size(datname)) as size,
    pg_database_size(datname) as size_bytes
FROM pg_database
WHERE datname NOT IN ('template0', 'template1')
ORDER BY size_bytes DESC;

-- Использование дискового пространства по схемам
SELECT 
    schemaname,
    count(*) as table_count,
    pg_size_pretty(sum(pg_total_relation_size(schemaname||'.'||tablename))) as total_size
FROM pg_tables
GROUP BY schemaname
ORDER BY sum(pg_total_relation_size(schemaname||'.'||tablename)) DESC;

-- ============================================
-- 6. ПРОИЗВОДИТЕЛЬНОСТЬ ЗАПРОСОВ
-- ============================================

-- Самые медленные запросы (требует pg_stat_statements)
-- Примечание: модуль pg_stat_statements должен быть включен
/*
SELECT 
    substring(query, 1, 100) as query,
    calls,
    round(total_exec_time::numeric, 2) as total_time_ms,
    round(mean_exec_time::numeric, 2) as avg_time_ms,
    round((100 * total_exec_time / sum(total_exec_time) OVER ())::numeric, 2) as pct_total_time,
    rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;
*/

-- Кэш-хиты (buffer cache efficiency)
SELECT 
    sum(heap_blks_read) as heap_read,
    sum(heap_blks_hit) as heap_hit,
    round(100.0 * sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit) + sum(heap_blks_read), 0), 2) as cache_hit_ratio
FROM pg_statio_user_tables;

-- ============================================
-- 7. СЕГМЕНТЫ И РАСПРЕДЕЛЕНИЕ ДАННЫХ
-- ============================================

-- Распределение данных по сегментам для таблицы
CREATE OR REPLACE FUNCTION analyze_table_distribution(p_table_name text)
RETURNS TABLE (
    segment_id integer,
    row_count bigint,
    percentage numeric,
    status text
) AS $$
DECLARE
    avg_rows numeric;
    total_rows bigint;
BEGIN
    -- Получаем общее количество строк
    EXECUTE format('SELECT count(*) FROM %I', p_table_name) INTO total_rows;
    
    -- Вычисляем среднее
    avg_rows := total_rows / (SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 AND role = 'p');
    
    RETURN QUERY EXECUTE format('
        SELECT 
            gp_segment_id::integer as segment_id,
            count(*)::bigint as row_count,
            round(100.0 * count(*) / %s, 2) as percentage,
            CASE 
                WHEN abs(count(*) - %s) / %s < 0.1 THEN ''✓ Balanced''
                WHEN abs(count(*) - %s) / %s < 0.2 THEN ''⚠️  Minor skew''
                ELSE ''⚠️  Significant skew''
            END as status
        FROM %I
        GROUP BY gp_segment_id
        ORDER BY gp_segment_id
    ', total_rows, avg_rows, avg_rows, avg_rows, avg_rows, p_table_name);
END;
$$ LANGUAGE plpgsql;

-- Использование: SELECT * FROM analyze_table_distribution('your_table_name');

-- Информация о размере таблиц на каждом сегменте
SELECT 
    gp_segment_id,
    count(*) as table_count,
    pg_size_pretty(sum(pg_relation_size(oid))) as total_size
FROM gp_dist_random('pg_class')
WHERE relkind = 'r'
    AND relnamespace NOT IN (
        SELECT oid FROM pg_namespace 
        WHERE nspname IN ('pg_catalog', 'information_schema', 'gp_toolkit')
    )
GROUP BY gp_segment_id
ORDER BY gp_segment_id;

-- ============================================
-- 8. СИСТЕМНЫЕ РЕСУРСЫ
-- ============================================

-- Статистика по транзакциям
SELECT 
    datname as database,
    xact_commit as commits,
    xact_rollback as rollbacks,
    round(100.0 * xact_commit / NULLIF(xact_commit + xact_rollback, 0), 2) as commit_ratio,
    blks_read as blocks_read,
    blks_hit as blocks_hit,
    round(100.0 * blks_hit / NULLIF(blks_hit + blks_read, 0), 2) as cache_hit_ratio,
    tup_returned as tuples_returned,
    tup_fetched as tuples_fetched,
    tup_inserted as tuples_inserted,
    tup_updated as tuples_updated,
    tup_deleted as tuples_deleted
FROM pg_stat_database
WHERE datname NOT IN ('template0', 'template1')
ORDER BY datname;

-- Статистика по background writer
SELECT 
    checkpoints_timed,
    checkpoints_req as checkpoints_requested,
    round(100.0 * checkpoints_timed / NULLIF(checkpoints_timed + checkpoints_req, 0), 2) as checkpoint_write_time_pct,
    buffers_checkpoint,
    buffers_clean,
    buffers_backend,
    buffers_alloc
FROM pg_stat_bgwriter;

-- ============================================
-- 9. РЕПЛИКАЦИЯ И ОТКАЗОУСТОЙЧИВОСТЬ
-- ============================================

-- Статус репликации между Primary и Mirror
SELECT 
    content,
    role,
    preferred_role,
    mode,
    status,
    CASE 
        WHEN role = preferred_role AND status = 'u' THEN '✓ Normal'
        WHEN role != preferred_role AND status = 'u' THEN '⚠️  Role changed'
        ELSE '✗ Problem'
    END as replication_status
FROM gp_segment_configuration
WHERE content >= 0
ORDER BY content, role;

-- Проверка синхронизации
SELECT 
    content,
    count(*) as segment_count,
    string_agg(role || ':' || status, ', ') as roles_status,
    CASE 
        WHEN count(*) = 2 AND 
             count(*) FILTER (WHERE status = 'u') = 2 
        THEN '✓ Synchronized'
        ELSE '⚠️  Check required'
    END as sync_status
FROM gp_segment_configuration
WHERE content >= 0
GROUP BY content
ORDER BY content;

-- ============================================
-- 10. КОМПЛЕКСНЫЙ ОТЧЕТ О ЗДОРОВЬЕ КЛАСТЕРА
-- ============================================

CREATE OR REPLACE VIEW cluster_health_report AS
WITH segment_health AS (
    SELECT 
        count(*) FILTER (WHERE content >= 0) as total_segments,
        count(*) FILTER (WHERE content >= 0 AND status = 'u') as up_segments,
        count(*) FILTER (WHERE content >= 0 AND status = 'd') as down_segments,
        count(*) FILTER (WHERE content >= 0 AND role = 'p') as primary_segments,
        count(*) FILTER (WHERE content >= 0 AND role = 'm') as mirror_segments
    FROM gp_segment_configuration
),
connection_stats AS (
    SELECT 
        count(*) as total_connections,
        count(*) FILTER (WHERE state = 'active') as active_connections,
        count(*) FILTER (WHERE state = 'idle') as idle_connections,
        count(*) FILTER (WHERE state = 'idle in transaction') as idle_in_tx
    FROM pg_stat_activity
),
database_stats AS (
    SELECT 
        sum(pg_database_size(datname)) as total_db_size
    FROM pg_database
    WHERE datname NOT IN ('template0', 'template1')
)
SELECT 
    'Cluster Health' as category,
    json_build_object(
        'total_segments', s.total_segments,
        'up_segments', s.up_segments,
        'down_segments', s.down_segments,
        'health_status', CASE WHEN s.down_segments = 0 THEN 'HEALTHY' ELSE 'UNHEALTHY' END,
        'total_connections', c.total_connections,
        'active_connections', c.active_connections,
        'idle_connections', c.idle_connections,
        'total_database_size', pg_size_pretty(d.total_db_size)
    ) as metrics
FROM segment_health s, connection_stats c, database_stats d;

-- Использование view
SELECT * FROM cluster_health_report;

-- ============================================
-- 11. УТИЛИТЫ ДЛЯ АДМИНИСТРИРОВАНИЯ
-- ============================================

-- Принудительная остановка запроса
-- SELECT pg_cancel_backend(pid); -- Мягкая остановка
-- SELECT pg_terminate_backend(pid); -- Жесткая остановка

-- Убить все idle соединения определенного пользователя
-- SELECT pg_terminate_backend(pid)
-- FROM pg_stat_activity
-- WHERE usename = 'username' AND state = 'idle';

-- Обновление статистики для всех таблиц
-- ANALYZE;

-- Vacuum для всех таблиц
-- VACUUM;

-- Полная очистка и обновление статистики
-- VACUUM ANALYZE;

-- ============================================
-- 12. АВТОМАТИЗАЦИЯ МОНИТОРИНГА
-- ============================================

-- Создание функции для ежедневной проверки здоровья
CREATE OR REPLACE FUNCTION daily_health_check()
RETURNS TABLE (
    check_name text,
    status text,
    details text
) AS $$
BEGIN
    -- Проверка сегментов
    RETURN QUERY
    SELECT 
        'Segment Health'::text,
        CASE 
            WHEN count(*) FILTER (WHERE status = 'd') > 0 THEN 'CRITICAL'
            ELSE 'OK'
        END::text,
        format('%s/%s segments UP', 
               count(*) FILTER (WHERE status = 'u'),
               count(*))::text
    FROM gp_segment_configuration
    WHERE content >= 0;
    
    -- Проверка долгих запросов
    RETURN QUERY
    SELECT 
        'Long Running Queries'::text,
        CASE 
            WHEN count(*) > 0 THEN 'WARNING'
            ELSE 'OK'
        END::text,
        format('%s queries running > 5 minutes', count(*))::text
    FROM pg_stat_activity
    WHERE state = 'active'
        AND now() - query_start > interval '5 minutes';
    
    -- Проверка дискового пространства
    RETURN QUERY
    SELECT 
        'Disk Space'::text,
        CASE 
            WHEN sum(pg_database_size(datname)) > 1099511627776 THEN 'WARNING' -- 1TB
            ELSE 'OK'
        END::text,
        pg_size_pretty(sum(pg_database_size(datname)))::text
    FROM pg_database
    WHERE datname NOT IN ('template0', 'template1');
    
    -- Проверка таблиц, требующих VACUUM
    RETURN QUERY
    SELECT 
        'Tables Need VACUUM'::text,
        CASE 
            WHEN count(*) > 10 THEN 'WARNING'
            WHEN count(*) > 0 THEN 'INFO'
            ELSE 'OK'
        END::text,
        format('%s tables with >10%% dead rows', count(*))::text
    FROM pg_stat_user_tables
    WHERE n_dead_tup > 1000
        AND (n_dead_tup::float / NULLIF(n_live_tup, 0)) > 0.1;
END;
$$ LANGUAGE plpgsql;

-- Запуск проверки
SELECT * FROM daily_health_check();

-- ============================================
-- ЗАВЕРШЕНИЕ
-- ============================================

-- Этот скрипт содержит все необходимые запросы для мониторинга
-- и диагностики кластера GreenPlum в Yandex Cloud.
-- 
-- Рекомендуется регулярно запускать эти проверки:
-- - Ежедневно: daily_health_check()
-- - Еженедельно: анализ неиспользуемых индексов
-- - Ежемесячно: проверка роста баз данных
