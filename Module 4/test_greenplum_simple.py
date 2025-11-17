"""
Минимальный DAG для быстрой проверки подключения к GreenPlum
Простой и понятный - всего 3 задачи

Использование:
1. Создайте Connection 'greenplum_default' в Airflow
2. Загрузите этот файл в папку dags/
3. Запустите DAG через Airflow UI
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Конфигурация DAG
dag = DAG(
    dag_id='test_greenplum_simple',
    description='Simple connection test',
    schedule_interval=None,  # Только ручной запуск
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'greenplum'],
)

# Задача 1: Проверка базового подключения
test_connection = PostgresOperator(
    task_id='test_connection',
    postgres_conn_id='greenplum_prod',
    sql="""
        SELECT 
            'Connection OK!' as status,
            NOW() as current_time,
            version() as database_version;
    """,
    dag=dag,
)

# Задача 2: Информация о кластере
test_cluster = PostgresOperator(
    task_id='test_cluster_info',
    postgres_conn_id='greenplum_prod',
    sql="""
        SELECT 
            current_database() as database,
            current_user as user,
            COUNT(*) as total_segments
        FROM gp_segment_configuration
        WHERE content >= 0;
    """,
    dag=dag,
)

# Задача 3: Проверка прав
test_permissions = PostgresOperator(
    task_id='test_permissions',
    postgres_conn_id='greenplum_prod',
    sql="""
        -- Создаем временную таблицу
        CREATE TEMP TABLE test_table (id INT, value TEXT);
        
        -- Вставляем данные
        INSERT INTO test_table VALUES (1, 'Test OK');
        
        -- Читаем данные
        SELECT * FROM test_table;
    """,
    dag=dag,
)

# Порядок выполнения
test_connection >> test_cluster >> test_permissions

"""
ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ:

1. Создайте Connection в Airflow Web UI:
   Admin → Connections → + Add
   
   Connection Id:    greenplum_default
   Connection Type:  Postgres
   Host:             c-xxxxx.rw.mdb.yandexcloud.net
   Schema:           postgres
   Login:            admin
   Password:         ваш_пароль
   Port:             6432
   Extra:            {"sslmode": "require"}

2. Сохраните этот файл как test_greenplum_simple.py

3. Скопируйте в папку dags:
   cp test_greenplum_simple.py ~/airflow/dags/

4. В Airflow UI:
   - Найдите DAG 'test_greenplum_simple'
   - Включите DAG
   - Нажмите ▶️ Trigger DAG

5. Проверьте результат:
   - Все 3 задачи должны быть зелеными ✅
   - Откройте логи для просмотра результатов

ОЖИДАЕМЫЙ РЕЗУЛЬТАТ:
✅ test_connection - показывает версию GreenPlum
✅ test_cluster_info - показывает информацию о кластере
✅ test_permissions - подтверждает права на чтение/запись

ЕСЛИ ОШИБКА:
❌ Проверьте параметры Connection
❌ Убедитесь в доступности GreenPlum
❌ Проверьте Security Groups
"""
