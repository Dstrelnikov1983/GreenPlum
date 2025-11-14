# Лабораторная работа №4: ETL-процесс с Apache Airflow и Object Storage

## Цель работы

Создать комплексный ETL-процесс для загрузки данных из Yandex Object Storage в GreenPlum (Yandex MPP Analytics for PostgreSQL) с использованием Managed Service for Apache Airflow™. Настроить сетевую связанность и безопасность сервисов Yandex Cloud.

## Продолжительность

**90 минут** (рекомендуется)

## Предварительные требования

- Аккаунт в Yandex Cloud с активным биллингом
- Установленный Yandex Cloud CLI
- Базовые знания Python и SQL
- Понимание концепций ETL
- Выполненная Лабораторная работа №1 (или существующий кластер GreenPlum)

## Архитектура решения

```
┌─────────────────────────────────────────────────────────────┐
│                    Yandex Cloud                             │
│                                                              │
│  ┌──────────────────┐      ┌───────────────────┐           │
│  │  Object Storage  │      │  Apache Airflow   │           │
│  │  (S3)            │◄─────┤  (Managed)        │           │
│  │                  │      │                   │           │
│  │ - sales_data.csv │      │ - DAG: ETL        │           │
│  │ - products.json  │      │ - Scheduler       │           │
│  │ - logs/          │      │ - Web UI          │           │
│  └──────────────────┘      └────────┬──────────┘           │
│                                      │                       │
│                                      │ Извлечение           │
│                                      │ Трансформация        │
│                                      │ Загрузка             │
│                                      ▼                       │
│                       ┌─────────────────────────┐           │
│                       │  GreenPlum Cluster      │           │
│                       │  (MPP Analytics)        │           │
│                       │                         │           │
│                       │  - Master               │           │
│                       │  - Segments (×2)        │           │
│                       └─────────────────────────┘           │
│                                                              │
│  Все сервисы в одной VPC (mpp-network)                     │
│  Подсеть: 10.1.0.0/24                                       │
└─────────────────────────────────────────────────────────────┘
```

## Часть 1: Подготовка инфраструктуры (20 минут)

### Шаг 1.1: Проверка существующих ресурсов

```bash
# Проверка существующего GreenPlum кластера
yc managed-greenplum cluster list

# Если кластера нет, создайте его (используя инструкции из Лаб.работы №1)
# Или используйте существующий кластер

# Сохраните имя/ID кластера
GP_CLUSTER_NAME="lab-gp-cluster"
```

### Шаг 1.2: Создание Object Storage (S3)

```bash
# Создание бакета для хранения данных
BUCKET_NAME="mpp-etl-data-$(date +%s)"

yc storage bucket create \
  --name $BUCKET_NAME \
  --default-storage-class standard \
  --max-size 10737418240

echo "Bucket created: $BUCKET_NAME"

# Сохраните имя бакета для дальнейшего использования
echo "export BUCKET_NAME=$BUCKET_NAME" >> ~/.bashrc
```

### Шаг 1.3: Создание статического ключа для доступа к S3

```bash
# Создание сервисного аккаунта для доступа к S3
yc iam service-account create \
  --name s3-airflow-sa \
  --description "Service account for Airflow S3 access"

# Получение ID сервисного аккаунта
SA_ID=$(yc iam service-account get s3-airflow-sa --format json | jq -r '.id')

# Назначение роли storage.editor
yc resource-manager folder add-access-binding $(yc config get folder-id) \
  --role storage.editor \
  --subject serviceAccount:$SA_ID

# Создание статического ключа доступа
yc iam access-key create \
  --service-account-name s3-airflow-sa \
  --description "Access key for S3" \
  --format json > s3_credentials.json

# Извлечение credentials
AWS_ACCESS_KEY_ID=$(jq -r '.access_key.key_id' s3_credentials.json)
AWS_SECRET_ACCESS_KEY=$(jq -r '.secret' s3_credentials.json)

echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"

# Сохраните эти значения - они понадобятся для Airflow
```

### Шаг 1.4: Настройка сетевой инфраструктуры

```bash
# Проверка существующей сети (если создавали в Лаб.работе №1)
yc vpc network list

# Если сети нет, создайте новую
yc vpc network create \
  --name mpp-network \
  --description "Network for MPP, Airflow, and S3"

# Создание подсети для Airflow (если её нет)
yc vpc subnet create \
  --name airflow-subnet-a \
  --network-name mpp-network \
  --zone ru-central1-a \
  --range 10.1.1.0/24

# Получение ID подсети
AIRFLOW_SUBNET_ID=$(yc vpc subnet get airflow-subnet-a --format json | jq -r '.id')
echo "Airflow Subnet ID: $AIRFLOW_SUBNET_ID"
```

### Шаг 1.5: Создание Security Group для Airflow

```bash
# Создание security group для Airflow
yc vpc security-group create \
  --name airflow-sg \
  --network-name mpp-network \
  --description "Security group for Airflow cluster"

# Получение ID security group
SG_ID=$(yc vpc security-group get airflow-sg --format json | jq -r '.id')

# Добавление правил
# 1. Разрешить исходящий трафик (для доступа к S3 и GreenPlum)
yc vpc security-group update-rules $SG_ID \
  --add-rule "direction=egress,port=any,protocol=any,v4-cidrs=[0.0.0.0/0]"

# 2. Разрешить входящий трафик на Web UI Airflow (порт 8080)
yc vpc security-group update-rules $SG_ID \
  --add-rule "direction=ingress,port=8080,protocol=tcp,v4-cidrs=[0.0.0.0/0]"

# 3. Разрешить внутрисетевой трафик
yc vpc security-group update-rules $SG_ID \
  --add-rule "direction=ingress,port=any,protocol=any,v4-cidrs=[10.1.0.0/16]"

echo "Security group configured: $SG_ID"
```

**Важно о безопасности:**
- Правило для порта 8080 открывает доступ к Web UI из любого места. В production следует ограничить доступ конкретными IP адресами
- Internal трафик между сервисами защищен через Security Groups
- Все соединения с GreenPlum используют SSL (sslmode=require)

## Часть 2: Создание кластера Apache Airflow (20 минут)

### Шаг 2.1: Создание сервисного аккаунта для Airflow

```bash
# Создание сервисного аккаунта для Airflow
yc iam service-account create \
  --name airflow-sa \
  --description "Service account for Apache Airflow"

# Получение ID сервисного аккаунта
AIRFLOW_SA_ID=$(yc iam service-account get airflow-sa --format json | jq -r '.id')

# Назначение необходимых ролей
yc resource-manager folder add-access-binding $(yc config get folder-id) \
  --role managed-airflow.integrationProvider \
  --subject serviceAccount:$AIRFLOW_SA_ID

yc resource-manager folder add-access-binding $(yc config get folder-id) \
  --role vpc.user \
  --subject serviceAccount:$AIRFLOW_SA_ID

yc resource-manager folder add-access-binding $(yc config get folder-id) \
  --role storage.viewer \
  --subject serviceAccount:$AIRFLOW_SA_ID
```

**Примечание:** Роли обеспечивают:
- `managed-airflow.integrationProvider` - интеграция с другими сервисами
- `vpc.user` - доступ к сетевым ресурсам
- `storage.viewer` - чтение из Object Storage

### Шаг 2.2: Создание кластера Airflow

```bash
# Создание кластера Apache Airflow
yc airflow cluster create \
  --name etl-airflow-cluster \
  --service-account-id $AIRFLOW_SA_ID \
  --subnet-id $AIRFLOW_SUBNET_ID \
  --security-group-ids $SG_ID \
  --dags-bucket $BUCKET_NAME \
  --webserver-resource-preset s2.micro \
  --scheduler-resource-preset s2.micro \
  --worker-resource-preset s2.micro \
  --min-worker-count 1 \
  --max-worker-count 3 \
  --triggerer-resource-preset s2.micro \
  --triggerer-count 1 \
  --admin-password "StrongPassword123!" \
  --version 2.2.4

echo "Airflow cluster creation started. This will take 10-15 minutes..."
echo "You can check the status with: yc airflow cluster get etl-airflow-cluster"
```

**Ожидание:** Создание кластера займет около 10-15 минут. Проверяйте статус:

```bash
# Проверка статуса создания
watch -n 30 "yc airflow cluster get etl-airflow-cluster --format json | jq -r '.status'"

# Когда статус станет "RUNNING", получите URL Web UI
yc airflow cluster get etl-airflow-cluster --format json | jq -r '.webserver.url'
```

### Шаг 2.3: Настройка подключения к GreenPlum в Airflow

После создания кластера, настройте Connection в Airflow Web UI:

1. Откройте Web UI Airflow (URL из предыдущего шага)
2. Войдите с credentials:
   - Username: `admin`
   - Password: `StrongPassword123!`
3. Перейдите в **Admin → Connections**
4. Нажмите **+ Add a new connection**
5. Заполните параметры:
   - **Connection Id**: `greenplum_default`
   - **Connection Type**: `Postgres`
   - **Host**: `<FQDN вашего GreenPlum Master>` (получите через `yc managed-greenplum hosts list lab-gp-cluster`)
   - **Schema**: `postgres`
   - **Login**: `admin`
   - **Password**: `<ваш пароль от GreenPlum>`
   - **Port**: `6432`
   - **Extra**: `{"sslmode": "require"}`
6. Нажмите **Test** для проверки соединения
7. Нажмите **Save**

**Получение FQDN GreenPlum Master:**
```bash
yc managed-greenplum hosts list $GP_CLUSTER_NAME \
  --format json | jq -r '.[] | select(.type == "MASTER") | .name'
```

### Шаг 2.4: Настройка подключения к S3 в Airflow

Аналогично создайте Connection для S3:

1. В Airflow Web UI: **Admin → Connections → + Add**
2. Параметры:
   - **Connection Id**: `s3_default`
   - **Connection Type**: `Amazon S3`
   - **Extra**: 
   ```json
   {
     "aws_access_key_id": "<ваш AWS_ACCESS_KEY_ID>",
     "aws_secret_access_key": "<ваш AWS_SECRET_ACCESS_KEY>",
     "endpoint_url": "https://storage.yandexcloud.net",
     "region_name": "ru-central1"
   }
   ```
3. Нажмите **Test** и **Save**

## Часть 3: Подготовка тестовых данных (10 минут)

### Шаг 3.1: Создание тестовых файлов данных

Создадим локальные файлы с данными для загрузки:

```bash
# Создание директории для данных
mkdir -p ~/airflow_lab_data
cd ~/airflow_lab_data

# Генерация файла sales_data.csv
cat > sales_data.csv << 'EOF'
order_id,customer_id,product_id,quantity,price,order_date,region
1001,501,2001,2,199.99,2025-01-15,North
1002,502,2003,1,89.99,2025-01-15,South
1003,503,2002,5,49.99,2025-01-16,East
1004,501,2001,1,199.99,2025-01-16,North
1005,504,2004,3,129.99,2025-01-17,West
1006,505,2002,2,49.99,2025-01-17,East
1007,502,2003,4,89.99,2025-01-18,South
1008,506,2005,1,299.99,2025-01-18,North
1009,507,2001,2,199.99,2025-01-19,West
1010,503,2004,1,129.99,2025-01-19,East
EOF

# Генерация файла products.json
cat > products.json << 'EOF'
[
  {"product_id": 2001, "name": "Laptop Pro", "category": "Electronics", "stock": 50},
  {"product_id": 2002, "name": "Wireless Mouse", "category": "Accessories", "stock": 200},
  {"product_id": 2003, "name": "USB-C Cable", "category": "Accessories", "stock": 150},
  {"product_id": 2004, "name": "Mechanical Keyboard", "category": "Peripherals", "stock": 80},
  {"product_id": 2005, "name": "4K Monitor", "category": "Electronics", "stock": 30}
]
EOF

# Генерация файла customers.csv
cat > customers.csv << 'EOF'
customer_id,name,email,registration_date,country
501,John Doe,john.doe@email.com,2024-01-10,USA
502,Jane Smith,jane.smith@email.com,2024-02-15,Canada
503,Bob Johnson,bob.j@email.com,2024-03-20,UK
504,Alice Williams,alice.w@email.com,2024-04-12,Germany
505,Charlie Brown,charlie.b@email.com,2024-05-08,France
506,Diana Prince,diana.p@email.com,2024-06-22,USA
507,Eve Adams,eve.a@email.com,2024-07-30,Canada
EOF

echo "Test data files created in ~/airflow_lab_data/"
```

### Шаг 3.2: Загрузка файлов в Object Storage

```bash
# Установка AWS CLI (если не установлен)
pip3 install awscli --user

# Конфигурация AWS CLI для работы с Yandex Object Storage
aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
aws configure set region ru-central1

# Загрузка файлов в бакет
aws s3 cp sales_data.csv s3://$BUCKET_NAME/input/sales/ \
  --endpoint-url=https://storage.yandexcloud.net

aws s3 cp products.json s3://$BUCKET_NAME/input/products/ \
  --endpoint-url=https://storage.yandexcloud.net

aws s3 cp customers.csv s3://$BUCKET_NAME/input/customers/ \
  --endpoint-url=https://storage.yandexcloud.net

# Проверка загруженных файлов
aws s3 ls s3://$BUCKET_NAME/input/ --recursive \
  --endpoint-url=https://storage.yandexcloud.net

echo "Files uploaded to S3 bucket: $BUCKET_NAME"
```

## Часть 4: Подготовка схемы данных в GreenPlum (10 минут)

### Шаг 4.1: Подключение к GreenPlum

```bash
# Получение FQDN Master хоста
GP_MASTER_FQDN=$(yc managed-greenplum hosts list $GP_CLUSTER_NAME \
  --format json | jq -r '.[] | select(.type == "MASTER") | .name')

echo "GreenPlum Master FQDN: $GP_MASTER_FQDN"

# Подключение через psql
psql "host=$GP_MASTER_FQDN port=6432 dbname=postgres user=admin sslmode=require"
```

### Шаг 4.2: Создание схемы и таблиц

Выполните следующие SQL команды в psql:

```sql
-- Создание схемы для ETL данных
CREATE SCHEMA IF NOT EXISTS etl_data;

-- Таблица для продаж
CREATE TABLE etl_data.sales (
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price NUMERIC(10,2),
    order_date DATE,
    region VARCHAR(50),
    load_timestamp TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (order_date)
(
    START (DATE '2025-01-01') INCLUSIVE
    END (DATE '2025-12-31') EXCLUSIVE
    EVERY (INTERVAL '1 month')
);

-- Таблица для продуктов
CREATE TABLE etl_data.products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    category VARCHAR(100),
    stock INTEGER,
    load_timestamp TIMESTAMP DEFAULT now()
) DISTRIBUTED REPLICATED;

-- Таблица для клиентов
CREATE TABLE etl_data.customers (
    customer_id INTEGER PRIMARY KEY,
    name VARCHAR(200),
    email VARCHAR(200),
    registration_date DATE,
    country VARCHAR(100),
    load_timestamp TIMESTAMP DEFAULT now()
) DISTRIBUTED BY (customer_id);

-- Staging таблица для временного хранения
CREATE TABLE etl_data.sales_staging (
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    price NUMERIC(10,2),
    order_date DATE,
    region VARCHAR(50)
) DISTRIBUTED RANDOMLY;

-- Проверка созданных таблиц
\dt etl_data.*

-- Выход
\q
```

**Примечание о дизайне таблиц:**
- `sales` - партиционирована по датам для эффективных запросов временных рядов
- `products` - REPLICATED, т.к. это справочник (маленькая таблица)
- `customers` - DISTRIBUTED BY customer_id для оптимизации JOIN с sales
- `sales_staging` - RANDOM для быстрой вставки данных

## Часть 5: Создание DAG для ETL-процесса (20 минут)

### Шаг 5.1: Создание Python DAG файла

Создайте файл с DAG локально. Этот DAG будет содержать полный ETL pipeline:

```bash
mkdir -p ~/airflow_dags
cd ~/airflow_dags
```

Создайте файл `etl_s3_to_greenplum.py` со следующим содержимым:

```python
"""
ETL DAG: Загрузка данных из S3 в GreenPlum
Архитектура: Extract → Transform → Load
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

# Параметры по умолчанию для всех задач
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'etl_s3_to_greenplum',
    default_args=default_args,
    description='ETL process: S3 → GreenPlum',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'greenplum', 's3'],
)

# Задача 1: Очистка staging таблицы
truncate_staging = PostgresOperator(
    task_id='truncate_staging_table',
    postgres_conn_id='greenplum_default',
    sql="""
        TRUNCATE TABLE etl_data.sales_staging;
    """,
    dag=dag,
)

# Задача 2: Загрузка данных из S3 в staging
def load_sales_from_s3(**context):
    """
    Загрузка CSV файла из S3 в staging таблицу
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import csv
    from io import StringIO
    
    # Подключение к S3
    s3_hook = S3Hook(aws_conn_id='s3_default')
    
    # Получение имени бакета
    bucket_name = context['params']['bucket_name']
    s3_key = 'input/sales/sales_data.csv'
    
    # Чтение файла из S3
    file_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
    
    # Парсинг CSV
    csv_reader = csv.DictReader(StringIO(file_content))
    
    # Подключение к GreenPlum
    pg_hook = PostgresHook(postgres_conn_id='greenplum_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Вставка данных
    insert_count = 0
    for row in csv_reader:
        cursor.execute("""
            INSERT INTO etl_data.sales_staging 
            (order_id, customer_id, product_id, quantity, price, order_date, region)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row['order_id']),
            int(row['customer_id']),
            int(row['product_id']),
            int(row['quantity']),
            float(row['price']),
            row['order_date'],
            row['region']
        ))
        insert_count += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Loaded {insert_count} rows into staging table")
    return insert_count

load_sales_staging = PythonOperator(
    task_id='load_sales_from_s3',
    python_callable=load_sales_from_s3,
    params={'bucket_name': '{{ var.value.bucket_name }}'},
    dag=dag,
)

# Задача 3: Загрузка продуктов из JSON
def load_products_from_s3(**context):
    """
    Загрузка JSON файла с продуктами из S3
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import json
    
    s3_hook = S3Hook(aws_conn_id='s3_default')
    bucket_name = context['params']['bucket_name']
    s3_key = 'input/products/products.json'
    
    # Чтение JSON из S3
    file_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
    products = json.loads(file_content)
    
    # Подключение к GreenPlum
    pg_hook = PostgresHook(postgres_conn_id='greenplum_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Upsert продуктов
    for product in products:
        cursor.execute("""
            INSERT INTO etl_data.products (product_id, name, category, stock)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                stock = EXCLUDED.stock,
                load_timestamp = now()
        """, (
            product['product_id'],
            product['name'],
            product['category'],
            product['stock']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Loaded {len(products)} products")
    return len(products)

load_products = PythonOperator(
    task_id='load_products_from_s3',
    python_callable=load_products_from_s3,
    params={'bucket_name': '{{ var.value.bucket_name }}'},
    dag=dag,
)

# Задача 4: Загрузка клиентов
def load_customers_from_s3(**context):
    """
    Загрузка CSV файла с клиентами из S3
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import csv
    from io import StringIO
    
    s3_hook = S3Hook(aws_conn_id='s3_default')
    bucket_name = context['params']['bucket_name']
    s3_key = 'input/customers/customers.csv'
    
    file_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
    csv_reader = csv.DictReader(StringIO(file_content))
    
    pg_hook = PostgresHook(postgres_conn_id='greenplum_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    insert_count = 0
    for row in csv_reader:
        cursor.execute("""
            INSERT INTO etl_data.customers 
            (customer_id, name, email, registration_date, country)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                name = EXCLUDED.name,
                email = EXCLUDED.email,
                country = EXCLUDED.country,
                load_timestamp = now()
        """, (
            int(row['customer_id']),
            row['name'],
            row['email'],
            row['registration_date'],
            row['country']
        ))
        insert_count += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Loaded {insert_count} customers")
    return insert_count

load_customers = PythonOperator(
    task_id='load_customers_from_s3',
    python_callable=load_customers_from_s3,
    params={'bucket_name': '{{ var.value.bucket_name }}'},
    dag=dag,
)

# Задача 5: Валидация данных
validate_staging_data = PostgresOperator(
    task_id='validate_staging_data',
    postgres_conn_id='greenplum_default',
    sql="""
        DO $$
        DECLARE
            null_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO null_count
            FROM etl_data.sales_staging
            WHERE order_id IS NULL 
               OR customer_id IS NULL 
               OR product_id IS NULL;
            
            IF null_count > 0 THEN
                RAISE EXCEPTION 'Found % rows with NULL values', null_count;
            END IF;
            
            RAISE NOTICE 'Data validation passed';
        END $$;
    """,
    dag=dag,
)

# Задача 6: Загрузка в production
load_to_production = PostgresOperator(
    task_id='load_staging_to_production',
    postgres_conn_id='greenplum_default',
    sql="""
        INSERT INTO etl_data.sales 
        (order_id, customer_id, product_id, quantity, price, order_date, region)
        SELECT 
            order_id, customer_id, product_id, quantity, 
            price, order_date, region
        FROM etl_data.sales_staging
        ON CONFLICT DO NOTHING;
    """,
    dag=dag,
)

# Задача 7: Обновление статистики
update_statistics = PostgresOperator(
    task_id='update_table_statistics',
    postgres_conn_id='greenplum_default',
    sql="""
        ANALYZE etl_data.sales;
        ANALYZE etl_data.products;
        ANALYZE etl_data.customers;
    """,
    dag=dag,
)

# Задача 8: Генерация отчета
def generate_load_report(**context):
    """
    Генерация отчета о результатах ETL
    """
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='greenplum_default')
    
    stats_query = """
        SELECT 
            'Sales' as table_name,
            COUNT(*) as total_rows,
            COUNT(*) FILTER (WHERE load_timestamp::date = CURRENT_DATE) as loaded_today
        FROM etl_data.sales
        UNION ALL
        SELECT 
            'Products' as table_name,
            COUNT(*) as total_rows,
            COUNT(*) FILTER (WHERE load_timestamp::date = CURRENT_DATE) as loaded_today
        FROM etl_data.products
        UNION ALL
        SELECT 
            'Customers' as table_name,
            COUNT(*) as total_rows,
            COUNT(*) FILTER (WHERE load_timestamp::date = CURRENT_DATE) as loaded_today
        FROM etl_data.customers;
    """
    
    results = pg_hook.get_records(stats_query)
    
    print("=" * 60)
    print("ETL LOAD REPORT")
    print("=" * 60)
    for row in results:
        print(f"{row[0]:<15} | Total: {row[1]:>6} | Loaded Today: {row[2]:>6}")
    print("=" * 60)
    
    return results

generate_report = PythonOperator(
    task_id='generate_load_report',
    python_callable=generate_load_report,
    dag=dag,
)

# Определение последовательности выполнения задач
truncate_staging >> load_sales_staging >> validate_staging_data >> load_to_production
[load_products, load_customers] >> load_to_production
load_to_production >> update_statistics >> generate_report
```

Сохраните этот код в файл.

### Шаг 5.2: Загрузка DAG в Object Storage

```bash
# Загрузка DAG файла в бакет (в папку dags)
aws s3 cp etl_s3_to_greenplum.py s3://$BUCKET_NAME/dags/ \
  --endpoint-url=https://storage.yandexcloud.net

# Проверка
aws s3 ls s3://$BUCKET_NAME/dags/ \
  --endpoint-url=https://storage.yandexcloud.net

echo "DAG uploaded to S3. Airflow will automatically sync it in ~1 minute."
```

### Шаг 5.3: Настройка переменной в Airflow

1. Откройте Airflow Web UI
2. Перейдите в **Admin → Variables**
3. Нажмите **+ Add a new record**
4. Заполните:
   - **Key**: `bucket_name`
   - **Val**: `<ваш BUCKET_NAME>`
5. Нажмите **Save**

## Часть 6: Запуск и мониторинг ETL (10 минут)

### Шаг 6.1: Активация и запуск DAG

1. В Airflow Web UI перейдите в **DAGs**
2. Найдите DAG `etl_s3_to_greenplum` (подождите ~1 минуту если не видите)
3. Включите DAG (переключатель слева)
4. Нажмите кнопку **Trigger DAG** (▶️)
5. Подтвердите запуск

### Шаг 6.2: Мониторинг выполнения

1. Кликните на название DAG для открытия детального вида
2. Выберите запущенный run
3. Просмотрите **Graph View** для визуализации прогресса
4. Кликайте на задачи для просмотра логов

**Ожидаемое поведение:**
- Все задачи должны выполниться успешно (зеленый цвет)
- Общее время выполнения: ~2-5 минут

### Шаг 6.3: Проверка результатов в GreenPlum

```bash
# Подключение к GreenPlum
psql "host=$GP_MASTER_FQDN port=6432 dbname=postgres user=admin sslmode=require"
```

Выполните проверочные запросы:

```sql
-- Проверка загруженных продаж
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(quantity * price) as total_revenue
FROM etl_data.sales
GROUP BY order_date
ORDER BY order_date;

-- Проверка продуктов
SELECT * FROM etl_data.products ORDER BY product_id;

-- Проверка клиентов
SELECT * FROM etl_data.customers ORDER BY customer_id;

-- Общая статистика
SELECT 
    'Sales' as table_name, COUNT(*) as row_count FROM etl_data.sales
UNION ALL
SELECT 'Products', COUNT(*) FROM etl_data.products
UNION ALL
SELECT 'Customers', COUNT(*) FROM etl_data.customers;

-- Аналитический запрос с JOIN
SELECT 
    c.name as customer_name,
    c.country,
    COUNT(s.order_id) as total_orders,
    SUM(s.quantity * s.price) as total_spent
FROM etl_data.sales s
JOIN etl_data.customers c ON s.customer_id = c.customer_id
GROUP BY c.name, c.country
ORDER BY total_spent DESC;

-- Проверка партиционирования
SELECT 
    tablename,
    partitiontablename,
    partitionrangestart,
    partitionrangeend
FROM pg_partitions
WHERE tablename = 'sales'
ORDER BY partitionrangestart;
```

**Ожидаемые результаты:**
- В таблице `sales` должно быть 10 записей
- В таблице `products` - 5 записей
- В таблице `customers` - 7 записей
- Данные корректно распределены по партициям

## Часть 7: Дополнительные задания (опционально)

### Задание 7.1: Добавление инкрементальной загрузки

Модифицируйте DAG для загрузки только новых данных:

```python
# Идея: Используйте XCom для хранения последней обработанной даты
# Фильтруйте данные в S3 по дате модификации файла
# Загружайте только изменившиеся файлы
```

### Задание 7.2: Настройка алертинга

Добавьте email уведомления при ошибках:

```python
default_args = {
    ...
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
```

### Задание 7.3: Добавление проверок качества данных

```sql
-- Создайте дополнительную задачу для проверки:
-- 1. Дубликатов
-- 2. Некорректных значений (negative prices, future dates)
-- 3. Referential integrity (все customer_id существуют в customers)
```

## Контрольные вопросы

1. **Архитектура:**
   - Объясните flow данных через систему
   - Зачем нужна staging таблица?
   - Почему продукты загружаются в REPLICATED таблицу?

2. **Безопасность:**
   - Какие меры безопасности применены?
   - Как защищен доступ к S3?
   - Для чего нужны Security Groups?

3. **Производительность:**
   - Как можно ускорить загрузку больших файлов?
   - Зачем партиционирование таблицы sales?
   - Почему разные стратегии DISTRIBUTED для таблиц?

4. **Airflow:**
   - Объясните назначение каждой задачи в DAG
   - Что произойдет при падении задачи?
   - Как настроить расписание выполнения?

## Очистка ресурсов

```bash
# ВНИМАНИЕ: Удаляет все созданные ресурсы!

# Удаление Airflow кластера
yc airflow cluster delete etl-airflow-cluster

# Очистка S3
aws s3 rm s3://$BUCKET_NAME --recursive \
  --endpoint-url=https://storage.yandexcloud.net

# Удаление бакета
yc storage bucket delete --name $BUCKET_NAME

# Удаление сервисных аккаунтов
yc iam service-account delete s3-airflow-sa
yc iam service-account delete airflow-sa

# Удаление security group
yc vpc security-group delete airflow-sg

# НЕ удаляйте GreenPlum если планируете следующие лаборатор ные!
```

## Результаты работы

После выполнения лабораторной работы вы должны:

✅ Понимать архитектуру ETL с Airflow  
✅ Уметь создавать Managed Airflow в Yandex Cloud  
✅ Настраивать сетевую связанность между сервисами  
✅ Работать с Object Storage (S3)  
✅ Создавать DAG для автоматизации ETL  
✅ Загружать данные из разных форматов (CSV, JSON)  
✅ Применять staging подход для валидации  
✅ Мониторить выполнение ETL задач  

## Полезные ссылки

- [Yandex Managed Airflow](https://cloud.yandex.ru/docs/managed-airflow/)
- [Yandex Object Storage](https://cloud.yandex.ru/docs/storage/)
- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [GreenPlum Best Practices](https://docs.vmware.com/en/VMware-Greenplum/)

---

**Поздравляем с завершением лабораторной работы №4!** 🎉

Вы освоили создание комплексного ETL-процесса с использованием современного стека технологий Yandex Cloud!
