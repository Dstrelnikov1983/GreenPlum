# Лабораторная работа №1: Создание и настройка кластера GreenPlum в Yandex Cloud

## Цель работы

Развернуть кластер GreenPlum в Yandex MPP Analytics for PostgreSQL, подключиться к нему, изучить архитектуру и выполнить базовые операции для проверки работоспособности MPP системы.

## Продолжительность

**45 минут** (рекомендуется)

## Предварительные требования

- Аккаунт в Yandex Cloud (триальный период подходит)
- Установленный Yandex Cloud CLI
- Установленный PostgreSQL клиент (psql)
- Базовые знания SQL и командной строки
- Доступ к интернету

## Архитектура создаваемого кластера

```
┌─────────────────────────────────────────────────┐
│              Yandex Cloud Network               │
│                                                 │
│  ┌──────────────┐         ┌──────────────┐    │
│  │ Master Host  │◄────────┤Standby Master│    │
│  │              │         │              │    │
│  └──────┬───────┘         └──────────────┘    │
│         │                                       │
│    ┌────┴────┬────────────┬─────────────┐     │
│    │         │            │             │     │
│ ┌──▼───┐ ┌──▼───┐    ┌───▼──┐     ┌───▼──┐  │
│ │Seg 1 │ │Seg 1 │    │Seg 2 │     │Seg 2 │  │
│ │Primary│ │Mirror│    │Primary│     │Mirror│  │
│ └──────┘ └──────┘    └──────┘     └──────┘  │
│                                                 │
└─────────────────────────────────────────────────┘
```

## Часть 1: Подготовка окружения (10 минут)

### Шаг 1.1: Проверка установки Yandex Cloud CLI

Убедитесь, что CLI установлен и настроен:

```bash
# Проверка версии CLI
yc --version

# Если CLI не установлен, выполните:
curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash

# Перезапустите терминал или выполните:
exec -l $SHELL

# Инициализация CLI (если еще не сделано)
yc init
```

**Ожидаемый результат:** Версия CLI должна быть 0.100.0 или выше.

### Шаг 1.2: Создание каталога для проекта

```bash
# Создание нового каталога (folder) в Yandex Cloud
yc resource-manager folder create --name mpp-lab-folder

# Получение ID созданного каталога
FOLDER_ID=$(yc resource-manager folder get mpp-lab-folder --format json | jq -r '.id')
echo "Folder ID: $FOLDER_ID"

# Установка текущего каталога
yc config set folder-id $FOLDER_ID
```

**Проверка:** 
```bash
yc config list
```

Вы должны увидеть `folder-id` с вашим новым каталогом.

### Шаг 1.3: Создание сетевой инфраструктуры

```bash
# Создание VPC сети
yc vpc network create \
  --name mpp-network \
  --description "Network for MPP Analytics cluster"

# Создание подсети в зоне ru-central1-a
yc vpc subnet create \
  --name mpp-subnet-a \
  --network-name mpp-network \
  --zone ru-central1-a \
  --range 10.1.0.0/24

# Получение ID подсети для дальнейшего использования
SUBNET_ID=$(yc vpc subnet get mpp-subnet-a --format json | jq -r '.id')
echo "Subnet ID: $SUBNET_ID"
```

**Проверка:**
```bash
yc vpc network list
yc vpc subnet list
```

Вы должны увидеть созданную сеть `mpp-network` и подсеть `mpp-subnet-a`.

### Шаг 1.4: Создание security group (опционально, но рекомендуется)

```bash
# Создание security group с правилами доступа
yc vpc security-group create \
  --name mpp-sg \
  --network-name mpp-network \
  --rule "direction=ingress,port=6432,protocol=tcp,v4-cidrs=[0.0.0.0/0]" \
  --rule "direction=egress,protocol=any,v4-cidrs=[0.0.0.0/0]"
```

**Примечание:** В production окружении следует ограничить доступ только с определенных IP адресов.

## Часть 2: Создание кластера GreenPlum (15 минут)

### Шаг 2.1: Генерация надежного пароля

```bash
# Генерация случайного пароля (или придумайте свой)
ADMIN_PASSWORD=$(openssl rand -base64 16)
echo "Admin password: $ADMIN_PASSWORD"

# ВАЖНО: Сохраните этот пароль в безопасном месте!
```

### Шаг 2.2: Создание кластера через CLI

```bash
# Создание кластера с минимальной конфигурацией
yc managed-greenplum cluster create \
  --name lab-gp-cluster \
  --description "Laboratory GreenPlum cluster for Module 1" \
  --environment production \
  --network-name mpp-network \
  --zone-id ru-central1-a \
  --subnet-id $SUBNET_ID \
  --assign-public-ip \
  --master-config resource-id=s2.medium,disk-size=50,disk-type=network-ssd \
  --segment-config resource-id=s2.medium,disk-size=50,disk-type=network-ssd \
  --segment-host-count 2 \
  --segment-in-host 1 \
  --user-name admin \
  --user-password "$ADMIN_PASSWORD" \
  --greenplum-version "6.25"
```

**Параметры команды:**

| Параметр | Значение | Описание |
|----------|----------|----------|
| `--name` | lab-gp-cluster | Имя кластера |
| `--master-config` | s2.medium, 50GB SSD | Конфигурация Master: 4 vCPU, 16GB RAM |
| `--segment-config` | s2.medium, 50GB SSD | Конфигурация Segment: 4 vCPU, 16GB RAM |
| `--segment-host-count` | 2 | Количество физических хостов для сегментов |
| `--segment-in-host` | 1 | Количество primary сегментов на хосте |
| `--assign-public-ip` | true | Публичный IP для подключения извне |

**Примечание:** Создание кластера занимает 15-20 минут.

### Шаг 2.3: Мониторинг создания кластера

```bash
# Проверка статуса кластера
yc managed-greenplum cluster get lab-gp-cluster

# Или в цикле каждые 30 секунд
watch -n 30 'yc managed-greenplum cluster get lab-gp-cluster --format json | jq -r ".status"'
```

Дождитесь статуса **RUNNING**.

### Шаг 2.4: Получение информации о хостах

```bash
# Список всех хостов кластера
yc managed-greenplum cluster list-hosts lab-gp-cluster

# Сохранение FQDN master хоста
MASTER_FQDN=$(yc managed-greenplum cluster list-hosts lab-gp-cluster \
  --format json | jq -r '.[] | select(.type=="MASTER") | .name')
echo "Master FQDN: $MASTER_FQDN"
```

**Ожидаемый вывод:**
```
+------+--------+---------+---------+----------------+
| NAME | TYPE   | ROLE    | HEALTH  | ZONE           |
+------+--------+---------+---------+----------------+
| ...  | MASTER | PRIMARY | ALIVE   | ru-central1-a  |
| ...  | MASTER | REPLICA | ALIVE   | ru-central1-a  |
| ...  | SEGMENT| PRIMARY | ALIVE   | ru-central1-a  |
| ...  | SEGMENT| REPLICA | ALIVE   | ru-central1-a  |
+------+--------+---------+---------+----------------+
```

## Часть 3: Подключение к кластеру (10 минут)

### Шаг 3.1: Установка SSL сертификата

```bash
# Создание директории для сертификатов
mkdir -p ~/.postgresql

# Загрузка сертификата Yandex Cloud
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
  -O ~/.postgresql/root.crt

# Проверка установки
ls -la ~/.postgresql/root.crt
```

### Шаг 3.2: Первое подключение через psql

```bash
# Подключение к базе данных postgres (по умолчанию)
psql "host=$MASTER_FQDN \
      port=6432 \
      sslmode=verify-full \
      dbname=postgres \
      user=admin"

# Введите пароль, который вы сохранили ранее
```

**Альтернативный способ (с паролем в URL):**
```bash
psql "postgresql://admin:$ADMIN_PASSWORD@$MASTER_FQDN:6432/postgres?sslmode=verify-full"
```

### Шаг 3.3: Настройка .pgpass для автоматической аутентификации

```bash
# Создание файла .pgpass
echo "$MASTER_FQDN:6432:*:admin:$ADMIN_PASSWORD" >> ~/.pgpass

# Установка правильных прав доступа
chmod 600 ~/.pgpass

# Теперь можно подключаться без ввода пароля
psql "host=$MASTER_FQDN port=6432 dbname=postgres user=admin sslmode=verify-full"
```

### Шаг 3.4: Проверка подключения

Выполните следующие команды после подключения:

```sql
-- Проверка версии GreenPlum
SELECT version();

-- Ожидаемый результат:
-- PostgreSQL 9.4.26 (Greenplum Database 6.25.0 build ...)

-- Информация о текущем пользователе
SELECT current_user, current_database();

-- Проверка количества подключений
SELECT count(*) FROM pg_stat_activity;
```

## Часть 4: Изучение архитектуры кластера (10 минут)

### Шаг 4.1: Анализ конфигурации сегментов

```sql
-- Просмотр всех сегментов
SELECT 
    content,
    role,
    preferred_role,
    mode,
    status,
    port,
    hostname,
    address
FROM gp_segment_configuration
ORDER BY content, role;
```

**Анализ результата:**
- `content = -1`: Master сегмент
- `content >= 0`: Data сегменты
- `role = 'p'`: Primary (активный) сегмент
- `role = 'm'`: Mirror (резервный) сегмент
- `status = 'u'`: Up (работает)

**Вопрос для размышления:** Сколько у вас primary сегментов? Сколько mirror?

### Шаг 4.2: Проверка распределения данных

```sql
-- Подсчет количества primary сегментов
SELECT 
    count(*) as total_segments,
    sum(CASE WHEN role = 'p' THEN 1 ELSE 0 END) as primary_segments,
    sum(CASE WHEN role = 'm' THEN 1 ELSE 0 END) as mirror_segments
FROM gp_segment_configuration
WHERE content >= 0;
```

### Шаг 4.3: Информация о хостах

```sql
-- Информация о сегментах на каждом хосте
SELECT 
    hostname,
    count(*) as segments_count,
    string_agg(content::text, ', ') as segment_ids
FROM gp_segment_configuration
WHERE content >= 0 AND role = 'p'
GROUP BY hostname
ORDER BY hostname;
```

## Часть 5: Работа с данными (10 минут)

### Шаг 5.1: Создание тестовой базы данных

```sql
-- Создание новой базы данных для тестов
CREATE DATABASE lab1_test;

-- Подключение к новой базе данных
\c lab1_test

-- Проверка подключения
SELECT current_database();
```

### Шаг 5.2: Создание таблицы с hash distribution

```sql
-- Создание таблицы продаж
CREATE TABLE sales (
    sale_id SERIAL,
    product_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    sale_date DATE NOT NULL,
    amount NUMERIC(10,2) NOT NULL,
    region VARCHAR(50)
) DISTRIBUTED BY (product_id);

-- Просмотр информации о таблице
\d+ sales
```

**Обратите внимание** на строку `Distributed by: (product_id)`.

### Шаг 5.3: Загрузка тестовых данных

```sql
-- Вставка тестовых данных (1000 записей)
INSERT INTO sales (product_id, customer_id, sale_date, amount, region)
SELECT 
    (random() * 100)::integer + 1 as product_id,
    (random() * 1000)::integer + 1 as customer_id,
    current_date - (random() * 365)::integer as sale_date,
    (random() * 1000)::numeric(10,2) as amount,
    CASE (random() * 4)::integer
        WHEN 0 THEN 'North'
        WHEN 1 THEN 'South'
        WHEN 2 THEN 'East'
        ELSE 'West'
    END as region
FROM generate_series(1, 1000);

-- Проверка количества записей
SELECT count(*) FROM sales;
```

### Шаг 5.4: Анализ распределения данных по сегментам

```sql
-- Подсчет записей на каждом сегменте
SELECT 
    gp_segment_id,
    count(*) as row_count
FROM sales
GROUP BY gp_segment_id
ORDER BY gp_segment_id;
```

**Анализ результата:**
- Данные должны быть равномерно распределены между сегментами
- Разница между сегментами должна быть небольшой (в идеале < 10%)

### Шаг 5.5: Проверка parallel execution

```sql
-- Включение отображения плана выполнения
EXPLAIN ANALYZE
SELECT 
    region,
    count(*) as sales_count,
    sum(amount) as total_amount,
    avg(amount) as avg_amount
FROM sales
GROUP BY region
ORDER BY total_amount DESC;
```

**Обратите внимание** на:
- `Gather Motion` - сбор данных с сегментов
- `Slice statistics` - выполнение на каждом сегменте
- Execution time на разных сегментах

### Шаг 5.6: Создание таблицы с replicated distribution

```sql
-- Создание справочника продуктов (маленькая таблица)
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10,2)
) DISTRIBUTED REPLICATED;

-- Вставка данных
INSERT INTO products (product_id, product_name, category, price)
SELECT 
    generate_series as product_id,
    'Product ' || generate_series as product_name,
    CASE (random() * 3)::integer
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Clothing'
        ELSE 'Food'
    END as category,
    (random() * 500 + 10)::numeric(10,2) as price
FROM generate_series(1, 100);

-- Проверка распределения
SELECT gp_segment_id, count(*) 
FROM products 
GROUP BY gp_segment_id;
```

**Вопрос:** Что вы заметили в распределении replicated таблицы?

### Шаг 5.7: JOIN с оптимизацией

```sql
-- JOIN двух таблиц
EXPLAIN ANALYZE
SELECT 
    p.category,
    count(*) as sales_count,
    sum(s.amount) as total_revenue
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;
```

**Анализ плана:**
- Replicated таблица не требует redistribution
- JOIN выполняется локально на каждом сегменте
- Это быстрее, чем redistribute обеих таблиц

## Часть 6: Мониторинг и диагностика (5 минут)

### Шаг 6.1: Просмотр активных запросов

```sql
-- Текущие активные запросы
SELECT 
    pid,
    usename,
    client_addr,
    query_start,
    state,
    substring(query, 1, 100) as query
FROM pg_stat_activity
WHERE state != 'idle'
ORDER BY query_start;
```

### Шаг 6.2: Статистика по таблицам

```sql
-- Статистика использования таблиц
SELECT 
    schemaname,
    tablename,
    n_live_tup as row_count,
    n_tup_ins as inserts,
    n_tup_upd as updates,
    n_tup_del as deletes
FROM pg_stat_user_tables
ORDER BY n_live_tup DESC;
```

### Шаг 6.3: Размер таблиц

```sql
-- Размер таблиц в базе данных
SELECT 
    schemaname || '.' || tablename as table_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY size_bytes DESC;
```

### Шаг 6.4: Проверка здоровья кластера

```sql
-- Проверка статуса всех сегментов
SELECT 
    CASE 
        WHEN count(*) = sum(CASE WHEN status = 'u' THEN 1 ELSE 0 END)
        THEN 'Кластер здоров - все сегменты работают'
        ELSE 'ВНИМАНИЕ: Есть проблемы с сегментами!'
    END as cluster_health,
    count(*) as total_segments,
    sum(CASE WHEN status = 'u' THEN 1 ELSE 0 END) as up_segments,
    sum(CASE WHEN status = 'd' THEN 1 ELSE 0 END) as down_segments
FROM gp_segment_configuration
WHERE content >= 0;
```

## Часть 7: Очистка ресурсов (опционально)

### Если вы хотите удалить кластер после лабораторной:

```bash
# ВНИМАНИЕ: Это удалит все данные!
yc managed-greenplum cluster delete lab-gp-cluster

# Удаление сетевых ресурсов
yc vpc subnet delete mpp-subnet-a
yc vpc network delete mpp-network

# Удаление каталога
yc resource-manager folder delete mpp-lab-folder
```

**Важно:** Если вы планируете выполнять следующие лабораторные работы, НЕ удаляйте кластер!

## Контрольные вопросы

Ответьте на следующие вопросы для самопроверки:

1. **Архитектура:**
   - Сколько primary сегментов в вашем кластере?
   - Зачем нужен Standby Master?
   - Что происходит, если один сегмент выходит из строя?

2. **Распределение данных:**
   - В чем разница между DISTRIBUTED BY и DISTRIBUTED REPLICATED?
   - Когда использовать каждую стратегию?
   - Как проверить равномерность распределения данных?

3. **Производительность:**
   - Почему JOIN с replicated таблицей быстрее?
   - Как GreenPlum выполняет запросы параллельно?
   - Что показывает `gp_segment_id`?

4. **Yandex Cloud:**
   - В чем преимущества managed сервиса?
   - Как масштабировать кластер?
   - Где хранятся резервные копии?

## Дополнительные задания (повышенной сложности)

Если вы закончили основную часть раньше:

### Задание 1: Эксперимент с data skew

```sql
-- Создайте таблицу с плохим distribution key
CREATE TABLE bad_distribution (
    id SERIAL,
    constant_value INTEGER DEFAULT 1,
    data TEXT
) DISTRIBUTED BY (constant_value);

-- Вставьте данные
INSERT INTO bad_distribution (data)
SELECT md5(random()::text)
FROM generate_series(1, 10000);

-- Проверьте распределение
SELECT gp_segment_id, count(*) 
FROM bad_distribution 
GROUP BY gp_segment_id;
```

**Вопрос:** Что произошло? Почему это плохо?

### Задание 2: Сравнение производительности

```sql
-- Создайте три идентичные таблицы с разными стратегиями
CREATE TABLE test_hash AS 
SELECT * FROM sales DISTRIBUTED BY (product_id);

CREATE TABLE test_random AS 
SELECT * FROM sales DISTRIBUTED RANDOMLY;

CREATE TABLE test_replicated AS 
SELECT * FROM sales DISTRIBUTED REPLICATED;

-- Сравните производительность JOIN
EXPLAIN ANALYZE 
SELECT count(*) FROM test_hash t1 JOIN test_hash t2 USING (product_id);

-- Повторите для других таблиц
```

### Задание 3: Monitoring через командную строку

```bash
# Создайте скрипт для мониторинга кластера
cat > monitor_cluster.sh << 'EOF'
#!/bin/bash
while true; do
    clear
    echo "=== GreenPlum Cluster Monitor ==="
    echo "Time: $(date)"
    echo ""
    
    psql -h $MASTER_FQDN -p 6432 -U admin -d postgres -c "
    SELECT 
        'Active queries: ' || count(*) 
    FROM pg_stat_activity 
    WHERE state = 'active';"
    
    sleep 5
done
EOF

chmod +x monitor_cluster.sh
./monitor_cluster.sh
```

## Результаты работы

После выполнения лабораторной работы вы должны:

✅ Уметь создавать кластер GreenPlum в Yandex Cloud  
✅ Понимать архитектуру Master-Segment  
✅ Подключаться к кластеру через psql  
✅ Создавать таблицы с разными стратегиями распределения  
✅ Анализировать распределение данных по сегментам  
✅ Использовать EXPLAIN для анализа планов выполнения  
✅ Мониторить состояние кластера  

## Полезные ссылки

- [Документация Yandex MPP Analytics](https://cloud.yandex.ru/docs/managed-greenplum/)
- [GreenPlum Best Practices](https://docs.vmware.com/en/VMware-Greenplum/index.html)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/9.4/)

## Поддержка

При возникновении проблем:
1. Проверьте статус кластера через веб-консоль Yandex Cloud
2. Посмотрите логи: `yc managed-greenplum cluster list-logs lab-gp-cluster`
3. Обратитесь к инструктору

---

**Поздравляем с завершением лабораторной работы №1!** 🎉

Теперь вы готовы к следующему модулю, где мы более детально изучим работу с данными в GreenPlum.
