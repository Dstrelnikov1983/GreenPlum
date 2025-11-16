-- 1. Создаем staging таблицу для сырого JSON
CREATE TABLE staging_json (
    id SERIAL,
    raw_json JSONB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DISTRIBUTED BY (id);

-- 2. Создаем External Table для S3
CREATE EXTERNAL TABLE ext_s3_json (
    json_line TEXT
)
LOCATION ('s3://s3.amazonaws.com/my-bucket/events/*.json config=/etc/s3config.yaml')
FORMAT 'TEXT' (DELIMITER OFF);

-- 3. Загружаем в staging с парсингом
INSERT INTO staging_json (raw_json)
SELECT json_line::JSONB
FROM ext_s3_json
WHERE json_line IS NOT NULL 
  AND json_line != '';

-- 4. Парсим и загружаем в целевую таблицу
INSERT INTO events (
    event_id,
    event_type,
    user_id,
    timestamp,
    properties
)
SELECT 
    (raw_json->>'event_id')::BIGINT,
    raw_json->>'event_type',
    (raw_json->>'user_id')::INT,
    (raw_json->>'timestamp')::TIMESTAMP,
    raw_json->'properties' -- сохраняем как JSONB
FROM staging_json
WHERE raw_json->>'event_id' IS NOT NULL;

-- 5. Проверяем результат
SELECT event_type, COUNT(*) 
FROM events 
GROUP BY event_type;