-- Создаем текстовую таблицу зон в нашей изолированной схеме
DROP TABLE IF EXISTS raw.raw_taxi_zones;

CREATE TABLE IF NOT EXISTS raw.raw_taxi_zones (
    location_id   VARCHAR(50),
    borough       VARCHAR(100),
    zone          VARCHAR(200),
    service_zone  VARCHAR(100)
);
