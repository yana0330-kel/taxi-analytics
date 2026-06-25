-- Создаем текстовую таблицу зон в нашей изолированной схеме
\c rawdb;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.raw_taxi_zones;

CREATE TABLE raw.raw_taxi_zones (
    locationid     INT,
    borough        VARCHAR(255),
    zone           VARCHAR(255),
    service_zone   VARCHAR(255),
    dt             DATE DEFAULT CURRENT_DATE -- Добавили маркер сюда для единообразия слоев
);

