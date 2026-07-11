-- Слой: RAW. Справочник зон NYC TLC (265 строк, статичный, но грузим тем же паттерном).

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.raw_taxi_zones (
    locationid     INT,
    borough        VARCHAR(255),
    zone           VARCHAR(255),
    service_zone   VARCHAR(255),
    dt             DATE DEFAULT CURRENT_DATE
);

TRUNCATE TABLE raw.raw_taxi_zones RESTART IDENTITY CASCADE;
