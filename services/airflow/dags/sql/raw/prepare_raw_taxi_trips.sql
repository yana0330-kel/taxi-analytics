-- Слой: RAW (карантин). Точка входа сырых данных о поездках такси в конвейер.
-- Все поля намеренно VARCHAR: на этом слое данные не валидируются, задача —
-- гарантированно принять файл целиком, даже если в нём встретится "грязная" строка.
-- Файл выполняется перед каждой загрузкой нового CSV (создаёт таблицу при первом
-- запуске, затем просто очищает её), поэтому загрузка идемпотентна: сколько раз
-- ни запусти DAG — дублей в raw не будет.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.raw_taxi_trips (
    vendorid               VARCHAR(10),
    tpep_pickup_datetime   VARCHAR(30),
    tpep_dropoff_datetime  VARCHAR(30),
    passenger_count        VARCHAR(10),
    trip_distance          VARCHAR(20),
    ratecodeid             VARCHAR(10),
    store_and_fwd_flag     VARCHAR(5),
    pulocationid           VARCHAR(10),
    dolocationid           VARCHAR(10),
    payment_type           VARCHAR(10),
    fare_amount            VARCHAR(20),
    extra                  VARCHAR(20),
    mta_tax                VARCHAR(20),
    tip_amount             VARCHAR(20),
    tolls_amount           VARCHAR(20),
    improvement_surcharge  VARCHAR(20),
    total_amount           VARCHAR(20),
    congestion_surcharge   VARCHAR(20),
    dt                     DATE DEFAULT CURRENT_DATE   -- дата фактической загрузки (аудит)
);

-- Идемпотентность: перед новой заливкой месяца — чистим полностью.
TRUNCATE TABLE raw.raw_taxi_trips RESTART IDENTITY CASCADE;
