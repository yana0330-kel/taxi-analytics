-- Слой: DATA MART (ClickHouse). Витрина One Big Table для BI (Superset).
-- Типы подобраны с запасом относительно фактического диапазона значений:
-- pulocationid/dolocationid — UInt16 (id зон NYC доходят до 265, что не влезло
-- бы в UInt8 и привело бы к переполнению) ratecodeid — UInt16 (код 99 =
-- Negotiated fare). PARTITION BY toYYYYMM(date_only) даёт возможность быстро
-- заменять/удалять данные за конкретный месяц без пересчёта всей таблицы.

CREATE DATABASE IF NOT EXISTS dm_ch;

DROP TABLE IF EXISTS dm_ch.obt_taxi_marts;

CREATE TABLE IF NOT EXISTS dm_ch.obt_taxi_marts
(   vendorid               UInt8,
    date_only              Date,
    tpep_pickup_datetime   DateTime,
    tpep_dropoff_datetime  DateTime,
    passenger_count        UInt8,
    trip_distance          Float32,
    ratecodeid             UInt16,
    store_and_fwd_flag     String,
    pulocationid           UInt16,
    dolocationid           UInt16,
    payment_type           UInt8,
    fare_amount            Float32,
    extra                  Float32,
    mta_tax                Float32,
    tip_amount             Float32,
    tolls_amount           Float32,
    improvement_surcharge  Float32,
    total_amount           Float32,
    congestion_surcharge   Float32,
    pickup_borough         String,
    pickup_zone            String,
    pickup_service_zone    String,
    dropoff_borough        String,
    dropoff_zone           String,
    dropoff_service_zone   String,
    hour_only              UInt8,
    trip_inmin             Float32
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(date_only)
PRIMARY KEY (date_only, vendorid)
ORDER BY (date_only, vendorid, hour_only);
