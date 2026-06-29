CREATE DATABASE IF NOT EXISTS dm_ch;

DROP TABLE IF EXISTS dm_ch.obt_taxi_marts;

CREATE TABLE IF NOT EXISTS dm_ch.obt_taxi_marts
(   vendorid               UInt8,
    date_only              Date,
    tpep_pickup_datetime   DateTime,
    tpep_dropoff_datetime  DateTime,
    passenger_count        UInt8,
    trip_distance          Float32,
    ratecodeid             UInt8,
    store_and_fwd_flag     String,
    pulocationid           UInt8,
    dolocationid           UInt8,
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
PRIMARY KEY (date_only, vendorid)
ORDER BY (date_only, vendorid, hour_only);