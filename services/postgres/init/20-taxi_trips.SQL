\c rawdb;

CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.raw_taxi_trips;

CREATE TABLE raw.raw_taxi_trips (
    vendorid               VARCHAR(10),  -- Слитно
    tpep_pickup_datetime   VARCHAR(30),
    tpep_dropoff_datetime  VARCHAR(30),
    passenger_count        VARCHAR(10),
    trip_distance          VARCHAR(20),
    ratecodeid             VARCHAR(10),  -- Слитно
    store_and_fwd_flag     VARCHAR(5),
    pulocationid           VARCHAR(10),  -- Слитно
    dolocationid           VARCHAR(10),  -- Слитно
    payment_type           VARCHAR(10),
    fare_amount            VARCHAR(20),
    extra                  VARCHAR(20),
    mta_tax                VARCHAR(20),
    tip_amount             VARCHAR(20),
    tolls_amount           VARCHAR(20),
    improvement_surcharge  VARCHAR(20),
    total_amount           VARCHAR(20),
    congestion_surcharge   VARCHAR(20),
    dt                     DATE DEFAULT CURRENT_DATE
);