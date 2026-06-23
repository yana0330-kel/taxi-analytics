CREATE SCHEMA IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.taxi_trips;

CREATE TABLE raw.taxi_trips (
    vendor_id              VARCHAR(10),
    tpep_pickup_datetime   VARCHAR(30),
    tpep_dropoff_datetime  VARCHAR(30),
    passenger_count        VARCHAR(10),
    trip_distance          VARCHAR(20),
    ratecode_id            VARCHAR(10),
    store_and_fwd_flag     VARCHAR(5),
    pulocation_id          VARCHAR(10),
    dolocation_id          VARCHAR(10),
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