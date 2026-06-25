CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.taxi_trips
(   vendorid              INTEGER,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        INTEGER,
    trip_distance          NUMERIC(10, 2),
    ratecodeid            INTEGER,
    store_and_fwd_flag     text,
    pulocationid          INTEGER,
    dolocationid          INTEGER,
    payment_type           INTEGER,
    fare_amount            NUMERIC(10, 2),
    extra                  NUMERIC(10, 2),
    mta_tax                NUMERIC(10, 2),
    tip_amount             NUMERIC(10, 2),
    tolls_amount           NUMERIC(10, 2),
    improvement_surcharge  NUMERIC(10, 2),
    total_amount           NUMERIC(10, 2),
    congestion_surcharge   NUMERIC(10, 2)
)
DISTRIBUTED BY (vendorid);