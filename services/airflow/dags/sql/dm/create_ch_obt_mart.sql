-- Слой: DATA MART. Writable PXF-мост Greenplum -> ClickHouse.
DROP EXTERNAL TABLE IF EXISTS dm.ext_ch_obt_taxi_marts CASCADE;

CREATE WRITABLE EXTERNAL TABLE dm.ext_ch_obt_taxi_marts
(
    vendorid               INTEGER,
    date_only              DATE,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        INTEGER,
    trip_distance          NUMERIC(10, 2),
    ratecodeid             INTEGER,
    store_and_fwd_flag     TEXT,
    pulocationid           INTEGER,
    dolocationid           INTEGER,
    payment_type           INTEGER,
    fare_amount            NUMERIC(10, 2),
    extra                  NUMERIC(10, 2),
    mta_tax                NUMERIC(10, 2),
    tip_amount             NUMERIC(10, 2),
    tolls_amount           NUMERIC(10, 2),
    improvement_surcharge  NUMERIC(10, 2),
    total_amount           NUMERIC(10, 2),
    congestion_surcharge   NUMERIC(10, 2),
    pickup_borough         TEXT,
    pickup_zone            TEXT,
    pickup_service_zone    TEXT,
    dropoff_borough        TEXT,
    dropoff_zone           TEXT,
    dropoff_service_zone   TEXT,
    hour_only              INTEGER,
    trip_inmin             NUMERIC(10, 2)
)
LOCATION ('pxf://dm_ch.obt_taxi_marts?PROFILE=JDBC&SERVER=clickhouse')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');

-- NB: типы этого моста должны соответствовать физической ClickHouse-таблице
-- (create_physical_ch_table.sql: Float32/UInt16/String и т.д.) — рассинхрон типов
-- между writable external table и целевой таблицей ClickHouse через PXF частая
-- причина тихих ошибок/потери точности при вставке.
