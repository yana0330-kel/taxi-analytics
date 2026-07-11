-- Слой: CORE / DWH (Greenplum). Пересоздаёт целевые таблицы и PXF-мосты к raw
-- перед каждой загрузкой периода — full-reload паттерн, подходящий для батчей
-- в пределах одного месяца. Для многолетней истории данных этот шаг стоит
-- заменить на партиционирование по месяцу и инкрементальный INSERT.

CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS ext;

DROP TABLE IF EXISTS dwh.taxi_zones CASCADE;

CREATE TABLE IF NOT EXISTS dwh.taxi_zones (
    locationid   INTEGER,
    borough      TEXT,
    zone         TEXT,
    service_zone TEXT
)
DISTRIBUTED BY (locationid);

DROP TABLE IF EXISTS dwh.taxi_trips CASCADE;

CREATE TABLE IF NOT EXISTS dwh.taxi_trips
(   vendorid               INTEGER,
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
    congestion_surcharge   NUMERIC(10, 2)
)
-- DISTRIBUTED BY (vendorid) обеспечивает равномерное распределение строк по
-- сегментам Greenplum. taxi_zones — маленький справочник (265 строк), поэтому
-- джойн с ним оптимизатор обычно решает через broadcast, а не redistribute.
-- Для больших справочников имело бы смысл распределять trips по ключу джойна
-- (pulocationid/dolocationid) ради co-located join.
DISTRIBUTED BY (vendorid);

DROP EXTERNAL TABLE IF EXISTS ext.pg_taxi_trips_raw;

CREATE READABLE EXTERNAL TABLE ext.pg_taxi_trips_raw
(   vendorid               VARCHAR(10),
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
    congestion_surcharge              VARCHAR(20)
)
LOCATION ('pxf://raw.raw_taxi_trips?PROFILE=JDBC&SERVER=raw_pg')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

DROP EXTERNAL TABLE IF EXISTS ext.pg_taxi_zones_raw;

CREATE READABLE EXTERNAL TABLE ext.pg_taxi_zones_raw
(   locationid    VARCHAR(50),
    borough       VARCHAR(100),
    zone          VARCHAR(200),
    service_zone  VARCHAR(100)
)
LOCATION ('pxf://raw.raw_taxi_zones?PROFILE=JDBC&SERVER=raw_pg')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');
