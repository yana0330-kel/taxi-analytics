-- Слой: CORE. Загрузка и типизация trips: явный ::CAST по каждому полю и явный
-- парсинг дат вместо неявного приведения типов — при некорректном формате в
-- источнике запрос упадёт здесь, с понятной ошибкой, а не тихо испортит данные
-- ниже по пайплайну. Это единственный путь загрузки trips в DWH.
TRUNCATE TABLE dwh.taxi_trips;

INSERT INTO dwh.taxi_trips
   ( vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge)
SELECT
    vendorid::INTEGER,
    to_timestamp(tpep_pickup_datetime, 'YYYY-MM-DD HH24:MI:SS'),
    to_timestamp(tpep_dropoff_datetime, 'YYYY-MM-DD HH24:MI:SS'),
    passenger_count::INTEGER,
    trip_distance::NUMERIC(10, 2),
    ratecodeid::INTEGER,
    store_and_fwd_flag,
    pulocationid::INTEGER,
    dolocationid::INTEGER,
    payment_type::INTEGER,
    fare_amount::NUMERIC(10, 2),
    extra::NUMERIC(10, 2),
    mta_tax::NUMERIC(10, 2),
    tip_amount::NUMERIC(10, 2),
    tolls_amount::NUMERIC(10, 2),
    improvement_surcharge::NUMERIC(10, 2),
    total_amount::NUMERIC(10, 2),
    congestion_surcharge::NUMERIC(10, 2)
FROM ext.pg_taxi_trips_raw;
