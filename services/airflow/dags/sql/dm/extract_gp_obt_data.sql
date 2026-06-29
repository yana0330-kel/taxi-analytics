INSERT INTO dm.ext_ch_obt_taxi_marts
SELECT 
    t.vendorid::INTEGER,
    t.tpep_pickup_datetime::DATE AS date_only, 
    t.tpep_pickup_datetime::TIMESTAMP,
    t.tpep_dropoff_datetime::TIMESTAMP,
    t.passenger_count::INTEGER,
    t.trip_distance::NUMERIC(10,2),
    t.ratecodeid::INTEGER,
    t.store_and_fwd_flag,
    t.pulocationid::INTEGER,
    t.dolocationid::INTEGER,
    t.payment_type::INTEGER,
    t.fare_amount::NUMERIC(10,2),
    t.extra::NUMERIC(10,2),
    t.mta_tax::NUMERIC(10,2),
    t.tip_amount::NUMERIC(10,2),
    t.tolls_amount::NUMERIC(10,2),
    t.improvement_surcharge::NUMERIC(10,2),
    t.total_amount::NUMERIC(10,2),
    t.congestion_surcharge::NUMERIC(10,2),
    COALESCE(z_pu.borough, 'Unknown') AS pickup_borough,
    COALESCE(z_pu.zone, 'Unknown') AS pickup_zone,
    COALESCE(z_pu.service_zone, 'Unknown') AS pickup_service_zone,
    COALESCE(z_do.borough, 'Unknown') AS dropoff_borough,
    COALESCE(z_do.zone, 'Unknown') AS dropoff_zone,
    COALESCE(z_do.service_zone, 'Unknown') AS dropoff_service_zone,
    EXTRACT(HOUR FROM t.tpep_pickup_datetime)::INTEGER AS hour_only,
    (EXTRACT(EPOCH FROM (t.tpep_dropoff_datetime - t.tpep_pickup_datetime)) / 60)::NUMERIC(10,2) AS trip_inmin
FROM dwh.taxi_trips t
LEFT JOIN dwh.taxi_zones z_pu ON t.pulocationid = z_pu.locationid
LEFT JOIN dwh.taxi_zones z_do ON t.dolocationid = z_do.locationid
WHERE t.tpep_pickup_datetime >= '2019-01-01' AND t.tpep_pickup_datetime < '2019-02-01'
  AND t.total_amount > 0 AND t.trip_distance > 0 
  AND (EXTRACT(EPOCH FROM (t.tpep_dropoff_datetime - t.tpep_pickup_datetime)) / 60) > 0.16
  AND (EXTRACT(EPOCH FROM (t.tpep_dropoff_datetime - t.tpep_pickup_datetime)) / 60) < 300;