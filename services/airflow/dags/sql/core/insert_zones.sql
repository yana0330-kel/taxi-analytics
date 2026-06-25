TRUNCATE TABLE dwh.taxi_zones;

INSERT INTO dwh.taxi_zones (locationid, borough, zone, service_zone)
SELECT 
    locationid::INTEGER, 
    borough, 
    zone, 
    service_zone  
FROM ext.pg_taxi_zones_raw;