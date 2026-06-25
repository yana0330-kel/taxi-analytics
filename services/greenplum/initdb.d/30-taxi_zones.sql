CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.taxi_zones (
    locationid INTEGER,
    borough text,
    zone text,
    service_zone text
)
DISTRIBUTED BY (locationid);
