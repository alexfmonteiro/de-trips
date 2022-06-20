-- created trips table with postgis geometry support for coordinates columns
-- spatial indexes created for better performance according to:
-- - http://postgis.net/workshops/postgis-intro/indexing.html

CREATE TABLE IF NOT EXISTS trips
(
    region VARCHAR(100) NOT NULL,
    orig_coord geography(Point, 4326) NOT NULL,
    dest_coord geography(Point, 4326) NOT NULL,
    datetime TIMESTAMP NOT NULL,
    time_of_day TIMESTAMP NOT NULL,
    datasource VARCHAR(100) NOT NULL,
    input_file_name VARCHAR(100) NOT NULL
) PARTITION BY RANGE (time_of_day);

CREATE TABLE trips_2018 PARTITION OF trips
    FOR VALUES FROM ('2018-01-01') TO ('2019-01-01');

CREATE INDEX IF NOT EXISTS trips_origin_coord_idx
  ON trips USING GIST (orig_coord);

CREATE INDEX IF NOT EXISTS trips_destin_coord_idx
  ON trips USING GIST (dest_coord);

COMMIT;
