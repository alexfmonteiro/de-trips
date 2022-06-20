-- script to upsert trips table with new data from the last csv file
-- upsert approach was chosen to keep the pipeline idempotent

BEGIN;

CREATE TEMP TABLE trips_temp (LIKE trips);

INSERT INTO trips_temp (region, orig_coord, dest_coord, datetime, time_of_day, datasource, input_file_name)
SELECT t.region,
       ST_MakePoint(t.lon_orig, t.lat_orig) AS orig_coord,
       ST_MakePoint(t.lon_dest, t.lat_dest) AS dest_coord,
       t.datetime,
       date_trunc('hour', t.datetime) as time_of_day,
       t.datasource,
       t.input_file_name
  FROM trips_staging t
 WHERE t.input_file_name = :last_input_file;

DELETE FROM trips WHERE input_file_name = :last_input_file;
INSERT INTO trips SELECT * FROM trips_temp;
  DROP TABLE trips_temp;

COMMIT;
