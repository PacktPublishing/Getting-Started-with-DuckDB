# Chapter 03

# Data import and manipulation

```sql
CREATE OR REPLACE TABLE web_log_text (raw_text VARCHAR);

COPY web_log_text FROM './access.log' (DELIM '');

SELECT *  
FROM web_log_text 
LIMIT 10; 

SELECT 
    regexp_extract(raw_text, '^[0-9\.]*' ) AS client_ip 
FROM  web_log_text
LIMIT 3;

SELECT regexp_extract(raw_text, '^[0-9\.]*' ) AS client_ip, 
    regexp_extract(raw_text, '\[(.*)\]',1 ) AS date_text,
    regexp_extract(raw_text, '"([A-Z]*) ',1 ) AS http_method,
    regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) AS lang
FROM  web_log_text
LIMIT 5;


CREATE OR REPLACE TABLE web_log_split AS
SELECT regexp_extract(raw_text, '^[0-9\.]*' ) AS client_ip, 
    regexp_extract(raw_text, '\[(.*)\]',1 ) AS http_date_text,
    regexp_extract(raw_text, '"([A-Z]*) ',1 ) AS http_method,
    regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) AS http_lang
FROM  web_log_text;
```

# Altering tables and creating views

```sql
SELECT *
FROM web_log_split
LIMIT 5;

SELECT client_ip,
    strptime(
        http_date_text,
        '%d/%b/%Y:%H:%M:%S %z'
    ) AS http_date,
    http_method,
    http_lang,
FROM web_log_split;

ALTER TABLE web_log_split ADD COLUMN http_date
    TIMESTAMP WITH TIME ZONE;

UPDATE web_log_split
SET http_date = strptime(
    http_date_text,
    '%d/%b/%Y:%H:%M:%S %z'
);

SELECT client_ip,
    http_date,
    http_method,
    http_lang
FROM web_log_split;

CREATE OR REPLACE TABLE language_iso (
    lang_iso VARCHAR PRIMARY KEY, 
    language_name VARCHAR
);

INSERT INTO language_iso
SELECT *
FROM read_csv('./language_iso.csv');

SELECT wls.http_date, wls.http_lang, lang.language_name
FROM web_log_split AS wls
LEFT OUTER JOIN language_iso AS lang
    ON (wls.http_lang = lang.lang_iso);
 
CREATE OR REPLACE VIEW web_log_view AS
SELECT wls.client_ip,
    strptime(
        wls.http_date_text,
        '%d/%b/%Y:%H:%M:%S %z'
    ) AS http_date,
    wls.http_method,
    wls.http_lang,
    lang.language_name 
FROM web_log_split wls
LEFT OUTER JOIN language_iso lang
    ON (wls.http_lang = lang.lang_iso);

DESCRIBE web_log_view;

SELECT *
FROM web_log_view
LIMIT 5;

DROP VIEW IF EXISTS web_log_view;
DROP TABLE IF EXISTS language_iso;
DROP TABLE IF EXISTS web_log_split;
DROP TABLE IF EXISTS web_log_text;

.read "web_log_script.sql"

SELECT *
FROM web_log_view
LIMIT 5;
```


# Aggregate functions and common table expressions
```sql
.read "web_log_script.sql"

SELECT min(http_date) AS date_earliest,
    max(http_date) AS date_latest,
    count(*) AS web_log_count
FROM web_log_view;


SELECT http_date,
    time_bucket(interval '1 day', http_date) AS day
FROM web_log_view;


WITH web_cte AS (
    SELECT client_ip,
        time_bucket(interval '1 day', http_date) AS day,
        language_name
    FROM web_log_view
)
SELECT day, language_name, count(*) AS count
FROM web_cte
GROUP BY day, language_name
ORDER BY day, count(*) DESC;


WITH web_cte AS (
  SELECT time_bucket(interval '1 day', http_date) AS day,
  language_name
  FROM web_log_view
)
PIVOT web_cte ON language_name USING count(*);
```

# Joining data from multiple tables
```sql
CREATE OR REPLACE TABLE trips AS
SELECT *
FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet');

SELECT tpep_pickup_datetime, 
    trip_distance, 
    fare_amount, 
    tip_amount,  
    PULocationID, 
    DOLocationID
FROM trips 
LIMIT 10;

CREATE OR REPLACE TABLE locations (
  LocationID int  PRIMARY KEY,
  Borough VARCHAR,
  Zone VARCHAR,
  service_zone VARCHAR
);

INSERT INTO locations(LocationID, Borough, Zone, service_zone)
SELECT LocationID, Borough, Zone, service_zone
FROM read_csv('https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv');

SELECT LocationID, Borough, Zone
FROM locations
LIMIT 5;

CREATE OR REPLACE TABLE trips_with_location AS
SELECT t.*,
    l_pu.zone AS pick_up_zone,
    l_do.zone AS drop_off_zone
FROM trips AS t
LEFT JOIN locations AS l_pu
    ON l_pu.LocationID = t.PULocationID
LEFT JOIN locations AS l_do
    ON l_do.LocationID = t.DOLocationID;


SELECT tpep_pickup_datetime, 
    pick_up_zone, 
    drop_off_zone, 
    trip_distance
FROM trips_with_location
LIMIT 5;


SELECT time_bucket(interval '1 day', tpep_pickup_datetime) AS day_of,
    count(*) AS num_trips,
    min(fare_amount) AS fare_min,
    max(fare_amount) AS fare_max,
    avg(fare_amount) AS fare_avg,
    avg(tip_amount) AS tip_avg,
    avg(case when Payment_type =1 then tip_amount/fare_amount end)*100 AS cc_tip_avg_pct
FROM trips_with_location 
WHERE tpep_pickup_datetime BETWEEN '2023-01-20 00:00:00' and '2023-01-29 23:59:59'
GROUP BY 1
ORDER BY 1;


SELECT time_bucket(interval '1 day', tpep_pickup_datetime) AS day_of,
    count(*) AS num_trips,
    min(fare_amount) AS fare_min,
    max(fare_amount) AS fare_max,
    round(avg(fare_amount), 2) AS fare_avg,
    round(avg(tip_amount), 2) AS tip_avg,
    round(avg(case when Payment_type =1 then tip_amount/fare_amount end)*100, 0) AS cc_tip_avg_pct
FROM trips_with_location 
WHERE tpep_pickup_datetime BETWEEN '2023-01-20 00:00:00' AND '2023-01-29 23:59:59' 
AND fare_amount>0 
GROUP BY 1
ORDER BY 1;


WITH cte AS (
    SELECT twl.*,
        max(fare_amount) OVER
            (PARTITION BY time_bucket(INTERVAL '1 day', 
                tpep_pickup_datetime)) AS max_day_fare_amount
    FROM trips_with_location twl
)
SELECT  tpep_pickup_datetime, pick_up_zone, drop_off_zone, fare_amount
FROM cte
WHERE fare_amount = max_day_fare_amount
AND tpep_pickup_datetime 
  BETWEEN '2023-01-20 00:00:00' AND '2023-01-29 23:59:59'
ORDER BY tpep_pickup_datetime;
```

