# Chapter 03

## Data import and manipulation
```sql

CREATE OR REPLACE TABLE web_log_text 
(raw_text VARCHAR);

COPY web_log_text 
FROM './access.log' (DELIMITER '\n');

SELECT *
FROM web_log_text;

SELECT regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip 
FROM  web_log_text;

SELECT regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip, 
regexp_extract(raw_text, '\[(.*)\]',1 ) as date_text,
regexp_extract(raw_text, '"([A-Z]*) ',1 ) as http_method,
regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) as lang
FROM  web_log_text
LIMIT 5;


CREATE OR REPLACE TABLE web_log_split
AS
SELECT regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip, 
  regexp_extract(raw_text, '\[(.*)\]',1 ) as http_date_text,
  regexp_extract(raw_text, '"([A-Z]*) ',1 ) as http_method,
  regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) as http_lang
FROM  web_log_text;
```

## Altering tables and views 

```sql
SELECT *
FROM web_log_split;

DESCRIBE web_log_split;

SELECT client_ip,
strptime(http_date_text, '%d/%b/%Y:%H:%M:%S %z') as http_date,
http_method,
http_lang,
FROM web_log_split;

ALTER TABLE web_log_split
ADD http_date timestamp with time zone;

UPDATE web_log_split
SET http_date = strptime(http_date_text, '%d/%b/%Y:%H:%M:%S %z');

SELECT client_ip,
http_date,
http_method,
http_lang
FROM web_log_split;

CREATE OR REPLACE TABLE language_iso(
  lang_iso VARCHAR PRIMARY KEY, 
  language_name VARCHAR
);

INSERT INTO language_iso
SELECT *
FROM read_csv('./language_iso.csv', AUTO_DETECT=TRUE, header=True);

SELECT wls.http_date,
wls.http_lang,
ln.language_name 
FROM web_log_split wls
JOIN language_iso ln on (wls.http_lang = ln.lang_iso);

SELECT wls.http_date,
wls.http_lang,
ln.language_name 
FROM web_log_split wls
LEFT OUTER JOIN language_iso ln on (wls.http_lang = ln.lang_iso);
 
CREATE OR REPLACE VIEW web_log_view
AS
SELECT wls.client_ip,
strptime(wls.http_date_text, '%d/%b/%Y:%H:%M:%S %z') as http_date,
wls.http_method,
wls.http_lang,
ln.language_name 
FROM web_log_split wls
LEFT OUTER JOIN language_iso ln on (wls.http_lang = ln.lang_iso);

DESCRIBE web_log_view;

SELECT *
FROM web_log_view;

DROP VIEW IF EXISTS web_log_view;
DROP TABLE IF EXISTS language_iso;
DROP TABLE IF EXISTS web_log_split;
DROP TABLE IF EXISTS web_log_text;

.read "web_log_script.sql"

SELECT *
FROM web_log_view;
```





## Aggregate functions and common table expressions
```sql

.read "web_log_script.sql"

SELECT min(http_date) as date_earliest,
max(http_date) as date_latest,
count(*) as cnt
FROM web_log_view;


SELECT http_date,
time_bucket(interval '1 day', http_date) as day
FROM web_log_view;


WITH web_cte as (
  SELECT client_ip,
  time_bucket(interval '1 day', http_date) as day,
  language_name
  FROM web_log_view
)
SELECT day, language_name, count(*) as count
FROM web_cte
GROUP BY day, language_name
ORDER BY day, count(*) DESC;
```

## Join, union and intersect data from multiple tables
```sql

CREATE OR REPLACE TABLE trips
AS
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
FROM read_csv('https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv', AUTO_DETECT=TRUE);

SELECT LocationID, Borough, Zone
FROM locations
LIMIT 5;

CREATE OR REPLACE TABLE trips_with_location
AS
SELECT t.*,
l_pu.zone as pick_up_zone,
l_do.zone as drop_off_zone
FROM trips t
LEFT JOIN locations l_pu on l_pu.LocationID = t.PULocationID 
LEFT JOIN locations l_do on l_do.LocationID = t.DOLocationID ;

SELECT tpep_pickup_datetime, 
pick_up_zone, 
drop_off_zone, 
trip_distance
FROM trips_with_location
LIMIT 5;


SELECT time_bucket(interval '1 day', tpep_pickup_datetime) as day_of,
count(*) as num_trips,
min(fare_amount) as fare_min,
max(fare_amount) as fare_max,
avg(fare_amount) as fare_avg,
avg(tip_amount) as tip_avg,
avg(case when Payment_type =1 then tip_amount/fare_amount end)*100 as cc_tip_avg_pct
FROM trips_with_location 
where tpep_pickup_datetime between '2023-01-20 00:00:00' and '2023-01-29 23:59:59'
GROUP BY 1
ORDER BY 1;

SELECT time_bucket(interval '1 day', tpep_pickup_datetime) as day_of,
count(*) as num_trips,
min(fare_amount) as fare_min,
max(fare_amount) as fare_max,
round(avg(fare_amount), 2) as fare_avg,
round(avg(tip_amount), 2) as tip_avg,
round(avg(case when Payment_type =1 then tip_amount/fare_amount end)*100, 0) as cc_tip_avg_pct
FROM trips_with_location 
where tpep_pickup_datetime between '2023-01-20 00:00:00' and '2023-01-29 23:59:59'
and fare_amount>0
GROUP BY 1
ORDER BY 1;

-- lets pull out the details of the highest daily fares
WITH cte AS (
  SELECT twl.*,
  max(fare_amount) over  (partition by time_bucket(interval '1 day', tpep_pickup_datetime)) as max_day_fare_amount
  FROM trips_with_location twl
)
SELECT  tpep_pickup_datetime, pick_up_zone, drop_off_zone, fare_amount
FROM cte
WHERE fare_amount = max_day_fare_amount
AND tpep_pickup_datetime between '2023-01-20 00:00:00' and '2023-01-29 23:59:59'
ORDER BY tpep_pickup_datetime
;
```

