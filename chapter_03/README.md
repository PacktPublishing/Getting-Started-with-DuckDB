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

WITH web_cte as (
  SELECT time_bucket(interval '1 day', http_date) as day,
  language_name
  FROM web_log_view
)
PIVOT web_cte ON language_name USING count(*);
```

# How to download the dataset

The dataset for this project is hosted by Kaggle. To download the necessary dataset for this project, please follow the instructions below.

1. Go to https://www.kaggle.com/datasets/shuhengmo/uber-nyc-forhire-vehicles-trip-data-2021
2. Click on the 'Download' button
3. Kaggle will prompt you to sign in or to register. If you do not have a Kaggle account, you can register for one.
4. Upon signing in, the download will start automatically.
5. After the download is complete, unzip the `archive.zip` zip file into a new `archive` directory

```bash
cd chapter_03
unzip -d archive archive.zip 
``` 

## Join data from multiple tables
```sql
INSTALL httpfs; 
LOAD httpfs; 

CREATE OR REPLACE TABLE trips
AS
SELECT *
FROM read_parquet('./archive/fhvhv_tripdata_2021-01.parquet');

summarize trips;

SELECT pickup_datetime, 
trip_miles, 
base_passenger_fare, 
tips,  
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
FROM read_csv('./archive/taxi_zone_lookup.csv', AUTO_DETECT=TRUE);

SELECT LocationID, Borough, Zone
FROM locations
LIMIT 5;

CREATE OR REPLACE TABLE trips_with_location
AS
SELECT t.*,
l_pu.zone as pick_up_zone,
l_pu.borough as pick_up_borough,
l_do.zone as drop_off_zone,
l_do.borough as drop_off_borough
FROM trips t
LEFT JOIN locations l_pu on l_pu.LocationID = t.PULocationID 
LEFT JOIN locations l_do on l_do.LocationID = t.DOLocationID ;

SELECT pickup_datetime, 
pick_up_zone, 
drop_off_zone, 
trip_miles
FROM trips_with_location
LIMIT 5;


SELECT time_bucket(interval '1 day', pickup_datetime) as day_of,
count(*) as num_trips,
min(base_passenger_fare) as fare_min,
max(base_passenger_fare) as fare_max,
avg(base_passenger_fare) as fare_avg,
avg(tips) as tip_avg,
avg(tips/base_passenger_fare)*100 as cc_tip_avg_pct
FROM trips_with_location 
WHERE pickup_datetime between '2021-01-20 00:00:00' and '2021-01-29 23:59:59'
GROUP BY 1
ORDER BY 1;

SELECT time_bucket(interval '1 day', pickup_datetime) as day_of,
count(*) as num_trips,
min(base_passenger_fare) as fare_min,
max(base_passenger_fare) as fare_max,
round(avg(base_passenger_fare), 2) as fare_avg,
round(avg(tips), 2) as tip_avg,
round(avg(tips/base_passenger_fare)*100, 0) as cc_tip_avg_pct
FROM trips_with_location 
WHERE pickup_datetime between '2021-01-20 00:00:00' and '2021-01-29 23:59:59'
and base_passenger_fare>0
GROUP BY 1
ORDER BY 1;

WITH cte AS (
  SELECT twl.*,
  max(base_passenger_fare) over  (partition by time_bucket(interval '1 day', pickup_datetime)) as max_day_base_passenger_fare
  FROM trips_with_location twl
)
SELECT  pickup_datetime, pick_up_zone, drop_off_zone, base_passenger_fare
FROM cte
WHERE base_passenger_fare = max_day_base_passenger_fare
AND pickup_datetime between '2021-01-20 00:00:00' and '2021-01-29 23:59:59'
ORDER BY pickup_datetime
;
```

