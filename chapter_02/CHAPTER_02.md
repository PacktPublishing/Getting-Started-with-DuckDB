# Chapter 02

# Load data into DuckDB 


```sql
drop table if exists foods;

create table foods (
  food_name varchar primary key, 
  color varchar,
  calories int, 
  is_healthy boolean);

COPY foods FROM 'foods_no_heading.csv';

select * 
from foods;
```

## COPY file options during load

```sql
COPY foods (food_name, is_healthy, color, calories)  
FROM 'foods_with_heading.txt' (DELIMITER '\t', HEADER);
```


## Error handling with the COPY Statement

```sql
COPY foods FROM 'foods_error.csv';

COPY foods FROM 'foods_error.csv' (ignore_errors true);
```


## File loading with read_csv

```sql
SELECT * 
FROM read_csv('food_prices.csv', AUTO_DETECT=TRUE);

DESCRIBE SELECT * 
FROM read_csv('food_prices.csv', AUTO_DETECT=TRUE);
```

## Table creation with read_csv

```sql
CREATE TABLE low_cost_foods 
as 
SELECT * 
FROM read_csv('food_prices.csv', AUTO_DETECT=TRUE) 
WHERE price < 4.00;

SELECT * 
FROM low_cost_foods;
```

## Date formats 

```sql
SELECT * 
FROM read_csv('food_orders.csv', AUTO_DETECT=TRUE);

SELECT * 
FROM read_csv('food_orders.csv', AUTO_DETECT=TRUE, dateformat='%Y%m%d');

SELECT * 
FROM read_csv('food_orders.csv'
, dateformat='%Y%m%d'
, columns={
   'food_name': 'VARCHAR', 
   'order_date': 'DATE', 
   'quantity': 'INTEGER'}
, header=true);
```

## Loading multiple files

```sql
SELECT food_name, color, filename   
FROM read_csv('food_collection/pizza*.csv', 
AUTO_DETECT=TRUE, 
FILENAME=TRUE);
```

## Mixed schemas

```sql
SELECT *  
FROM read_csv('food_collection/*.csv', AUTO_DETECT=TRUE);

SELECT *  
FROM read_csv('food_collection/*.csv', 
UNION_BY_NAME=TRUE, 
AUTO_DETECT=TRUE);
```


# JSON Files

```sql
SELECT *  
FROM read_json('pizza_orders_records.json',  
AUTO_DETECT=true,
JSON_FORMAT='records');

SELECT *
FROM read_json('pizza_orders_array_of_records.json',
AUTO_DETECT=true,
JSON_FORMAT='array_of_records');
```

# Parquet


## Loading Parquet Files

```sql
SELECT * 
FROM parquet_schema('food_orders.parquet');

SELECT *
FROM read_parquet('food_orders.parquet');
```


# Exploring public data sets

- Visit  [Melbourne Bike Share Station Readings](https://data.melbourne.vic.gov.au/explore/dataset/melbourne-bike-share-station-readings-2011-2017/information/)
- Download the compressed CSV file locally to your local machine.
- Unzip the downloaded file to extract the files from the compressed file and save them on your computer
- You should have a file called `archive/74id-aqj9.csv` 

## Loading the bike station readings

```sql
.mode line

select *
from read_csv(
'archive/74id-aqj9.csv',
auto_detect=true)
limit 1;

.mode duckbox

CREATE TABLE bikes
as
SELECT * 
from read_csv(
  'archive/74id-aqj9.csv', 
  delim=',', 
  header=True, 
  dateformat='%Y%m%d%H%M%S',
  columns={
    'id': 'INT', 
    '_name': 'VARCHAR',
    'terminalname': 'INT',
    'nbbikes': 'INT',
    'nbemptydocks': 'VARCHAR',
    'rundate': 'DATE',
    'installed': 'BOOLEAN',
    '_temporary': 'BOOLEAN',
    '_locked': 'BOOLEAN',
    'lastcommwithserver': 'LONG',
    'latestupdatetime': 'LONG',
    'removaldate': 'LONG',
    'installdate': 'LONG',
    'lat': 'FLOAT',
    'long': 'FLOAT',
    '_location': 'VARCHAR'
  }
) ;


select count(*)
from bikes;

summarize 
select *  
from bikes;
```

# Exporting data

```sql
create table bike_rides_april 
as 
select * 
from bikes 
where rundate between '2017-04-01' and '2017-04-30';

COPY bike_rides_april
TO 'bike_rides_april.csv' (HEADER, DELIMITER ',');


COPY (select _name, rundate from bike_rides_april)
TO 'bike_rides_april.json' (FORMAT JSON, dateformat '%d %B %Y');


COPY bike_rides_april 
TO 'bike_rides' 
(FORMAT PARQUET, PARTITION_BY (rundate));


```

