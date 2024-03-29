# Chapter 02

# Load data into DuckDB 


```sql
CREATE OR REPLACE TABLE foods (
  food_name VARCHAR PRIMARY KEY, 
  color VARCHAR,
  calories INT, 
  is_healthy BOOLEAN
);

COPY foods FROM 'foods_no_heading.csv';

SELECT * 
FROM foods;
```

## COPY file options during load

```sql
COPY foods (food_name, is_healthy, color, calories)  
FROM 'foods_with_heading.tsv' (DELIMITER '\t', HEADER);
```


## Error handling with the COPY Statement

```sql
COPY foods FROM 'foods_error.csv';

COPY foods FROM 'foods_error.csv' (IGNORE_ERRORS true);
```


## File loading with read_csv

```sql
SELECT * 
FROM read_csv('food_prices.csv', auto_detect=true);

DESCRIBE SELECT * 
FROM read_csv('food_prices.csv', auto_detect=true);
```

## Table creation with read_csv

```sql
CREATE OR REPLACE TABLE low_cost_foods 
as 
SELECT * 
FROM read_csv('food_prices.csv', auto_detect=true) 
WHERE price < 4.00;

SELECT * 
FROM low_cost_foods;
```

## Date formats 

```sql
SELECT * 
FROM read_csv('food_orders.csv', auto_detect=true);

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
auto_detect=true, 
filename=true);
```

## Mixed schemas

```sql
SELECT *  
FROM read_csv('food_collection/*.csv', auto_detect=true);

SELECT *  
FROM read_csv('food_collection/*.csv', 
union_by_name=true, 
auto_detect=true);
```


# JSON Files

```sql
SELECT *  
FROM read_json('pizza_orders_records.json',  
auto_detect=true,
format='newline_delimited');

SELECT *
FROM read_json('pizza_orders_array_of_records.json',
auto_detect=true,
format='array');
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

SELECT *
FROM read_csv(
'archive/74id-aqj9.csv',
auto_detect=true)
LIMIT 1;

.mode duckbox

CREATE OR REPLACE TABLE bikes
AS
SELECT * 
from read_csv(
  'archive/74id-aqj9.csv', 
  header=true, 
  dateformat='%Y%m%d%H%M%S',
  columns={
    'id': 'INT', 
    'name': 'VARCHAR',
    'terminalname': 'INT',
    'nbbikes': 'INT',
    'nbemptydocks': 'VARCHAR',
    'rundate': 'DATE',
    'installed': 'BOOLEAN',
    'temporary': 'BOOLEAN',
    'locked': 'BOOLEAN',
    'lastcommwithserver': 'LONG',
    'latestupdatetime': 'LONG',
    'removaldate': 'LONG',
    'installdate': 'LONG',
    'lat': 'FLOAT',
    'long': 'FLOAT',
    'location': 'VARCHAR'
  }
) ;


SELECT count(*)
FROM bikes;

SUMMARIZE 
SELECT *  
FROM bikes;
```

# Exporting table data

```sql
CREATE OR REPLACE TABLE bike_rides_april 
as 
SELECT * 
FROM bikes 
WHERE rundate between '2017-04-01' and '2017-04-30';

COPY bike_rides_april
TO 'bike_rides_april.csv' (HEADER, DELIMITER ',');


COPY (select name, rundate from bike_rides_april)
TO 'bike_rides_april.json' (FORMAT JSON, dateformat '%d %B %Y');


COPY bike_rides_april 
TO 'bike_rides' 
(FORMAT PARQUET, PARTITION_BY (rundate), OVERWRITE_OR_IGNORE true);
```

# Exporting a database

```sql
EXPORT DATABASE 'export_directory';
```