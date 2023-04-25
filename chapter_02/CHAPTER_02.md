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


# Parquet


## Loading Parquet Files

```sql
describe select * from read_parquet('food_orders.parquet');
```


# IGNORE BELOW


```
┌─────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
│ column_name │ column_type │  null   │   key   │ default │  extra  │
│   varchar   │   varchar   │ varchar │ varchar │ varchar │ varchar │
├─────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
│ food_name   │ VARCHAR     │ YES     │         │         │         │
│ order_date  │ DATE        │ YES     │         │         │         │
│ quantity    │ INTEGER     │ YES     │         │         │         │
└─────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┘
```


```sql
drop table if exists food_orders;

create table food_orders as select * from read_parquet('food_orders.parquet');
```

```sql
 describe food_orders;
 ```

 ```
┌─────────────┬─────────────┬─────────┬─────────┬─────────┬───────┐
│ column_name │ column_type │  null   │   key   │ default │ extra │
│   varchar   │   varchar   │ varchar │ varchar │ varchar │ int32 │
├─────────────┼─────────────┼─────────┼─────────┼─────────┼───────┤
│ food_name   │ VARCHAR     │ YES     │         │         │       │
│ order_date  │ DATE        │ YES     │         │         │       │
│ quantity    │ INTEGER     │ YES     │         │         │       │
└─────────────┴─────────────┴─────────┴─────────┴─────────┴───────┘
```



https://duckdb.org/docs/api/cli.html 

```sql
.mode line

select * 
from read_csv(
  'archive/74id-aqj9.csv', 
  auto_detect=true, 
  normalize_names=true, 
  IGNORE_ERRORS=true
) 
limit 1;
```

```
                id = 2
             _name = Harbour Town - Docklands
      terminalname = 60000
           nbbikes = 10
      nbemptydocks = 11
           rundate = 20170422134506
         installed = true
        _temporary = false
           _locked = false
lastcommwithserver = 1492832566010
  latestupdatetime = 1492832565029
       removaldate =
       installdate = 1313724600000
               lat = -37.814022
              long = 144.939521
         _location = (-37.814022, 144.939521)
```

```sql
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


 .mode duckbox
```


# OLD BELOW

```sql
create table wines as select * from read_csv('archive/winemag-data_first150k.csv', auto_detect=true);
D describe wines;
┌─────────────┬─────────────┬─────────┬─────────┬─────────┬───────┐
│ column_name │ column_type │  null   │   key   │ default │ extra │
│   varchar   │   varchar   │ varchar │ varchar │ varchar │ int32 │
├─────────────┼─────────────┼─────────┼─────────┼─────────┼───────┤
│ column00    │ BIGINT      │ YES     │         │         │       │
│ country     │ VARCHAR     │ YES     │         │         │       │
│ description │ VARCHAR     │ YES     │         │         │       │
│ designation │ VARCHAR     │ YES     │         │         │       │
│ points      │ BIGINT      │ YES     │         │         │       │
│ price       │ DOUBLE      │ YES     │         │         │       │
│ province    │ VARCHAR     │ YES     │         │         │       │
│ region_1    │ VARCHAR     │ YES     │         │         │       │
│ region_2    │ VARCHAR     │ YES     │         │         │       │
│ variety     │ VARCHAR     │ YES     │         │         │       │
│ winery      │ VARCHAR     │ YES     │         │         │       │
├─────────────┴─────────────┴─────────┴─────────┴─────────┴───────┤
│ 11 rows                                               6 columns │
└─────────────────────────────────────────────────────────────────┘
```

```
D select * from read_csv('archive/74id-aqj9.csv', auto_detect=true, normalize_names=true, IGNORE_ERRORS=true) limit 1;
┌───────┬──────────────────────┬──────────────┬─────────┬───┬────────────┬────────────┬──────────────────────┐
│  id   │        _name         │ terminalname │ nbbikes │ … │    lat     │    long    │      _location       │
│ int64 │       varchar        │    int64     │  int64  │   │   double   │   double   │       varchar        │
├───────┼──────────────────────┼──────────────┼─────────┼───┼────────────┼────────────┼──────────────────────┤
│     2 │ Harbour Town - Doc…  │        60000 │      10 │ … │ -37.814022 │ 144.939521 │ (-37.814022, 144.9…  │
├───────┴──────────────────────┴──────────────┴─────────┴───┴────────────┴────────────┴──────────────────────┤
│ 1 rows                                                                                16 columns (7 shown) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```