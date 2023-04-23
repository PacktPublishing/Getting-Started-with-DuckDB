# Chapter 02

The file `foods_no_heading.csv` contains this data
```
apple,red,100,true
banana,yellow,100,true
cookie,brown,200,false
chocolate,brown,150,false
```

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

Shows we have loaded four rows of data into the table `foods`
```
┌───────────┬─────────┬──────────┬────────────┐
│ food_name │  color  │ calories │ is_healthy │
│  varchar  │ varchar │  int32   │  boolean   │
├───────────┼─────────┼──────────┼────────────┤
│ apple     │ red     │      100 │ true       │
│ banana    │ yellow  │      100 │ true       │
│ cookie    │ brown   │      200 │ false      │
│ chocolate │ brown   │      150 │ false      │
└───────────┴─────────┴──────────┴────────────┘
```

Now led us look at some data with an unexpected value. The file `foods_error.csv` contains text

```
orange,orange,eighty,true
```

```sql
COPY foods FROM 'foods_error.csv';
```

```
Error: Invalid Input Error: Could not convert string 'eighty' to INT32 at line 1 in column 2.
```

```sql
SELECT * 
FROM read_csv_auto('foods_no_heading.csv');
```

```
┌───────────┬─────────┬──────────┬────────────┐
│ food_name │  color  │ calories │ is_healthy │
│  varchar  │ varchar │  int64   │  boolean   │
├───────────┼─────────┼──────────┼────────────┤
│ apple     │ red     │      100 │ true       │
│ banana    │ yellow  │      100 │ true       │
│ cookie    │ brown   │      200 │ false      │
│ chocolate │ brown   │      150 │ false      │
└───────────┴─────────┴──────────┴────────────┘
```


Now imagine we have some illegal or unexpected data. The following data file has a string (eighty) instead of a number

```
orange,orange,eighty,true
pear,green,60,true
```

```sql
COPY foods FROM 'foods_error.csv' ;
```

Gives this error

```
Error: Invalid Input Error: Could not convert string 'eighty' to INT32 at line 1 in column 2. Parser options:
```

We can use the `ignore_errors` option to skip over any parsing errors encountered. This alows us to instead ignore rows with errors.

```sql
COPY foods FROM 'foods_error.csv' (ignore_errors true);
```



## Mixed schemas

We sometime face the situation where the files we wish to load many files - but there is a difference in how they are formatted. Take for example the situation where we wish to load a collection of food files, but the 

```
food_name,color
pizza,mixed
fries,yellow
```

However our next file has a different ordering of columns

```
color,food_name,calories,is_healthy
green,salad,50,TRUE
white,yogurt,20,TRUE
```

```sql
SELECT * 
FROM read_csv_auto('food_collection/*.csv');
```


Gives this error

```
Error: Invalid Input Error: Could not convert string 'FALSE' to INT64 at line 2 in column 2. Parser options:
```

```sql
SELECT * 
FROM read_csv_auto('food_collection/*.csv', union_by_name=True);
```

```
┌───────────┬─────────┬──────────┬────────────┐
│ food_name │  color  │ calories │ is_healthy │
│  varchar  │ varchar │  int64   │  boolean   │
├───────────┼─────────┼──────────┼────────────┤
│ sushi     │ mixed   │       60 │ true       │
│ pho       │ yellow  │       70 │ true       │
│ pizza     │ mixed   │      120 │ false      │
│ fries     │ yellow  │      100 │ false      │
│ salad     │ green   │       50 │ true       │
│ yogurt    │ white   │       20 │ true       │
└───────────┴─────────┴──────────┴────────────┘
```


## Date formatting

```sql
describe SELECT * FROM read_csv_auto('food_orders.csv');
```

```
┌─────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
│ column_name │ column_type │  null   │   key   │ default │  extra  │
│   varchar   │   varchar   │ varchar │ varchar │ varchar │ varchar │
├─────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
│ food_name   │ VARCHAR     │ YES     │         │         │         │
│ order_date  │ VARCHAR     │ YES     │         │         │         │
│ quantity    │ BIGINT      │ YES     │         │         │         │
└─────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┘
```

What we want to do is to ensure the `order_date` is actually a DATE and not a VARCHAR



```sql
SELECT * 
from read_csv('food_orders.csv', 
delim=',', 
header=True, 
dateformat='%Y-%b-%d',
columns={
  'food_name': 'VARCHAR',
  'order_date ': 'DATE',
  'quantity': 'INT'
 }
) ;

create table food_orders (food_name varchar not null, order_date date not null, quantity integer, PRIMARY KEY(food_name, order_date));
```




## Parquet

```sql
describe select * from read_parquet('food_orders.parquet');
```

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