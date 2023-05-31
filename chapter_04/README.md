# Chapter 04

## DuckDB indexes 
```sql
COPY (
    SELECT * 
    FROM read_parquet('s3://amazon-reviews-pds/parquet/product_category=Books/part-0000[0]*-*.parquet')
) TO 'reviews_original.parquet' (compression uncompressed);

CREATE OR REPLACE TABLE book_reviews
AS
SELECT *
FROM read_parquet('./reviews_original.parquet');

SELECT * 
FROM book_reviews
USING SAMPLE 10;

CREATE INDEX book_reviews_idx 
ON book_reviews(year);

-- selecting a year which is sparse
EXPLAIN SELECT count(*)
FROM book_reviews
WHERE year = 1995;

-- selecting a year which is common
EXPLAIN SELECT count(*)
FROM book_reviews
WHERE year = 2015;

-- close and restart DuckDB
.open

CALL pragma_database_size();

-- create in-memory
CREATE OR REPLACE TABLE book_reviews
AS
SELECT *
FROM read_parquet('./reviews_original.parquet');

CALL pragma_database_size();

CREATE INDEX book_reviews_idx1 
ON book_reviews(marketplace, star_rating);

CALL pragma_database_size();


-- close memory db and open disk database
.shell rm -f newdb.db;

.open newdb.db

CALL pragma_database_size();

-- create on disk
CREATE OR REPLACE TABLE book_reviews
AS
SELECT *
FROM read_parquet('./reviews_original.parquet');

CALL pragma_database_size();

CREATE INDEX book_reviews_idx1 
ON book_reviews(marketplace, star_rating);

CALL pragma_database_size();

SELECT * 
FROM duckdb_indexes;

DROP TABLE IF EXISTS book_reviews;

```

## Optimizing file read performance of DuckDB
```sql
-- Hive partitioning

-- configure to use only 1 thread
SET threads TO 1;

COPY (
    SELECT * 
    FROM read_parquet('./reviews_original.parquet')
) TO 'book_reviews_hive' (
    format parquet, 
    partition_by (year, marketplace), 
    overwrite_or_ignore true
);

.timer on
 
CREATE OR REPLACE TABLE book_reviews_2015_JP
AS
SELECT * 
FROM parquet_scan('book_reviews_hive/*/*/*.parquet', hive_partitioning=true)  
WHERE year='2015' 
AND marketplace='JP';

CREATE OR REPLACE TABLE book_reviews_2015_JP
AS
SELECT * 
FROM read_parquet('./reviews_original.parquet')  
WHERE year='2015' 
AND marketplace='JP';

DROP TABLE IF EXISTS book_reviews_2015;

.shell rm -fr book_reviews_hive;

-- Pushdown

-- Reset to default number of threads
reset threads;

-- create a fake file
COPY (
    SELECT rv.*
    FROM read_parquet('./reviews_original.parquet') rv CROSS JOIN (select range from range (0, 10)
    where (range=1 or marketplace<>'JP'))
) TO 'reviews_huge.parquet' (compression uncompressed);
;

-- return the current number of threads
SELECT current_setting('threads');


-- configure to use only 1 thread
SET threads TO 1;

PRAGMA enable_optimizer;
PRAGMA enable_profiling;
PRAGMA profiling_output='profile_with_pushdown.log';
--
CREATE OR REPLACE TABLE book_reviews_2015_JP
AS
SELECT marketplace, product_title, review_headline, review_date, year, product_category
FROM read_parquet('./reviews_huge*.parquet') 
WHERE marketplace='JP' 
AND year = 2015 ;
--
PRAGMA disable_profiling;


select * from book_reviews_2015_JP;

PRAGMA disable_optimizer;
PRAGMA enable_profiling;
PRAGMA profiling_output='profile_without_pushdown.log';
--
CREATE OR REPLACE TABLE book_reviews_2015_JP
AS
SELECT marketplace, product_title, review_headline, review_date, year, product_category
FROM read_parquet('./reviews_huge*.parquet') 
WHERE marketplace='JP' 
AND year = 2015 ;
--
PRAGMA disable_profiling;

DROP TABLE IF EXISTS book_reviews_2015_JP;

```


## Timestamp With Time Zone Functions
```sql

SET timezone = 'UTC';

CREATE OR REPLACE TABLE timestamp_demo (
    col_ts TIMESTAMP, 
    col_tstz TIMESTAMPTZ
);

INSERT INTO timestamp_demo (col_ts, col_tstz) VALUES('1969-07-21 02:56:00', '1969-07-21 02:56:00');

SELECT current_setting('timezone') as tz,
col_ts,
extract(epoch from col_ts) as epoc_ts,
col_tstz,
extract(epoch from col_tstz) as epoc_tstz
FROM timestamp_demo;

SET timezone = 'America/New_York';

SELECT current_setting('timezone') as tz,
col_ts,
extract(epoch from col_ts) as epoc_ts,
col_tstz,
extract(epoch from col_tstz) as epoc_tstz
FROM timestamp_demo;


SET timezone = 'America/New_York';

SELECT current_setting('timezone') as tz,
col_ts,
dayofmonth(col_ts) as day_of_month_ts,
dayname(col_ts) as day_name_ts,
col_tstz,
dayofmonth(col_tstz)  as day_of_month_tstz,
dayname(col_tstz)  as day_name_tstz
FROM timestamp_demo;
```

## Window Functions
```sql
CREATE OR REPLACE TABLE apollo_events
AS
SELECT * 
FROM read_csv('apollo.csv', auto_detect=true, header=true, timestampformat='%d/%b/%Y %H:%M');

SELECT *
FROM apollo_events
WHERE astronaut = 'Neil Armstrong'
ORDER BY event_time;

SELECT TIMESTAMP '1969-07-21 05:09:00' - TIMESTAMP '1969-07-21 02:56:00' as interval_on_moon;

SELECT event_description, 
event_time, 
astronaut, astronaut_location, 
LEAD(event_time, 1) OVER(PARTITION BY astronaut ORDER BY event_time) as end_time,
end_time-event_time as event_duration
FROM apollo_events
WHERE astronaut = 'Neil Armstrong'
ORDER BY astronaut, event_time;
```


