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

ALTER TABLE book_reviews ADD book_reviews_id INT;

UPDATE book_reviews 
SET book_reviews_id = rowid;

ALTER TABLE book_reviews ALTER book_reviews_id SET NOT NULL;

CREATE UNIQUE INDEX book_reviews_pk on book_reviews(book_reviews_id);

EXPLAIN SELECT *
FROM book_reviews
WHERE book_reviews_id = 1;

SELECT year, count(*)
FROM book_reviews
GROUP BY 1;

CREATE INDEX book_reviews_idx on book_reviews(year);

-- selecting a year which is sparse
EXPLAIN SELECT count(*)
FROM book_reviews
WHERE year = 1995;

-- selecting a year which is common
EXPLAIN SELECT count(*)
FROM book_reviews
WHERE year = 2015;

DROP TABLE IF EXISTS book_reviews;

-- Hive partitioning
COPY (
    SELECT * 
    FROM read_parquet('./reviews_original.parquet')
) TO 'book_reviews_hive' (
    format parquet, 
    partition_by (year), 
    overwrite_or_ignore true
);

EXPLAIN SELECT * 
FROM parquet_scan('book_reviews_hive/*/*.parquet', hive_partitioning=true) 
WHERE year = 2015;

.timer on
PRAGMA disable_optimizer;
PRAGMA enable_optimizer;

CREATE OR REPLACE TABLE book_reviews_2015
AS
SELECT * 
FROM parquet_scan('book_reviews_hive/*/*.parquet', hive_partitioning=true) 
WHERE year = 2015;

EXPLAIN CREATE OR REPLACE TABLE book_reviews_2015
AS
SELECT * 
FROM parquet_scan('book_reviews_hive/*/*.parquet', hive_partitioning=true) 
WHERE year = 2015;

DROP TABLE IF EXISTS book_reviews_2015;

.shell rm -fr book_reviews_hive;
```

## Optimizing performance of DuckDB
```sql
-- Pushdown

-- create a fake file
COPY (
    SELECT rv.*
    FROM read_parquet('./reviews_original.parquet') rv, (select range from range (0, 10)
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
CREATE OR REPLACE TABLE book_reviews_jp_2015
AS
SELECT * 
FROM read_parquet('./reviews_huge*.parquet') 
WHERE marketplace='JP' 
AND year = 2015 ;
--
PRAGMA disable_profiling;


PRAGMA disable_optimizer;
PRAGMA enable_profiling;
PRAGMA profiling_output='profile_without_pushdown.log';
--
CREATE OR REPLACE TABLE book_reviews_jp_2015
AS
SELECT * 
FROM read_parquet('./reviews_huge*.parquet') 
WHERE marketplace='JP' 
AND year = 2015 ;
--
PRAGMA disable_profiling;

DROP TABLE IF EXISTS book_reviews_jp_2015;

-- close and restart DuckDB
CALL pragma_database_size();

CREATE OR REPLACE TABLE book_reviews
AS
SELECT *
FROM read_parquet('./reviews_original.parquet');

create  index book_reviews_idx3 on book_reviews(marketplace, star_rating);
-- adds 0.3GB
drop index if exists book_reviews_idx3;

create  index book_reviews_idx3 on book_reviews(review_headline);
-- adds 0.8GB

SELECT * FROM   duckdb_indexes;

duckdb_indexes();

```


## Timestamp With Time Zone Functions
```sql

```

## Window Functions
```sql
CREATE OR REPLACE TABLE appolo_events
AS
SELECT * 
FROM read_csv('apollo.csv', auto_detect=true, header=true, timestampformat='%d/%b/%Y %H:%M');


SELECT event_description, 
event_time, 
astronaut, astronaut_location, 
LEAD(event_time, 1) OVER(PARTITION BY astronaut ORDER BY event_time) as end_time,
end_time-event_time
FROM appolo_events
ORDER BY astronaut, event_time;


```


