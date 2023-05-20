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

-- Pushdown

-- create a fake file
COPY (
    SELECT rv.*
    FROM read_parquet('./reviews_original.parquet') rv, (select range from range (0, 10)
    where (range=1 or marketplace<>'JP'))
) TO 'reviews_huge.parquet' (compression uncompressed);
;

EXPLAIN SELECT * 
FROM read_parquet('./reviews_huge.parquet') 
WHERE marketplace='DE' ;

PRAGMA enable_optimizer;
PRAGMA enable_profiling;
PRAGMA profiling_output='profile_with_pushdown.log';
--
CREATE OR REPLACE TABLE BOOK_REVIEWS
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
CREATE OR REPLACE TABLE BOOK_REVIEWS
AS
SELECT * 
FROM read_parquet('./reviews_huge*.parquet') 
WHERE marketplace='JP' 
AND year = 2015 ;
--
PRAGMA disable_profiling;

```

## Timestamp With Time Zone Functions
```sql

```

## Window Functions
```sql

```

## Optimizing performance of DuckDB
```sql

```

