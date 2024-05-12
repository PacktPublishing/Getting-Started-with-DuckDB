# Chapter 04

# How to download the dataset

The dataset for this project is hosted by Kaggle. To download the necessary dataset for this project, please follow the instructions below.

1. Go to https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews
2. Click on the 'Download' button
3. Kaggle will prompt you to sign in or to register. If you do not have a Kaggle account, you can register for one.
4. Upon signing in, the download will start automatically.
5. After the download is complete, unzip the "archive" zip file

```bash
cd chapter_04 
unzip archive.zip
``` 

### Out of Memory Error

If you receive the error "Error: Out of Memory Error: failed to allocate data of size", allow unused blocks to be offloaded to disk if required

You can ignore this step otherwise
```sql
PRAGMA temp_directory='./tmp.tmp';
```


## DuckDB indexes 
```sql
CREATE OR REPLACE SEQUENCE book_reviews_seq;

COPY (
    SELECT nextval('book_reviews_seq') as book_reviews_id,
    Id as book_id,
    Title as book_title,
    Price as price,
    User_id as user_id,
    region,
    to_timestamp("review/time") as review_time,
    cast(datepart('year', review_time) as VARCHAR) as review_year,
    "review/summary" as review_summary, 
    "review/text" as review_text,
    "review/score" as review_score
    FROM read_csv('./Books_rating.csv',  AUTO_DETECT=TRUE) 
    CROSS JOIN (SELECT range, case when range=0 then 'JP' else 'US' end as region FROM range (0, 2))
) TO 'book_reviews.parquet';


CREATE OR REPLACE TABLE book_reviews AS
SELECT *
FROM read_parquet('./book_reviews.parquet');

SUMMARIZE book_reviews;

SELECT * 
FROM book_reviews
USING SAMPLE 10;

EXPLAIN SELECT count(*)
FROM book_reviews
WHERE user_id = 'A1WQVN65FTJCJ6';

EXPLAIN SELECT count(*)
FROM book_reviews
WHERE review_year = '2012';

CREATE INDEX book_reviews_idx_user_id 
ON book_reviews(user_id);

CREATE INDEX book_reviews_idx_year 
ON book_reviews(review_year);

SELECT * 
FROM duckdb_indexes;

EXPLAIN
SELECT count(*)
FROM book_reviews
WHERE user_id = 'A1RRTLWXDOYER5';

EXPLAIN
SELECT count(*)
FROM book_reviews
WHERE review_year = '2012';


-- close and restart DuckDB
.open

PRAGMA database_size;

-- create in-memory
CREATE OR REPLACE TABLE book_reviews AS
SELECT *
FROM read_parquet('book_reviews.parquet');

PRAGMA database_size;

CREATE INDEX book_reviews_idx1 ON book_reviews(region, review_score);

PRAGMA database_size;

```

## Optimizing file read performance of DuckDB
```sql
-- Hive partitioning

-- configure to use only 1 thread
SET threads TO 1;

COPY (
    SELECT * 
    FROM read_parquet('book_reviews.parquet')
) TO 'book_reviews_hive' (
    FORMAT parquet, 
    PARTITION_BY (review_year, region), 
    OVERWRITE_OR_IGNORE true
);

.timer on

SELECT * 
FROM read_parquet('book_reviews_hive/*/*/*.parquet', hive_partitioning=true)  
WHERE review_year='2012' AND region='JP';


SELECT *
FROM read_parquet('book_reviews.parquet')
WHERE review_year='2012' AND region='JP';


-- Pushdown


-- configure to use only 1 thread
SET threads TO 1;

PRAGMA enable_optimizer;
PRAGMA enable_profiling;
PRAGMA profiling_output='profile_with_pushdown.log';
--
CREATE OR REPLACE TABLE book_reviews_1970_JP AS
SELECT region, review_summary, review_text, review_time, review_year
FROM read_parquet('./book_reviews.parquet') 
WHERE region='JP' 
AND review_year = '1970' ;
--
PRAGMA disable_profiling;



PRAGMA disable_optimizer;
PRAGMA enable_profiling;
PRAGMA profiling_output='profile_without_pushdown.log';
--
CREATE OR REPLACE TABLE book_reviews_1970_JP AS
SELECT region, review_summary, review_text, review_time, review_year
FROM read_parquet('./book_reviews.parquet') 
WHERE region='JP' AND review_year = '1970' ;
--
PRAGMA disable_profiling;

DROP TABLE IF EXISTS book_reviews_1970_JP;

```


## Timestamp With Time Zone Functions
```sql

SELECT TIMESTAMP '1969-07-21 02:56:00' AS moonstep;

SELECT TIMESTAMPTZ '1969-07-20 22:56:00-04' AS moonstep_z;

SET timezone = 'UTC';

CREATE OR REPLACE TABLE timestamp_demo (
    col_ts TIMESTAMP, 
    col_tstz TIMESTAMPTZ
);

INSERT INTO timestamp_demo (col_ts, col_tstz) VALUES('1969-07-21 02:56:00', '1969-07-21 02:56:00');

SELECT current_setting('timezone') AS tz,
    col_ts,
    extract(epoch from col_ts) AS epoc_ts,
    col_tstz,
    extract(epoch from col_tstz) AS epoc_tstz
FROM timestamp_demo;

SET timezone = 'America/New_York';

SELECT current_setting('timezone') AS tz,
    col_ts,
    extract(epoch from col_ts) AS epoc_ts,
    col_tstz,
    extract(epoch from col_tstz) AS epoc_tstz
FROM timestamp_demo;


SET timezone = 'America/New_York';

SELECT current_setting('timezone') AS tz,
    col_ts,
    dayofmonth(col_ts) AS day_of_month_ts,
    dayname(col_ts) AS day_name_ts,
    col_tstz,
    dayofmonth(col_tstz) AS day_of_month_tstz,
    dayname(col_tstz) AS day_name_tstz
FROM timestamp_demo;
```

## Window Functions
```sql
CREATE OR REPLACE TABLE apollo_events AS
SELECT *
FROM read_csv(
    'apollo.csv',
    timestampformat='%d/%b/%Y %H:%M',
    columns={
        'event_time': 'TIMESTAMP', 
        'astronaut': 'VARCHAR',
        'event_description': 'VARCHAR',
        'astronaut_location': 'VARCHAR'
    }
);


SELECT *
FROM apollo_events
WHERE astronaut = 'Neil Armstrong'
ORDER BY event_time;

SELECT TIMESTAMP '1969-07-21 05:09:00' - TIMESTAMP '1969-07-21 02:56:00' as interval_on_moon;

CREATE OR REPLACE VIEW apollo_activities AS
SELECT event_description, 
    event_time, 
    astronaut, 
    astronaut_location, 
    LEAD(event_time, 1) OVER(PARTITION BY astronaut ORDER BY event_time) AS end_time,
    end_time-event_time AS event_duration
FROM apollo_events;

SELECT *
FROM apollo_activities
WHERE astronaut = 'Neil Armstrong'
ORDER BY astronaut, event_time;

SELECT *
FROM apollo_activities
WHERE astronaut_location = 'Moon'
ORDER BY event_time;
```


