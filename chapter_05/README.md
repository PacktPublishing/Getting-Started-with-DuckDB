# Chapter 05

## DuckDB Extensions

```sql
SELECT * 
FROM duckdb_extensions();

INSTALL sqlite_scanner;
LOAD sqlite_scanner;

SELECT extension_name, installed, loaded
FROM duckdb_extensions()
WHERE extension_name = 'sqlite_scanner';

ATTACH 'my_sqlite.db' (TYPE sqlite);

SHOW ALL TABLES;

SELECT *
FROM my_sqlite.countries_sqlite;


-- Reading remote files with the httpfs extension
INSTALL httpfs;
LOAD httpfs;

SELECT *
FROM read_csv('https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/cities/totals/sub-est2022.csv');


CREATE OR REPLACE SECRET mysecret (
    TYPE S3,
    REGION 'us-east-1',
    ENDPOINT 's3.amazonaws.com'
);


SELECT *
FROM read_parquet('s3://duckdb-s3-bucket-public/countries.parquet')
WHERE name SIMILAR TO '.*Republic.*';

-- Allow unused blocks to be offloaded to disk if required
PRAGMA temp_directory='./tmp.tmp';

CREATE OR REPLACE SEQUENCE book_details_seq;

CREATE OR REPLACE TABLE book_details AS
SELECT nextval('book_details_seq') AS book_details_id,
    Title as book_title, 
    description as book_description
FROM read_csv('../chapter_04/books_data.csv');

SUMMARIZE book_details;

-- full-text search indexes
INSTALL fts; 
LOAD fts;

PRAGMA create_fts_index(
    'book_details',
    'book_details_id',
    'book_title',
    'book_description',
    overwrite=true
);

WITH book_cte AS (
    SELECT *, 
    fts_main_book_details.match_bm25(
        book_details_id, 
        'travel france wine'
      ) AS match_score
    FROM book_details
)
SELECT book_title, book_description, match_score
FROM book_cte
WHERE match_score IS NOT NULL
ORDER BY match_score DESC
LIMIT 10;
```

## Geo-spatial extension

```sql
INSTALL spatial; 
LOAD spatial; 

SELECT st_point(48.858935, 2.293412) AS Eiffel_Tower;

SELECT
  st_point(48.858935, 2.293412) AS Eiffel_Tower, 
  st_point(48.873407, 2.295471) AS Arc_de_Triomphe,
  st_distance(
      st_transform(Eiffel_Tower, 'EPSG:4326', 'EPSG:27563'), 
      st_transform(Arc_de_Triomphe, 'EPSG:4326', 'EPSG:27563')
  ) AS Aerial_Distance_M;


SELECT *
FROM st_read('stations.xlsx', layer='stations');

-- reading Excel XLSX files
CREATE OR REPLACE TABLE stations AS
SELECT *
FROM st_read('stations.xlsx', layer='stations');

SELECT geom FROM st_read('bordeaux_wine_region.geojson');

SELECT station_name
FROM stations
WHERE st_within(
    st_point(longitude, latitude), 
    (
        SELECT geom
        FROM st_read('bordeaux_wine_region.geojson')
    )
);
```

