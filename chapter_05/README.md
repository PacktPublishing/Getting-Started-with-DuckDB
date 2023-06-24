# Chapter 05

## DuckDB Extensions

```sql
SELECT * 
FROM duckdb_extensions();

INSTALL sqlite_scanner;
LOAD sqlite_scanner;

SELECT 
    extension_name,
    installed,
    loaded
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
FROM read_csv(
        'https://www2.census.gov/programs-surveys/popest/datasets/2020-2022/cities/totals/sub-est2022.csv',
        auto_detect=TRUE
    );

SELECT * FROM duckdb_settings()
WHERE name like 's3%';

SET s3_region='us-east-1';
SET s3_endpoint='s3.amazonaws.com';

SELECT *
FROM read_parquet('s3://duckdb-s3-bucket-public/countries.parquet')
WHERE name SIMILAR TO '.*Republic.*';

-- Allow unused blocks to be offloaded to disk if required
PRAGMA temp_directory='./tmp.tmp';

CREATE OR REPLACE SEQUENCE book_details_seq;

CREATE OR REPLACE TABLE book_details AS
SELECT nextval('book_details_seq') AS book_details_id,
    "Title" as book_title, 
    description as book_description
FROM read_csv('../chapter_04/books_data.csv',  auto_detect=TRUE);

SUMMARIZE book_details;

-- full-text search indexes
INSTALL fts; 
LOAD fts;

PRAGMA create_fts_index(
         'book_details',
         'book_details_id',
         'book_title',
         'book_description',
         overwrite=1
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
    ) as Aerial_Distance_M;


SELECT *
FROM st_read('stations.xlsx', layer='stations');

-- reading Excel XLSX files
CREATE OR REPLACE TABLE stations AS
SELECT *
FROM st_read('stations.xlsx', layer='stations');

SELECT wkb_geometry FROM st_read('./bordeaux_wine_region.geojson');

SELECT station_name
FROM stations
WHERE st_within(
    st_point(longitude, latitude), 
    (
        SELECT  st_geomfromwkb(wkb_geometry)
        FROM st_read('./bordeaux_wine_region.geojson')
    )
);
```

## Recursive queries and macros

```sql
CREATE OR REPLACE TABLE wines AS
SELECT *
FROM read_csv('wines.csv', auto_detect=TRUE);

SELECT *
FROM wines
ORDER BY wine_id;

WITH RECURSIVE wine_hierarchy(wine_id, start_with, wine_path) AS 
(
  SELECT wine_id, wine_name, [wine_name] AS wine_path
  FROM wines
  WHERE sub_class_of IS NULL
  UNION ALL
  SELECT wines.wine_id, wines.wine_name, list_prepend(wines.wine_name, wine_hierarchy.wine_path)
  FROM wines, wine_hierarchy
  WHERE wines.sub_class_of = wine_hierarchy.wine_id
)
SELECT wine_path
FROM wine_hierarchy
WHERE start_with = 'Rothschild';


-- macros
CREATE OR REPLACE TABLE wine_prices AS
SELECT *
FROM read_csv('wine_prices.csv', auto_detect=TRUE);

SELECT wine_name, price, capacity_ml
FROM wine_prices;

CREATE OR REPLACE MACRO unit_price(price, capacity) AS round(price/capacity, 3);

SELECT wine_name, 
    price,
    capacity_ml,
    unit_price(price, capacity_ml) AS price_ml
FROM wine_prices;

```

