# Chapter 05

## Full-Text Search Indexes

```sql

SELECT * 
FROM duckdb_extensions();

LOAD json;
INSTALL json;

SELECT * 
FROM read_json('./countries.json', auto_detect=true, format='auto');


LOAD httpfs;
INSTALL httpfs;

SELECT *
FROM read_csv('https://www2.census.gov/programs-surveys/stc/datasets/2022/FY2022-Flat-File.txt', AUTO_DETECT=TRUE);



SELECT * 
FROM duckdb_settings()
WHERE name like 's3%';


SET s3_region='us-east-1';
SET s3_endpoint='s3.amazonaws.com';
SET s3_access_key_id=''; -- add AWS access key id here if not public
SET s3_secret_access_key=''; -- add AWS secret access key here if not public


SELECT *
FROM read_parquet('s3://duckdb-s3-bucket-public/countries.parquet');

-- Allow unused blocks to be offloaded to disk if required
PRAGMA temp_directory='./tmp.tmp';

CREATE OR REPLACE SEQUENCE book_details_seq;

CREATE OR REPLACE TABLE book_details
AS
SELECT nextval('book_details_seq') as book_details_id,
"Title" as book_title, 
description as book_description
FROM read_csv('../chapter_04/books_data.csv',  AUTO_DETECT=TRUE);

SUMMARIZE book_details;

INSTALL fts; 
LOAD fts;

PRAGMA create_fts_index('book_details', 'book_details_id', 'book_title', 'book_description', overwrite='TRUE');

WITH cte AS
(
    SELECT *, fts_main_book_details.match_bm25(book_details_id, 'travel france wine') AS match_score
    FROM book_details
)
SELECT book_title, book_description, match_score
FROM cte
WHERE match_score IS NOT NULL
ORDER BY match_score DESC
LIMIT 10;
```

## Geo-spatial extension

```sql

INSTALL spatial; 
LOAD spatial; 

-- reading Excel XLSX files
CREATE OR REPLACE TABLE stations AS
SELECT * 
FROM st_read('stations.xlsx', layer='stations');

SELECT *
FROM stations
WHERE station_name in ('Paris Montparnasse', 'Bordeaux');

SELECT st_point(48.858935, 2.293412) AS Eiffel_Tower;

SELECT  
st_point(48.858935, 2.293412) AS Eiffel_Tower, 
st_point(48.873407, 2.295471) AS Arc_de_Triomphe,
st_distance(
  st_transform(Eiffel_Tower, 'EPSG:4326', 'EPSG:27563'), 
  st_transform(Arc_de_Triomphe, 'EPSG:4326', 'EPSG:27563')
) as Aerial_Distance_M;

SELECT wkb_geometry 
FROM st_read('./bordeaux_wine_region.geojson');


SELECT station_name, st_point(latitude,longitude), st_point(longitude,latitude)
FROM stations
WHERE st_within(st_point(longitude, latitude), 
  (
    SELECT  ST_GeomFromWKB(wkb_geometry) 
    FROM st_read('./bordeaux_wine_region.geojson'))
  );

```

##

```sql

CREATE OR REPLACE TABLE wines AS
SELECT *
from read_csv('wines.csv', AUTO_DETECT=TRUE);

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
WHERE start_with = 'Mission Haut Blanc';

```