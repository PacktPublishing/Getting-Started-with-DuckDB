# Chapter 05

## Full-Text Search Indexes

```sql

LOAD httpfs;
INSTALL httpfs;

SELECT * 
FROM duckdb_extensions();


SELECT * 
FROM duckdb_settings()
WHERE name like 's3%';


SET s3_region='us-east-1';
SET s3_endpoint='s3.amazonaws.com';
SET s3_access_key_id=''; -- add AWS access key id here if not public
SET s3_secret_access_key=''; -- add AWS secret access key here if not public


COPY (
    SELECT review_id, product_title, review_headline, star_rating
    FROM read_parquet('s3://amazon-reviews-pds/parquet/*/part-0000[0]*-*.parquet', hive_partitioning=True)
    WHERE product_category='Books'
) TO 'reviews_original.parquet';


CREATE OR REPLACE TABLE book_reviews
AS
SELECT *
FROM read_parquet('./reviews_original.parquet');


INSTALL fts; 
LOAD fts;

PRAGMA create_fts_index('book_reviews', 'review_id', 'product_title', 'review_headline', overwrite='TRUE');

WITH cte AS
(
    SELECT *, fts_main_book_reviews.match_bm25(review_id, 'travel france wine') AS match_score
    FROM book_reviews
)
SELECT product_title, review_headline, match_score, star_rating
FROM cte
WHERE match_score IS NOT NULL
ORDER BY match_score DESC
LIMIT 10;
```

## Geo-spatial extension

```sql

INSTALL spatial; 
LOAD spatial; 

CREATE OR REPLACE TABLE stations AS
SELECT * 
FROM st_read('stations.xlsx', layer='stations');

SELECT *
FROM stations
WHERE station_name in ('Paris Montparnasse', 'Bordeaux');



SELECT ST_Point(44.837731, -0.577431) AS Paris, ST_Point(48.841172 , 2.320514) AS Bordeaux;

SELECT ST_Distance(ST_Point(44.837731, -0.577431) , ST_Point(48.841172 , 2.320514) );

SELECT  
st_point(48.858935, 2.293412) AS Eiffel_Tower, 
st_point(48.873407, 2.295471) AS Arc_de_Triomphe,
st_distance(
  st_transform(Eiffel_Tower, 'EPSG:4326', 'EPSG:27563'), 
  st_transform(Arc_de_Triomphe, 'EPSG:4326', 'EPSG:27563')
) as Aerial_Distance_M;

SELECT  
ST_Point(44.837731, -0.577431) AS Paris, 
ST_Point(48.841172 , 2.320514) AS Bordeaux,
st_distance(
st_transform(Paris, 'EPSG:4326', 'EPSG:27563'), 
st_transform(Bordeaux, 'EPSG:4326', 'EPSG:27563'))  as aerial_distance;

SELECT station_name, st_point(latitude,longitude), st_point(longitude,latitude)
FROM stations
WHERE st_within(st_point(longitude, latitude), 
  (SELECT  ST_GeomFromWKB(wkb_geometry) FROM st_read('./bordeaux_wine_region.geojson'))
  );

```