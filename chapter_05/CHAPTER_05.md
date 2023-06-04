# Chapter 05

## Full-Text Search Indexes

```sql

SELECT * 
FROM duckdb_extensions();

CREATE OR REPLACE TABLE book_reviews
AS
SELECT *
FROM read_parquet('../chapter_04/reviews_original.parquet');

INSTALL fts; 
LOAD fts;

PRAGMA create_fts_index('book_reviews', 'review_id', 'product_title', 'review_headline', overwrite='TRUE');

WITH cte AS
(
    SELECT *, fts_main_book_reviews.match_bm25(review_id, 'travel france wine') AS match_score
    FROM book_reviews
)
SELECT product_title, review_headline, star_rating, match_score
FROM cte
WHERE match_score IS NOT NULL
ORDER BY match_score DESC
LIMIT 10;
```

## Geospatial extension

```sql

INSTALL spatial; 
LOAD spatial; 

CREATE OR REPLACE TABLE stations AS
SELECT * 
FROM st_read('stations.xlsx', layer='stations');

SELECT station_name, st_point(latitude,longitude), st_point(longitude,latitude)
FROM stations
WHERE st_within(st_point(longitude, latitude), 
  (SELECT  ST_GeomFromWKB(wkb_geometry) FROM st_read('./bordeaux_wine_region.geojson'))
  );

```