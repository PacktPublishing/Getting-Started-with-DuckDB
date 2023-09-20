
# Chapter 06 - Semi-structured data manipulation


## Data Types


```sql

-- List; int32[]
SELECT [7,8,9] as list_int;

-- List;  varchar[]      
SELECT ['Quantum of Solace', 'Skyfall', 'Spectre', 'No Time to Die'] as list_string;

-- Map; map(varchar, integer) 
CREATE OR REPLACE TABLE movies AS
SELECT map(
  ['Quantum of Solace', 'Skyfall', 'Spectre', 'No Time to Die'],
  [2008,2012,2015,2021]) as movie_release_map;

SELECT movie_release_map
FROM movies;

SELECT movie_release_map['Quantum of Solace']
FROM movies;

-- Struct; mixed data types; struct(movie varchar, release_year integer, box_office decimal(4,1)) 
SELECT {'movie': 'No Time to Die', 
'release_year':2021, 
box_office:771.2} as struct_movie;

-- Struct; nested data types; struct(movie varchar, staring varchar[])
SELECT {'movie': 'No Time to Die', 'staring': ['Daniel Craig', 'Rami Malek', 'Léa Seydoux']} as struct_nested_movie;
```



## Building JSON

```sql

CREATE OR REPLACE TABLE film_actors AS
SELECT * 
FROM read_csv('film_actors.csv', AUTO_DETECT=TRUE, HEADER=TRUE);

SELECT film_name, actor_name, character_name
FROM film_actors;

SELECT json_object('film_name', film_name, 
'actor_name', actor_name) as json_created
FROM film_actors;

SELECT film_actors
FROM film_actors;

SELECT film_name, json_group_array(actor_name) as actor_list
FROM film_actors 
GROUP BY film_name;

SELECT film_name, json_group_object(actor_name, character_name) as actor_character_list
FROM film_actors 
GROUP BY film_name;
```




## Technical requirements

```sql
INSTALL 'json';
LOAD 'json';
```

## JSON import 


```sql
SELECT *
FROM read_json('./media_tv.json',
columns = { 
  "media_type": VARCHAR, 
  "name": VARCHAR, 
  "media_payload": 'STRUCT(
    "type" VARCHAR, 
    "genres" VARCHAR[], 
    "premiered" DATE, 
    "schedule" STRUCT(
      "time" VARCHAR, 
      "days" VARCHAR[]
    )
  )'
}, records='true', format='newline_delimited');

SELECT *
FROM read_json('./media_tv.json', auto_detect=true, format='newline_delimited');

CREATE OR REPLACE TABLE media AS
SELECT *
FROM read_json('./media_tv.json', auto_detect=true, format='newline_delimited');

DESCRIBE media;

SELECT name, media_payload
FROM media;

SELECT name,
media_payload.type,
media_payload.genres,
media_payload.premiered,
media_payload.schedule 
FROM media;


SELECT name, 
media_payload.*
FROM media;
```




## Transforming JSON


```sql
select name, 
json_transform(media_payload, '{"genres":["VARCHAR"],"premiered":"VARCHAR","language":"VARCHAR"}')
from media;

CREATE OR REPLACE TABLE media_extracted AS
SELECT name, 
media_payload.*
FROM media;

SELECT *
FROM media_extracted;

SELECT name, 
genres,
json_array_length(genres) as genres_array_length
FROM media_extracted;

-- Beware the cardinality
SELECT name, 
unnest(genres)
FROM media_extracted;



```

## JSON extraction within a query


-- TODO - store "media_payload" as a JSON string, and emp this is parse at query time rather than load

```sql
-- extracts as  varchar    │    varchar[]     │    date    │ struct("time" varchar, "days" varchar[]) 
SELECT name, 
media_payload.genres[1], 
media_payload.premiered,
media_payload.schedule
FROM read_json('./media_tv.json', auto_detect=true, records='true', format='newline_delimited') ;

-- extracts as varchar    │           json            │             json             │                json
SELECT name, 
media_payload->'genres'->0, 
media_payload->'premiered',
media_payload->'schedule'
FROM read_json('./media_tv.json', auto_detect=true, records='true', format='newline_delimited') ;

-- extracts as  varchar    │                  json                   │                    json                    │                   json             
SELECT name, 
json_extract(media_payload, '$.genres[0]'), 
json_extract(media_payload, '$.premiered'),
json_extract(media_payload, '$.schedule')
FROM read_json('./media_tv.json', auto_detect=true, records='true', format='newline_delimited') ;


-- extracts as varchar    │           varchar            │             varchar             │              varchar  
SELECT name, 
media_payload->>'genres'->0, 
media_payload->>'premiered',
media_payload->>'schedule'
FROM read_json('./media_tv.json', auto_detect=true, records='true', format='newline_delimited') ;

-- extracts as varchar    │                    varchar                     │                      varchar                      │                     varchar       
SELECT name, 
json_extract_string(media_payload, '$.genres[0]'), 
json_extract_string(media_payload, '$.premiered'),
json_extract_string(media_payload, '$.schedule')
FROM read_json('./media_tv.json', auto_detect=true, records='true', format='newline_delimited') ;

-- For the extraction of multiple values from a JSON payload, use a list of paths
WITH extracted_cte AS
(
  SELECT  json_extract(media_payload, [ '$.genres[0]', '$.premiered', '$.schedule' ]) as json_list
  FROM read_json('./media_tv.json', auto_detect=true, records='true', format='newline_delimited') 
)
SELECT json_list[1], json_list[2], json_list[3]
FROM extracted_cte ;

```


## Working with inconsistent JSON schemas
```sql

SELECT media_type, json_group_structure(media_payload)
FROM read_json('./media_mixed.json', auto_detect=true, records='true', format='newline_delimited') 
group by media_type;

-- TODO - add example of extracting films

SELECT media_payload.*
FROM read_json('./media_mixed.json', auto_detect=true, records='true', format='newline_delimited') 
WHERE media_type = 'tv';

SELECT media_payload.*
FROM read_json('./media_mixed.json', auto_detect=true, records='true', format='newline_delimited') 
WHERE media_type = 'film';


SELECT media_type, name, media_payload.*
FROM read_json('./media_mixed.json',
columns = { 
  "media_type": VARCHAR, 
  "name": VARCHAR, 
  "media_payload": 'STRUCT(
    "type" VARCHAR, 
    "genres" VARCHAR[], 
    "premiered" DATE, 
    "schedule" STRUCT(
      "time" VARCHAR, 
      "days" VARCHAR[]
    )
  )'
}, records='auto', format='newline_delimited')
WHERE media_type = 'tv';

SELECT media_type, name, media_payload.*
FROM read_json('./media_mixed.json',
columns = { 
  "media_type": VARCHAR, 
  "name": VARCHAR, 
  "media_payload": 'STRUCT(
    "first_film_screened" DATE, 
    "staring" VARCHAR[]
  )'
}, records='auto', format='newline_delimited')
WHERE media_type = 'film';

```





## Query JSON from an API

```sql

select *
from read_json('https://api.tvmaze.com/singlesearch/shows?q=The%20Simpsons', auto_detect=true, format='newline_delimited');

-- Now we know The Simpsons is coded as show number 83
select *
from read_json('https://api.tvmaze.com/shows/83/seasons', auto_detect=true);

-- Se can look at the episode list of season 34 of "The Simpsons"
select season, number, name, rating, summary
from read_json('https://api.tvmaze.com/seasons/124283/episodes', auto_detect=true);

select season, number, name, rating, summary, airstamp
from read_json('https://api.tvmaze.com/shows/83/episodebynumber?season=34&number=1', auto_detect=true, format='newline_delimited');

```

## Summary
