
# Chapter 06 - Semi-structured data manipulation

## Technical requirements

```sql
INSTALL 'json';
LOAD 'json';
```

## JSON import 
JSON data: objects, arrays, and schemas
Working with deeply-nested JSON data

```sql
SELECT *
FROM read_json('./media_tv.json',
columns = { 
  "media_type": VARCHAR, 
  "name": VARCHAR, 
  "media_payload": 'STRUCT(
    "type" VARCHAR, 
    "genres" VARCHAR[], 
    "premiered" VARCHAR, 
    "schedule" STRUCT(
      "time" VARCHAR, 
      "days" VARCHAR[]
    )
  )'
}, records='auto', format='newline_delimited');

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

## JSON manipulation
Apply these lessons to raw file

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

```


## Working with inconsistent JSON schemas
```sql

select media_type, json_group_structure(media_payload)
FROM read_json('./media_mixed.json', auto_detect=true, records='true', format='newline_delimited') 
group by media_type;

-- TODO - add example of extracting films

```

## Building JSON

```sql

CREATE OR REPLACE TABLE film_actors AS
SELECT * 
FROM read_csv('film_actors.csv', AUTO_DETECT=TRUE, HEADER=TRUE);

SELECT film_name, actor_name, character_name
FROM film_actors;

SELECT film_name, json_group_array(actor_name) as actor_list
FROM film_actors 
GROUP BY film_name;

SELECT film_name, json_group_object(actor_name, character_name) as actor_character_list
FROM film_actors 
GROUP BY film_name;

-- Automatic struct creation; convert into a single-column struct by SELECTing the table name itself.
SELECT film_actors
FROM film_actors;


```


## Transforming JSON
Transform JSON into the desired structure , both skipping keys plus add new keys.

```sql

select name, json_transform(media_payload, '{"genres":["VARCHAR"],"premiered":"VARCHAR","language":"VARCHAR"}')
from media;

```

## Query JSON from an API

```sql

select *
from read_json('https://api.tvmaze.com/search/shows?q=simpsons', auto_detect=true);

select show.*
from read_json('https://api.tvmaze.com/search/shows?q=simpsons', auto_detect=true);

```

## Summary
