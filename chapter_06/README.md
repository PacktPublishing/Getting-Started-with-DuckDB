
# Chapter 06 - Semi-structured data manipulation

## Licensing
- The [Query JSON from an API](#query-json-from-an-api) section makes use of the [TVmaze API](https://www.tvmaze.com/api) which is licensed by [CC BY-SA](http://creativecommons.org/licenses/by-sa/4.0/). 
- All other data sets used in this chapter were created by the author


## Technical requirements

```sql
INSTALL json;
LOAD json;
```

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

-- Struct; nested data types; struct(movie varchar, starring varchar[])
SELECT {'movie': 'No Time to Die', 'starring': ['Daniel Craig', 'Rami Malek', 'Léa Seydoux']} as struct_nested_movie;
```



## Building JSON

```sql

CREATE OR REPLACE TABLE film_actors AS
SELECT * 
FROM read_csv('film_actors.csv', AUTO_DETECT=TRUE, HEADER=TRUE);

SELECT film_name, actor_name, character_name
FROM film_actors;

SELECT json_object(
  'film_name', film_name, 
  'actor_name', actor_name
) as json_created
FROM film_actors;

SELECT film_actors
FROM film_actors;


SELECT film_name, 
list(actor_name) AS actor_name_list
FROM film_actors
GROUP BY film_name
ORDER BY film_name;


SELECT film_name, 
list_slice(list(actor_name), 2, 3) as other_actors
FROM film_actors
GROUP BY film_name
ORDER BY film_name;


SELECT film_name, 
json_group_object(actor_name, character_name) as actor_character_json
FROM film_actors 
GROUP BY film_name;
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

CREATE OR REPLACE TABLE media_extracted AS
SELECT name, 
media_payload.*
FROM media;

SELECT *
FROM media_extracted;

SELECT name, 
unnest(genres)
FROM media_extracted;

select name, 
json_transform(media_payload, '{"genres":["VARCHAR"],"premiered":"VARCHAR","language":"VARCHAR"}')  AS transformed_json
from media;

```


## Performance

```sql

CREATE OR REPLACE TABLE media AS
SELECT *
FROM read_json('./media_tv.json', auto_detect=true, format='newline_delimited');

SELECT json_extract_string(media_payload, '$.genres[0]') AS by_json_extract_string,
media_payload.genres[1] AS by_struct_access
FROM media m; 


.timer on

SELECT max(json_extract_string(media_payload, '$.genres[0]')) AS by_json_extract_string
FROM media m 
CROSS JOIN generate_series(1, 50000000); 

SELECT max(media_payload.genres[1]) AS by_struct_access
FROM media m 
CROSS JOIN generate_series(1, 50000000); 

.timer off
```




## Working with inconsistent JSON schemas
```sql

SELECT media_type, media_payload
FROM read_json('./media_mixed.json', auto_detect=true, records='true', format='newline_delimited');

SELECT media_type, json_group_structure(media_payload)
FROM read_json('./media_mixed.json', auto_detect=true, records='true', format='newline_delimited') 
group by media_type;

SELECT media_payload.*
FROM read_json('./media_mixed.json', auto_detect=true, records='true', format='newline_delimited') 
WHERE media_type = 'tv';

SELECT media_payload.*
FROM read_json('./media_mixed.json', auto_detect=true, records='true', format='newline_delimited') 
WHERE media_type = 'film';

CREATE OR REPLACE TABLE films AS
SELECT name, media_payload.starring
FROM read_json('./media_mixed.json',
columns = { 
  "media_type": VARCHAR, 
  "name": VARCHAR, 
  "media_payload": 'STRUCT(
    "first_film_screened" DATE, 
    "starring" VARCHAR[]
  )'
}, records='auto', format='newline_delimited')
WHERE media_type = 'film';

SELECT *
FROM films;

SELECT name, 
unnest(starring) as actor_name
FROM films;

SELECT media_type, name, media_payload.*
FROM read_json('./media_mixed.json',
columns = { 
  "media_type": VARCHAR, 
  "name": VARCHAR, 
  "media_payload": 'STRUCT(
    "first_film_screened" DATE, 
    "starring" VARCHAR[]
  )'
}, records='auto', format='newline_delimited')
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
```





## Query JSON from an API

```sql

INSTALL httpfs;
LOAD httpfs;

SELECT *
FROM read_json('https://api.tvmaze.com/singlesearch/shows?q=The%20Simpsons', 
auto_detect=true, format='newline_delimited');

-- Now we know The Simpsons is coded as show number 83

SELECT season, 
number, 
name, 
json_extract_string(rating, '$.average') as avg_rating, 
summary
FROM read_json('https://api.tvmaze.com/shows/83/episodebynumber?season=34&number=2', auto_detect=true, format='newline_delimited');

```
