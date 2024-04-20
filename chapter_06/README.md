
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

SELECT movie_release_map['Quantum of Solace'][1]
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

SELECT list(actor_name) AS actors
FROM film_actors;

SELECT film_name, list(actor_name) AS actor_name_list
FROM film_actors
GROUP BY film_name
ORDER BY film_name;


SELECT film_name, 
list_slice(list(actor_name), 2, 3) as other_actors
FROM film_actors
GROUP BY film_name
ORDER BY film_name;

SELECT film_name, list(actor_name)[2:3] AS other_actors
FROM film_actors
GROUP BY film_name
ORDER BY film_name;

SELECT list_distinct(list(actor_name)) AS actors
FROM film_actors;

SELECT list_sort(list_distinct(list(actor_name))) AS actors
FROM film_actors;

SELECT list(actor_name).list_distinct().list_sort() AS actors
FROM film_actors;

SELECT
 [actor.split(' ')[-1].lower()
 FOR actor IN list(actor_name)
 ] AS actor_last_name
FROM film_actors;

SELECT
 [actor.upper()
 FOR actor IN list(character_name)
 IF length(actor) > 12
 ] AS long_characters
FROM film_actors;

```





## JSON import 


```sql

SELECT json_object(
 'film_name', film_name,
 'actor_name', actor_name
) AS json_created
FROM film_actors;

SELECT film_name,
 json_group_object(
 actor_name,
 character_name
 ) AS actor_character_json
FROM film_actors
GROUP BY film_name;

SELECT film_actors
FROM film_actors;

SELECT *
FROM read_json('media_tv.json');

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
  }
);


CREATE OR REPLACE TABLE media AS
SELECT *
FROM read_json('./media_tv.json');

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

SELECT name, unnest(genres)
FROM media_extracted;

SELECT media_type, media_payload
FROM read_json('media_mixed.json');

SELECT media_type, json_group_structure(media_payload)
FROM read_json('media_mixed.json')
GROUP BY media_type;

SELECT media_type,
  name,
  media_payload.first_film_screened,
  media_payload.staring
FROM read_json('media_mixed.json')
WHERE media_type = 'film';

SELECT media_type,
  name,
  media_payload.type,
  media_payload.genres,
  media_payload.premiered,
  media_payload.schedule,
FROM read_json('media_mixed.json')
WHERE media_type = 'tv';

```





## Query JSON from an API

```sql

INSTALL httpfs;
LOAD httpfs;

SELECT *
FROM read_json('https://api.tvmaze.com/singlesearch/shows?q=The%20Simpsons');

-- Now we know The Simpsons is coded as show number 83

SELECT season, 
number, 
name, 
json_extract_string(rating, '$.average') as avg_rating, 
summary
FROM read_json('https://api.tvmaze.com/shows/83/episodebynumber?season=34&number=2');

```
