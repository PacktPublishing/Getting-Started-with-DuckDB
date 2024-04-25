# Chapter 10 - Effective DuckDB usage 

# Effective column selection

```sql
CREATE OR REPLACE TABLE skiers AS
SELECT * 
FROM read_csv('skiers.csv', AUTO_DETECT=TRUE);

SELECT *
FROM skiers;

SELECT skier_first_name,
skier_last_name,
skier_age,
skier_height,
skier_helmet_color,
skier_bib_color,
FROM skiers;

SELECT s.* EXCLUDE (skier_age, skier_height)
FROM skiers s;

SELECT s.* REPLACE (round(skier_age/10)*10)::integer as skier_age
FROM skiers s;

SELECT skier_first_name, COLUMNS('.*color$')
FROM skiers;

SELECT skier_first_name, COLUMNS('.*color.*')
FROM skiers
WHERE COLUMNS('.*color.*') = 'yellow';
```

# Function chaining 

```sql

SELECT skier_first_name, skier_last_name
FROM skiers;

-- Let's Concatinate, uppercase, left-pad
SELECT CONCAT(
  LPAD(
    UPPER(
      CONCAT_WS(' ', skier_first_name, skier_last_name))
      , 20
    , '>')
  , '.') as skier_name
FROM skiers;

SELECT CONCAT_WS(' ', skier_first_name, skier_last_name)
.UPPER()
.LPAD(20, '>')
.CONCAT('.') as skier_name
FROM skiers;
```
# BY NAME
```sql
CREATE UNIQUE INDEX skier_unique 
ON skiers (skier_first_name);

INSERT INTO skiers
SELECT 'Kim' as skier_first_name, 
'blue' as skier_helmet_color;

INSERT INTO skiers(skier_first_name, skier_helmet_color)
SELECT 'Kim' as skier_first_name, 
'blue' as skier_helmet_color;

INSERT INTO skiers BY NAME 
SELECT 'green' as skier_helmet_color, 
'red' as skier_bib_color,
'Liam' as skier_first_name;

SELECT skier_first_name, 
skier_helmet_color, 
skier_bib_color
FROM skiers
WHERE skier_first_name='Liam';

INSERT OR REPLACE 
INTO skiers BY NAME 
SELECT 'Liam' as skier_first_name, 
'black' as skier_helmet_color;

SELECT skier_first_name, 
skier_helmet_color, 
skier_bib_color
FROM skiers
WHERE skier_first_name='Liam';
```


# Positional joins

```sql
CREATE OR REPLACE TABLE skiers AS
SELECT * 
FROM read_csv('skiers.csv', AUTO_DETECT=TRUE);

CREATE OR REPLACE TABLE scores AS
SELECT * 
FROM read_csv('skier_scores.csv', AUTO_DETECT=TRUE);

SELECT df1.skier_first_name, df1.skier_last_name, df2.score 
FROM skiers df1 POSITIONAL JOIN scores df2;
```

# ASOF

```sql
CREATE OR REPLACE TABLE weather AS
SELECT * 
FROM read_csv('weather.csv', AUTO_DETECT=TRUE, TIMESTAMPFORMAT='%Y-%m-%d %H:%M:%S');

SELECT *
FROM weather
LIMIT 10;

-- what was the weather for the first skier at 2023-12-01 10:01:00
WITH weather_cte AS
(
  SELECT measurment_time, 
  wind_speed, 
  temp,
  LEAD(measurment_time, 1) OVER (ORDER BY measurment_time) AS measurment_end
  FROM weather
  ORDER BY measurment_time
)
SELECT *
FROM weather_cte
WHERE TIMESTAMP '2023-12-01 10:01:00' BETWEEN measurment_time AND measurment_end;


SELECT *
FROM scores s ASOF JOIN weather w
ON s.score_time >= w.measurment_time ;

SELECT s.*, bar(w.wind_speed, 0, 20, 20) as wind_bar_plot, w.wind_speed
FROM scores s ASOF JOIN weather w
ON s.score_time >= w.measurment_time ;
```

## Recursive queries and macros

```sql
CREATE OR REPLACE TABLE wines AS
SELECT *
FROM read_csv('wines.csv', auto_detect=true);

SELECT *
FROM wines
ORDER BY wine_id;

WITH RECURSIVE wine_hierarchy(wine_id, start_with, wine_path) AS 
(
  SELECT wine_id, wine_name, [wine_name] AS wine_path
  FROM wines
  WHERE sub_class_of IS NULL
  UNION ALL
  SELECT wines.wine_id,
      wines.wine_name,
      list_prepend(wines.wine_name, wine_hierarchy.wine_path)
  FROM wines, wine_hierarchy
  WHERE wines.sub_class_of = wine_hierarchy.wine_id
)
SELECT wine_path
FROM wine_hierarchy
WHERE start_with = 'Rothschild';


-- macros
CREATE OR REPLACE TABLE wine_prices AS
SELECT *
FROM read_csv('wine_prices.csv', auto_detect=true);

SELECT wine_name, price, capacity_ml
FROM wine_prices;

CREATE OR REPLACE MACRO unit_price(price, capacity) AS round(price/capacity, 3);

SELECT wine_name, 
    price,
    capacity_ml,
    unit_price(price, capacity_ml) AS price_ml
FROM wine_prices;

```

