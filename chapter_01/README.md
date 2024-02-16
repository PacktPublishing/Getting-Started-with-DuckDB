# Chapter 01

```sql
DROP TABLE IF EXISTS foods;

CREATE TABLE foods (
  food_name VARCHAR PRIMARY KEY, 
  color VARCHAR,
  calories INT, 
  is_healthy BOOLEAN);

INSERT INTO foods (food_name, color, calories, is_healthy) 
VALUES ('apple', 'red', 100, true);

INSERT INTO foods (food_name, color, calories, is_healthy) 
VALUES ('banana', 'yellow', 100, true);

INSERT INTO foods (food_name, color, calories, is_healthy) 
VALUES ('cookie', 'brown', 200, false);

INSERT INTO foods (food_name, color, calories, is_healthy) 
VALUES ('chocolate', 'brown', 150, false);

SELECT * 
FROM foods;
```

```
┌───────────┬─────────┬──────────┬────────────┐
│ food_name │  color  │ calories │ is_healthy │
│  varchar  │ varchar │  int32   │  boolean   │
├───────────┼─────────┼──────────┼────────────┤
│ apple     │ red     │      100 │ true       │
│ banana    │ yellow  │      100 │ true       │
│ cookie    │ brown   │      200 │ false      │
│ chocolate │ brown   │      150 │ false      │
└───────────┴─────────┴──────────┴────────────┘
```

```sql
INSERT INTO foods (food_name, color, calories, is_healthy) 
VALUES ('apple', 'green', 100, true);
```

```
Error: Constraint Error: Duplicate key "food_name: apple" violates primary key constraint
```


```sql
SELECT food_name, color 
FROM foods 
WHERE food_name = 'apple';
```

```
┌───────────┬─────────┐
│ food_name │  color  │
│  varchar  │ varchar │
├───────────┼─────────┤
│ apple     │ red     │
└───────────┴─────────┘
```

```sql
UPDATE foods 
SET color = 'green' 
WHERE food_name = 'apple';

SELECT food_name, color 
FROM foods 
WHERE food_name = 'apple';
```

```
┌───────────┬─────────┐
│ food_name │  color  │
│  varchar  │ varchar │
├───────────┼─────────┤
│ apple     │ green   │
└───────────┴─────────┘
```