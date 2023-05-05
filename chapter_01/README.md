# Chapter 01

```sql
drop table if exists foods;

create table foods (
  food_name varchar primary key, 
  color varchar,
  calories int, 
  is_healthy boolean);

insert into foods (food_name, color, calories, is_healthy) values ('apple', 'red', 100, true);

insert into foods (food_name, color, calories, is_healthy) values ('banana', 'yellow', 100, true);
insert into foods (food_name, color, calories, is_healthy) values ('cookie', 'brown', 200, false);
insert into foods (food_name, color, calories, is_healthy) values ('chocolate', 'brown', 150, false);

select * 
from foods;
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
insert into foods (food_name, color, calories, is_healthy) values ('apple', 'green', 100, true);
```

```
Error: Constraint Error: Duplicate key "food_name: apple" violates primary key constraint
```


```sql
select food_name, color 
from foods 
where food_name = 'apple';
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
update foods 
set color = 'green' 
where food_name = 'apple';

select food_name, color 
from foods 
where food_name = 'apple';
```

```
┌───────────┬─────────┐
│ food_name │  color  │
│  varchar  │ varchar │
├───────────┼─────────┤
│ apple     │ green   │
└───────────┴─────────┘
```