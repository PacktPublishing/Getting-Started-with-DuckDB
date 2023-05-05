# Chapter 03

-- https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs

```sql

create table web_log_tmp (raw_text varchar);

copy web_log_tmp from './access.log' (delimiter '\n');

select regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip, 
  strptime(regexp_extract(raw_text, '\[(.*)\]',1 ), '%d/%b/%Y:%H:%M:%S %z') as http_date,
  regexp_extract(raw_text, '"([A-Z]*) ',1 ) as http_method
from  web_log_tmp;
```

- build as steps into a script
- various ways to run a script
- export at the end

# Play below

```sql
select  (substr(raw_text,strpos(raw_text,'[')+1,20) ) from web_log_tmp;

create table accesslog as

(select m[1] as clientip
    from
(select 
  regexp_matches(raw_text, E'(.*) (.*) (.*) \\[(.*)\\] "(.*)" (\\d+) (\\d+)') as m 
   from  web_log_tmp) s);


(select m[1] as clientip,
  (to_char(to_timestamp(m[4], 'DD/Mon/YYYY:HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS ')
        || split_part(m[4], ' ',2))::timestamp with time zone as "time",
  split_part(m[5], ' ', 1) as method,
  split_part(split_part(m[5], ' ', 2), '?', 1) as uri,
  split_part(split_part(m[5], ' ', 2), '?', 2) as query,
  m[6]::smallint as status,
  m[7]::bigint bytes
    from
(select 
  regexp_extract(raw_text, E'(.*) (.*) (.*) \\[(.*)\\] "(.*)" (\\d+) (\\d+)') as m 
   from  web_log_tmp) s);