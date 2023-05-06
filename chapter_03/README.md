# Chapter 03

-- https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs

```sql

drop table if exists web_log_text;

create table web_log_text (raw_text varchar);

copy web_log_text from './access.log' (delimiter '\n');

select *
from  web_log_text;


drop table if exists web_log_split;

create table web_log_split
as
select regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip, 
  regexp_extract(raw_text, '\[(.*)\]',1 ) as http_date,
  regexp_extract(raw_text, '"([A-Z]*) ',1 ) as http_method,
  regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) as http_lang
from  web_log_text;

select *
from web_log_split;


drop table if exists language_iso;


CREATE TABLE language_iso(lang_iso VARCHAR PRIMARY KEY, language_name VARCHAR);

insert into language_iso
select *
from read_csv('./language_iso.csv', AUTO_DETECT=TRUE, header=True);

select wls.client_ip,
wls.http_date,
wls.http_method,
wls.http_lang,
ln.language_name from web_log_split wls
join language_iso ln on (wls.http_lang = ln.lang_iso);

select wls.client_ip,
wls.http_date,
wls.http_method,
wls.http_lang,
ln.language_name 
from web_log_split wls
left outer join language_iso ln on (wls.http_lang = ln.lang_iso);

select wls.client_ip,
strptime(wls.http_date, '%d/%b/%Y:%H:%M:%S %z') as http_date,
wls.http_method,
wls.http_lang,
ln.language_name 
from web_log_split wls
left outer join language_iso ln on (wls.http_lang = ln.lang_iso);
   
drop view if exists web_log;

create view web_log
as
select wls.client_ip,
strptime(wls.http_date, '%d/%b/%Y:%H:%M:%S %z') as http_date,
wls.http_method,
wls.http_lang,
ln.language_name 
from web_log_split wls
left outer join language_iso ln on (wls.http_lang = ln.lang_iso);

describe web_log;

select *
from web_log;

.read "web_log_script.sql"

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