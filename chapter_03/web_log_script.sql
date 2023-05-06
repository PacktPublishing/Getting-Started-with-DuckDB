drop table if exists web_log_text;

create table web_log_text (raw_text varchar);

copy web_log_text from './access.log' (delimiter '\n');


drop table if exists web_log_split;

create table web_log_split
as
select regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip, 
  regexp_extract(raw_text, '\[(.*)\]',1 ) as http_date,
  regexp_extract(raw_text, '"([A-Z]*) ',1 ) as http_method,
  regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) as http_lang
from  web_log_text;


drop table if exists language_iso;

CREATE TABLE language_iso(lang_iso VARCHAR PRIMARY KEY, language_name VARCHAR);

insert into language_iso
select *
from read_csv('./language_iso.csv', AUTO_DETECT=TRUE, header=True);

   
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