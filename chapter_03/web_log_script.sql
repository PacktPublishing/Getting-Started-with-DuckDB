CREATE OR REPLACE TABLE web_log_text (raw_text VARCHAR);

COPY web_log_text 
FROM './access.log' (delimiter '');

CREATE OR REPLACE TABLE web_log_split
AS
SELECT regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip, 
  regexp_extract(raw_text, '\[(.*)\]',1 ) as http_date_text,
  regexp_extract(raw_text, '"([A-Z]*) ',1 ) as http_method,
  regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) as http_lang
FROM  web_log_text;


CREATE OR REPLACE TABLE language_iso(
  lang_iso VARCHAR PRIMARY KEY, 
  language_name VARCHAR);

INSERT INTO language_iso
SELECT *
FROM read_csv('./language_iso.csv', AUTO_DETECT=TRUE, header=True);

CREATE OR REPLACE VIEW web_log_view
AS
SELECT wls.client_ip,
strptime(wls.http_date_text, '%d/%b/%Y:%H:%M:%S %z') as http_date,
wls.http_method,
wls.http_lang,
ln.language_name 
FROM web_log_split wls
LEFT OUTER JOIN language_iso ln on (wls.http_lang = ln.lang_iso);

