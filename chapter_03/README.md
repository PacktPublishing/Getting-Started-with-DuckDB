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

select  time_bucket(interval '1 day', http_date), count(*)
from web_log
group by 1
order by 2 desc;

```

# TO DO


- build as steps into a script
- various ways to run a script
- export at the end


# Read lastest Redit Joke
- https://www.reddit.com/r/funny/

```sql
select json_extract(json, '$.data.children[2].data.title')
from read_ndjson_objects('https://www.reddit.com/r/funny.json');
```

# IGNORE BELOW
# PLAY File create

```sql

select json_extract(data, '$.data.children[5].data.title')
from read_json('https://www.reddit.com/r/funny.json', auto_detect=true);


INSTALL json;
LOAD json;


select data->'$'
from (read_json('./archive/funny.json', auto_detect=true));


select json->'$.data.children[19].data.title'
from read_ndjson_objects('./archive/funny.json');

select json_extract(json, '$.data.children[0].data.title')
from read_ndjson_objects('https://www.reddit.com/r/funny.json');



select json->'$.data.children[1].data.title'
from read_ndjson_objects('https://www.reddit.com/r/dataengineering.json');


describe from './archive/funny.json' limit 1;


select * 
from './archive/funny.json' limit 1;

select unnest(data)
from './archive/funny.json' limit 1;


describe select *
from
(
select json_extract(json, '$.data.children') as x
from read_ndjson_objects('./archive/funny.json')
);

with cte as (
  select json_extract(json, '$.data.children') x
  from read_json_auto('./archive/funny.json')
)
select json(x)
from cte;

 SELECT json_group_structure(data)
FROM read_json('./archive/funny.json', auto_detect=true, json_format='records')
limit 1;

.mode lines

.mode duckbox

SELECT typeof(json_transform('{}', '
{
  "after": "VARCHAR",
  "dist": "UBIGINT",
  "modhash": "VARCHAR",
  "geo_filter": "NULL",
  "children": [
    {
      "kind": "VARCHAR",
      "data": {
        "approved_at_utc": "NULL",
        "subreddit": "VARCHAR",
        "selftext": "VARCHAR",
        "author_fullname": "VARCHAR",
        "saved": "BOOLEAN",
        "mod_reason_title": "NULL",
        "gilded": "UBIGINT",
        "clicked": "BOOLEAN",
        "title": "VARCHAR",
        "link_flair_richtext": [
          "NULL"
        ],
        "subreddit_name_prefixed": "VARCHAR",
        "hidden": "BOOLEAN",
        "pwls": "UBIGINT",
        "link_flair_css_class": "NULL",
        "downs": "UBIGINT",
        "thumbnail_height": "UBIGINT",
        "top_awarded_type": "NULL",
        "hide_score": "BOOLEAN",
        "name": "VARCHAR",
        "quarantine": "BOOLEAN",
        "link_flair_text_color": "VARCHAR",
        "upvote_ratio": "DOUBLE",
        "author_flair_background_color": "VARCHAR",
        "subreddit_type": "VARCHAR",
        "ups": "UBIGINT",
        "total_awards_received": "UBIGINT",
        "media_embed": "JSON",
        "thumbnail_width": "UBIGINT",
        "author_flair_template_id": "NULL",
        "is_original_content": "BOOLEAN",
        "user_reports": [
          "NULL"
        ],
        "secure_media": {
          "reddit_video": {
            "bitrate_kbps": "UBIGINT",
            "fallback_url": "VARCHAR",
            "height": "UBIGINT",
            "width": "UBIGINT",
            "scrubber_media_url": "VARCHAR",
            "dash_url": "VARCHAR",
            "duration": "UBIGINT",
            "hls_url": "VARCHAR",
            "is_gif": "BOOLEAN",
            "transcoding_status": "VARCHAR"
          }
        },
        "is_reddit_media_domain": "BOOLEAN",
        "is_meta": "BOOLEAN",
        "category": "NULL",
        "secure_media_embed": "JSON",
        "link_flair_text": "NULL",
        "can_mod_post": "BOOLEAN",
        "score": "UBIGINT",
        "approved_by": "NULL",
        "is_created_from_ads_ui": "BOOLEAN",
        "author_premium": "BOOLEAN",
        "thumbnail": "VARCHAR",
        "edited": "BOOLEAN",
        "author_flair_css_class": "NULL",
        "author_flair_richtext": [
          "NULL"
        ],
        "gildings": "JSON",
        "content_categories": "NULL",
        "is_self": "BOOLEAN",
        "mod_note": "NULL",
        "created": "DOUBLE",
        "link_flair_type": "VARCHAR",
        "wls": "UBIGINT",
        "removed_by_category": "NULL",
        "banned_by": "NULL",
        "author_flair_type": "VARCHAR",
        "domain": "VARCHAR",
        "allow_live_comments": "BOOLEAN",
        "selftext_html": "VARCHAR",
        "likes": "NULL",
        "suggested_sort": "NULL",
        "banned_at_utc": "NULL",
        "view_count": "NULL",
        "archived": "BOOLEAN",
        "no_follow": "BOOLEAN",
        "is_crosspostable": "BOOLEAN",
        "pinned": "BOOLEAN",
        "over_18": "BOOLEAN",
        "all_awardings": [
          {
            "giver_coin_reward": "NULL",
            "subreddit_id": "NULL",
            "is_new": "BOOLEAN",
            "days_of_drip_extension": "NULL",
            "coin_price": "UBIGINT",
            "id": "VARCHAR",
            "penny_donate": "NULL",
            "award_sub_type": "VARCHAR",
            "coin_reward": "UBIGINT",
            "icon_url": "VARCHAR",
            "days_of_premium": "NULL",
            "tiers_by_required_awardings": "NULL",
            "resized_icons": [
              {
                "url": "VARCHAR",
                "width": "UBIGINT",
                "height": "UBIGINT"
              }
            ],
            "icon_width": "UBIGINT",
            "static_icon_width": "UBIGINT",
            "start_date": "NULL",
            "is_enabled": "BOOLEAN",
            "awardings_required_to_grant_benefits": "NULL",
            "description": "VARCHAR",
            "end_date": "NULL",
            "sticky_duration_seconds": "NULL",
            "subreddit_coin_reward": "UBIGINT",
            "count": "UBIGINT",
            "static_icon_height": "UBIGINT",
            "name": "VARCHAR",
            "resized_static_icons": [
              {
                "url": "VARCHAR",
                "width": "UBIGINT",
                "height": "UBIGINT"
              }
            ],
            "icon_format": "VARCHAR",
            "icon_height": "UBIGINT",
            "penny_price": "UBIGINT",
            "award_type": "VARCHAR",
            "static_icon_url": "VARCHAR"
          }
        ],
        "awarders": [
          "NULL"
        ],
        "media_only": "BOOLEAN",
        "can_gild": "BOOLEAN",
        "spoiler": "BOOLEAN",
        "locked": "BOOLEAN",
        "author_flair_text": "VARCHAR",
        "treatment_tags": [
          "NULL"
        ],
        "visited": "BOOLEAN",
        "removed_by": "NULL",
        "num_reports": "NULL",
        "distinguished": "VARCHAR",
        "subreddit_id": "VARCHAR",
        "author_is_blocked": "BOOLEAN",
        "mod_reason_by": "NULL",
        "removal_reason": "NULL",
        "link_flair_background_color": "VARCHAR",
        "id": "VARCHAR",
        "is_robot_indexable": "BOOLEAN",
        "report_reasons": "NULL",
        "author": "VARCHAR",
        "discussion_type": "NULL",
        "num_comments": "UBIGINT",
        "send_replies": "BOOLEAN",
        "whitelist_status": "VARCHAR",
        "contest_mode": "BOOLEAN",
        "mod_reports": [
          "NULL"
        ],
        "author_patreon_flair": "BOOLEAN",
        "author_flair_text_color": "VARCHAR",
        "permalink": "VARCHAR",
        "parent_whitelist_status": "VARCHAR",
        "stickied": "BOOLEAN",
        "url": "VARCHAR",
        "subreddit_subscribers": "UBIGINT",
        "created_utc": "DOUBLE",
        "num_crossposts": "UBIGINT",
        "media": {
          "reddit_video": {
            "bitrate_kbps": "UBIGINT",
            "fallback_url": "VARCHAR",
            "height": "UBIGINT",
            "width": "UBIGINT",
            "scrubber_media_url": "VARCHAR",
            "dash_url": "VARCHAR",
            "duration": "UBIGINT",
            "hls_url": "VARCHAR",
            "is_gif": "BOOLEAN",
            "transcoding_status": "VARCHAR"
          }
        },
        "is_video": "BOOLEAN",
        "url_overridden_by_dest": "VARCHAR",
        "post_hint": "VARCHAR",
        "preview": {
          "images": [
            {
              "source": {
                "url": "VARCHAR",
                "width": "UBIGINT",
                "height": "UBIGINT"
              },
              "resolutions": [
                {
                  "url": "VARCHAR",
                  "width": "UBIGINT",
                  "height": "UBIGINT"
                }
              ],
              "variants": {
                "obfuscated": {
                  "source": {
                    "url": "VARCHAR",
                    "width": "UBIGINT",
                    "height": "UBIGINT"
                  },
                  "resolutions": [
                    {
                      "url": "VARCHAR",
                      "width": "UBIGINT",
                      "height": "UBIGINT"
                    }
                  ]
                },
                "nsfw": {
                  "source": {
                    "url": "VARCHAR",
                    "width": "UBIGINT",
                    "height": "UBIGINT"
                  },
                  "resolutions": [
                    {
                      "url": "VARCHAR",
                      "width": "UBIGINT",
                      "height": "UBIGINT"
                    }
                  ]
                }
              },
              "id": "VARCHAR"
            }
          ],
          "enabled": "BOOLEAN"
        }
      }
    }
  ],
  "before": "NULL"
}'));




select json_extract(data, '.')
from
(SELECT json_group_structure(json) as data
FROM (select *
from read_ndjson_objects('https://www.reddit.com/r/funny.json')
)
);

select json_extract(data, '$.data.children')

select (data->'$.data.children')
from
(SELECT json_group_structure(json) as data
FROM (select *
from read_ndjson_objects('./archive/funny.json')
)
);

$.data.children[19].data.title



select *
from read_csv('https://raw.githubusercontent.com/PacktPublishing/Getting-started-with-DuckDB/main/chapter_02/food_orders.csv', AUTO_DETECT=TRUE);


select *
from read_csv('s3://duckdbdata/access.log', AUTO_DETECT=TRUE);





drop table if exists web_log_text;

create table web_log_text (raw_text varchar);

copy web_log_text from './archive/access.log' (delimiter '\n');

select strptime(http_date, '%d/%b/%Y:%H:%M:%S %z') as http_date

copy
(
select raw_text
--, strptime(http_date, '%d/%b/%Y:%H:%M:%S %z') as http_date
from
(
select regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip, 
  regexp_extract(raw_text, '\[(.*)\]',1 ) as http_date,
  regexp_extract(raw_text, '"([A-Z]*) ',1 ) as http_method,
  regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) as http_lang,
  raw_text
from  
(
select 
regexp_replace(
  regexp_replace(raw_text, 'http[s]://[^"]*', 'https://www.example.com')
  , '/2019', '/2023') as raw_text
from  web_log_text
where raw_text not like '%formatJalaliDate%'
and raw_text not like '%`%'
and raw_text not like '%|%'
)
where length(http_date)=26)
) to 'demo.log' (DELIMITER '|');
;

select regexp_extract(raw_text, '^[0-9\.]*' ) as client_ip, 
  regexp_extract(raw_text, '\[(.*)\]',1 ) as http_date,
  regexp_extract(raw_text, '"([A-Z]*) ',1 ) as http_method,
  regexp_extract(raw_text, '([a-zA-Z\-]*)"$', 1) as http_lang
from  web_log_text;


```

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