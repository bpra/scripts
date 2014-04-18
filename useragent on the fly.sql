select
distinct http_useragent,
case 
when http_useragent ilike '%select%' or http_useragent ilike '%order by%' or http_useragent like '-%' or http_useragent ilike '%case%when%' then 'Garbage'
when http_useragent ilike '%crawl%' or http_useragent ilike '%spider%' or http_useragent ilike '%bot%' then 'Bot - Self Identified'
when http_useragent ilike '%silk%' then 'Android Kindle'
when (http_useragent ilike '%blackberry%' or http_useragent ilike '%bb10%') and http_useragent not ilike '%playbook%' then 'Blackberry Phone'
when http_useragent ilike '%playbook%' then 'Blackberry Tablet'
when http_useragent ilike '%android%' and http_useragent not ilike '%mobile%' then 'Android Tablet'
when http_useragent ilike '%android%' and http_useragent ilike '%mobile%' then 'Android Phone'
when http_useragent ilike 'RTR iPhone App%' then 'iOS App'
when http_useragent like '%iPad%' then 'iPad'
when http_useragent like '%iPod%' then 'iPod'
when http_useragent like '%iPhone%' and http_useragent <> 'RTR iPhone App 1.0' then 'iPhone'
when http_useragent ilike '%windows%' and (http_useragent ilike '%phone%' or http_useragent ilike '%mobile%'  or http_useragent ilike '%iemobile%')  then 'Windows Phone'
when http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%' then 'Windows Tablet'
when http_useragent ilike '%symbian%' or http_useragent ilike '%symbos%' or http_useragent ilike '%series60%' or http_useragent ilike '%series40%' or http_useragent ilike '%s60%nokia%' or http_useragent ilike '%s40%nokia%' then 'Symbian Phone'
when http_useragent ilike  '%brandingbrand%' then 'Other Mobile'
else 'Desktop'
end as device
from etl.pixel_raw2 
where log_source = 'pixel'
and date(datetime_cst) >= '2013-01-07'


select 'Desktop' as device , 1 as device_id
union all select 'Android Phone' , 10
union all select 'Android Tablet' , 11
union all select 'Android Kindle' , 12
union all select 'iPhone' , 20
union all select 'iPod' , 21
union all select 'iPad' , 22
union all select 'iOS App' , 23
union all select 'Blackberry Phone' , 30
union all select 'Blackberry Tablet' , 31
union all select 'Windows Phone' , 40
union all select 'Windows Tablet' , 41
union all select 'Symbian Phone', 50
union all select 'Other Mobile', 60
union all select 'Bot - Self Identified' , 101
union all select 'Garbage' , 999