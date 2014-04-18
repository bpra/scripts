

select 'hello'||'sir'
select '/var/data/pixel/'||date_part('year',timestampadd("d",-1,getdate()))||date_part('month',timestampadd("d",-1,getdate()))||date_part('day',timestampadd("d",-1,getdate()))

\set date concat(concat(concat(concat('/var/data/pixel/',date_part('year',timestampadd("d",-1,getdate()))),date_part('month',timestampadd("d",-1,getdate()))),date_part('day',timestampadd("d",-1,getdate()))),'%')
select * from etl.pixel_raw2 
where 1=1
and log_file >= :date
and log_source = 'pixel'
limit 1000
select * from rtrbi.devices 

select
max(asofdate_est)
--*
from rtrbi.daily_user_funnels where device_id = 23 
--and asofdate_est = ''
limit 10

drop table etl.unit owner;
alter schema etl.unit owner to etl;
select * from 
create table 

update etl.z_agenttodevicemapping
set device = 'Garbage'
where http_useragent ilike '%select%' or http_useragent ilike '%order by%' or http_useragent like '-%' or http_useragent ilike '%case%when%'



update etl.z_agenttodevicemapping
set device = 'Blackberry Phone'
where http_useragent ilike '%blackberry%' and http_useragent not ilike '%playbook%' 

update etl.z_agenttodevicemapping
set device = 'Blackberry Tablet'
where http_useragent ilike '%playbook%' 

select * from etl.z_agenttodevicemapping
where http_useragent ilike '%crawl%' or http_useragent ilike '%spider%' or http_useragent ilike '%bot%'

select * from etl.z_agenttodevicemapping

where http_useragent like '%iPad%' 

select * from etl.z_agenttodevicemapping
where http_useragent like '%iPod%' and device = 'iOS Phone'

update etl.agent2device
set device = 'Windows Phone',
 device_id = 40
--select * from etl.agent2device
where http_useragent ilike '%windows%' and http_useragent ilike '%mobile%' and device_id = 1

update etl.agent2device
set device = 'Symbian Phone',
 device_id = 50
 --select * from etl.agent2device
where (http_useragent ilike '%symbian%' or http_useragent ilike '%symbos%' or http_useragent ilike '%series60%' or http_useragent ilike '%series40%' or http_useragent ilike '%s60%nokia%' or http_useragent ilike '%s40%nokia%') and device_id = 1 

update etl.agent2device
set device = 'Android Kindle Silk',
 device_id = 60
 --select * from etl.agent2device
where (http_useragent ilike '%silk%')  

update etl.agent2device
set device = 'Other Mobile',
device_id = 90
--select * from etl.agent2device
where http_useragent ilike '%brandingbrand%' and device_id = 1

update etl.agent2device
set device = 'Blackberry Phone' ,device_id = 30
--select * from etl.agent2device
where http_useragent ilike '%bb10%' and device_id = 1

select distinct device,device_id from etl.agent2device order by device_id limit 1000
create table etl.agent2device
as
select 
http_useragent
,device
, case 
when device =  'Web Portal' then 1
when device = 'Android Phone' then 10
when device = 'Android Tablet' then 11
when device = 'iOS Phone' then 20
when device = 'iOS IPod' then 21
when device = 'iOS App' then 22
when device = 'iOS Tablet' then 23
when device = 'Blackberry Phone' then 30
when device = 'Blackberry Tablet' then 31
when device = 'Windows Phone' then 40
when device = 'Windows Tablet' then 41
when device = 'Bot - Self Identified' then 101
when device = 'Garbage' then 999
end as device_id
from etl.z_agenttodevicemapping 
where http_useragent ilike '%ipod%' and device = 'iOS Phone'



when then 

select * from etl.z_agenttodevicemapping where device ilike '%android%'
select * from etl.z_agenttodevicemapping where device ilike '%ios app%'
select * from etl.z_agenttodevicemapping where device ilike '%blackberry%'
select * from etl.z_agenttodevicemapping where device ilike '%windows%'


explain select 
distinct a.device,
is_mobile
from etl.pixel_raw2 r
inner join etl.agent2device a
on r.http_useragent = a.http_useragent
where log_source = 'pixel'
and date(datetime_cst) >= '2013-01-07'
and a.device_id <= 100 
group by 1,2
SELECT * FROM V_MONITOR.DESIGN_STATUS;



select ANALYZE_STATISTICS('etl.agent2device');
as
select
distinct http_useragent,
case 
when http_useragent ilike '%bot%' then 'Bot - Self Identified'
when http_useragent ilike '%android%' and http_useragent not ilike '%mobile%' then 'Android Tablet'
when http_useragent ilike '%android%' and http_useragent ilike '%mobile%' then 'Android Phone'
when http_useragent ilike '%ipad%' then 'iOS Tablet'
when http_useragent ilike '%ipod%' then 'iOS IPod'
when http_useragent ilike '%iphone%' and http_useragent <> 'RTR iPhone App 1.0' then 'iOS Phone'
when http_useragent = 'RTR iPhone App 1.0' then 'iOS App'
when http_useragent ilike '%blackberry%' and not ilike '%playbook%' then 'Blackberry Phone'
when http_useragent ilike '%playbook%' then 'Blackberry Tablet'
when http_useragent ilike '%windows%' and http_useragent ilike '%phone%' then 'Windows Phone'
when http_useragent ilike '%windows%' and http_useragent ilike '%mobile%' then 'Windows Phone'
when http_useragent ilike '%symbian%' or http_useragent ilike '%symbos%' or http_useragent ilike '%series60%' then 'Symbian Phone'
when http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%' then 'Windows Tablet'
when http_useragent ilike '%bot%' then 'Bot - Self Identified'
else 'Web Portal'
end as device
from etl.pixel_raw2 
where log_source = 'pixel'
and date(datetime_cst) >= '2013-01-07'

select * from schemata

select 
case 
when http_useragent ilike '%android%' and http_useragent not ilike '%mobile%' then 'Android Tablet'
when http_useragent ilike '%android%' and http_useragent ilike '%mobile%' then 'Android Phone'
when http_useragent ilike '%iphone%' and http_useragent <> 'RTR iPhone App 1.0' then 'iOS Phone'
when http_useragent = 'RTR iPhone App 1.0' then 'iOS App'
when http_useragent ilike '%ipad%' then 'iOS Tablet'
when http_useragent ilike '%windows%' and http_useragent ilike '%phone%' then 'Windows Phone'
when http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%' then 'Windows Tablet'
else 'Web Portal'
end as device,
is_mobile,
count(distinct uid),
count(distinct browser_id)
from etl.pixel_raw2 
where log_source = 'pixel'
and http_useragent not ilike '%bot%'
group by 1,2
limit 10
 where http_useragent in 
select * from etl.z_UAstrings where http_useragent like '%android%' and http_useragent not ilike '%mobile%';
select * from etl.z_UAstrings where http_useragent like '%android%' and http_useragent ilike '%mobile%';
select * from etl.z_UAstrings where http_useragent like '%iphone%';
select * from etl.z_UAstrings where http_useragent like '%ipad%';
select * from etl.z_UAstrings where http_useragent like '%blackberry%';
select * from etl.z_UAstrings where http_useragent like '%ipod%';
select * from etl.z_UAstrings where http_useragent like '%windows phone%';
select * from etl.z_UAstrings where http_useragent like '%iemobile%';

select * from etl.pixel_raw2 where log_file like '%postq%' limit 10
select dbd_cancel_populate_design('useragentmapping');
select dbd_drop_all_workspaces(); 
select * from tables where table_name like 'v_dbd_%'

select * from rtrbi.devices order by device_id limit 1000


CREATE PROJECTION devices_id (
 device ENCODING AUTO,
 device_id ENCODING AUTO
)
AS
 SELECT device,
        device_id
 FROM rtrbi.devices
 ORDER BY device_id
UNSEGMENTED ALL NODES;
select refresh('rtrbi.devices'); 
select *,
case 
when http_useragent ilike '%select%' or http_useragent ilike '%order by%' or http_useragent like '-%' or http_useragent ilike '%case%when%' then 'Garbage'
when http_useragent ilike '%crawl%' or http_useragent ilike '%spider%' or http_useragent ilike '%bot%' then 'Bot - Self Identified'
when http_useragent ilike '%silk%' then 'Android Kindle'
when http_useragent ilike '%android%' and http_useragent not ilike '%mobile%' then 'Android Tablet'
when http_useragent ilike '%android%' and http_useragent ilike '%mobile%' then 'Android Phone'
when http_useragent like '%iPad%' then 'iPad'
when http_useragent like '%iPod%' then 'iPod'
when http_useragent like '%iPhone%' and http_useragent <> 'RTR iPhone App 1.0' then 'iPhone'
when http_useragent ilike 'RTR iPhone App%' then 'iOS App'
when (http_useragent ilike '%blackberry%' or http_useragent ilike '%bb10%') and http_useragent not ilike '%playbook%' then 'Blackberry Phone'
when http_useragent ilike '%playbook%' then 'Blackberry Tablet'
when http_useragent ilike '%windows%' and (http_useragent ilike '%phone%' or http_useragent ilike '%mobile%'  or http_useragent ilike '%iemobile%')  then 'Windows Phone'
when http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%' then 'Windows Tablet'
when http_useragent ilike '%symbian%' or http_useragent ilike '%symbos%' or http_useragent ilike '%series60%' or http_useragent ilike '%series40%' or http_useragent ilike '%s60%nokia%' or http_useragent ilike '%s40%nokia%' then 'Symbian Phone'
when http_useragent ilike  '%brandingbrand%' then 'Other Mobile'
else 'Desktop' end as devicee
 from etl.agent2device 

select refresh('etl.agent2device');
select start_refresh();

select * from 


select uid,device 
from etl.daily_user_funnels limit 100

group by user
