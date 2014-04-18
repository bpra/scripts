select max(asofdate_est) from rtrbi.daily_user_funnels limit 10
select * from select * from rtrbi.daily_user_funnels where order_total is not null limit 1000 order by asofdate_est desc limit 10

drop table if exists rtrbi.daily_user_funnels;
create table rtrbi.daily_user_funnels
(
asofdate_est date
,uid int
,uid_discovered int
,session_id varchar(60)
,browser_id varchar(60)
,device_id int
,first_timestamp_est datetime
,last_timestamp_est datetime
,action_count int
,rtrhome int
,rtrdesigners_page_view int
,designer_detail int
,home_search_nov2011 int
,search_dresses int
,search_accessories int
,search_whatsnew int
,search_occasions int
,view_node int
,rezo_search int
,rezo_select int
,rezo_add_to_cart int
,outfits_add_to_cart int
,shopping_bag_page_view int
,shipping_info int
,billing_info int
,precheckout int
,precheckout_has_data int
,precheckout_page_empty int
,checkout_complete int
,checkout_other int
,checkout_failure int
,insert_order int
,order_total numeric
,num_orders int
)
segmented by hash(uid) all nodes;

grant all on rtrbi.daily_user_funnels to analytics_role;
grant all on rtrbi.daily_user_funnels to etl_role;
alter table rtrbi.daily_user_funnels owner to etl;
commit;

set timezone to 'US/Eastern';



insert into rtrbi.daily_user_funnels
select 
	date(datetime_cst) as asofdate_est
	,case when pixel.uid = 0 then null else pixel.uid end as uid
	,case 
		when history.uid is not null then history.uid
		else case 
					 when pixel.uid = 0 then null
					 else pixel.uid
				 end
	 end as uid_discovered
	,session_id
	,pixel.browser_id
	,
	case 
			when http_useragent ilike '%select%' or http_useragent ilike '%order by%' or http_useragent like '-%' or http_useragent ilike '%case%when%' then 999 --garbage
			when http_useragent ilike '%yahoo%slurp%' or http_useragent ilike '%genieo%' or http_useragent ilike '%mon.itor.us%' then 101 --bots
			when http_useragent ilike '%httrack%' or http_useragent ilike '%Pinterest.com%' or http_useragent ilike '%gnip%' or http_useragent ilike 'parsijoo'  then 101 --not attempting to be racist;this is a legit iranian searchengine;
			when http_useragent ilike '%crawl%' or http_useragent ilike '%spider%' or http_useragent ilike '%bot%' or http_useragent ilike '%MSNPTC%' or http_useragent ilike '%facebookexternalhit%'  then 101 --more bots
			when http_useragent ilike  '%panopta%' or http_useragent ilike '%typhoeus%' or http_useragent ilike '%nagios%' or http_useragent ilike '%HTTP-Monitor%' or http_useragent ilike '%newrelicpinger%' or http_useragent ilike '%HttpMonitor%'  then 120 --monitoring
			when http_useragent ilike '%Windows NT%' or http_useragent ilike '%Macintosh%' or http_useragent ilike '%Linux%x86%' or http_useragent ilike '%Linux%i686%' or http_useragent like '%CrOS%' then 1 --desktop
			when http_useragent ilike '%silk%' then 12 -- kindle android
			when (http_useragent ilike '%blackberry%' or http_useragent ilike '%bb10%') and http_useragent not ilike '%playbook%' then 30 --bberry phones
			when http_useragent ilike '%playbook%' then 31 --bbery tablets
			when http_useragent ilike '%android%' and http_useragent not ilike '%mobile%' then 11 -- android tablets
			when http_useragent ilike '%android%' and http_useragent ilike '%mobile%' then 10 --android mobile
			when http_useragent ilike 'RTR iPhone App%' then 23 --our ios app
			when http_useragent ilike '%hp-tablet%' or http_useragent ilike '%webos%' then 61 --Other tablet (webos) 
			when http_useragent like '%iPad%' then 22 
			when http_useragent like '%iPod%' then 21
			when http_useragent like '%iPhone%' and http_useragent <> 'RTR iPhone App 1.0' then 20 --iphones accessing msite and not app
			when http_useragent ilike '%windows%' and (http_useragent ilike '%phone%' or http_useragent ilike '%mobile%'  or http_useragent ilike '%iemobile%' or http_useragent ilike '%windows ce%')  then 40 -- windows phones
			when http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%' then 41 -- windows tablets
			when http_useragent ilike '%symbian%' or http_useragent ilike '%symbos%' or http_useragent ilike '%series60%' or http_useragent ilike '%series40%' or http_useragent ilike '%s60%nokia%' or http_useragent ilike '%s40%nokia%' then 50 -- symbian phones
			when http_useragent ilike '%mini%' or http_useragent ilike '%mobile%' or http_useragent ilike '%midp%' or http_useragent ilike '%mmp%' or http_useragent ilike '% wap%' or http_useragent ilike '%obigo%'  then 60 --other mobile devices
			when http_useragent ilike '%python%' or http_useragent ilike '%ruby%' or http_useragent ilike '%java/%' or http_useragent ilike '%ia_archiver%' or http_useragent ilike '%linkdex.com%' then 101 --even more bots
			when http_useragent ilike '%playstation%' or http_useragent ilike '%xbox%' or http_useragent ilike '%nintendo%' then 70 -- consoles
			else 100 end --bot suspects
	 as device_id
	,min(datetime_cst) as first_timestamp_est
	,max(datetime_cst) as last_timestamp_est
	,count(*) as action_count 
	,sum(case when (context like '%rtr_home%' or action_type = 'rtr_home') and object_type = 'page_view' then 1 else 0 end) as rtrhome
	,sum(case when context like '%rtrdesigners%' and object_type = 'top_nav' and action_type = 'page_load' then 1 else 0 end) as rtrdesigners_page_view
	,sum(case when action_type = 'designer_detail' and object_type = 'page_view' then 1 else 0 end) as designer_detail
	,sum(case when parser_type = 'nov_2011_home_search' then 1 else 0 end) as home_search_nov2011
	,sum(case when object_type ='grid' and action_type ='grid' and url = '/dress/search' then 1 else 0 end) as search_dresses
	,sum(case when object_type ='grid' and action_type ='grid' and url = '/accessory/search' then 1 else 0 end) as search_accessories
	,sum(case when object_type ='grid' and action_type ='grid' and url like '%/whatsnew%' then 1 else 0 end) as search_whatsnew
	,sum(case when object_type ='grid' and action_type ='grid' and url like '%/occasions/%' then 1 else 0 end) as search_occasions
	,sum(case when object_type ='node' and action_type ='view'  then 1 else 0 end) as view_node
	,sum(case when object_type ='node' and action_type = 'rezo_search' then 1 else 0 end) as rezo_search
	,sum(case when object_type ='node' and action_type = 'rezo_select' then 1 else 0 end) as rezo_select
	,sum(case when object_type ='node' and action_type = 'rezo_add_to_cart' then 1 else 0 end) as rezo_add_to_cart
	,sum(case when object_type = 'precheckout' and action_type = 'outfits' then 1 else 0 end) as outfits_add_to_cart
	,sum(case when object_type = 'page_view' and action_type = 'shopping_bag' then 1 else 0 end) as shopping_bag_page_view
	,sum(case when object_type = 'checkout' and action_type = 'shipping_info' then 1 else 0 end) as shipping_info
	,sum(case when object_type = 'checkout' and action_type = 'billing_info' then 1 else 0 end) as billing_info
	,sum(case when action_type = 'precheckout' and object_type = 'page_view' then 1 else 0 end) as precheckout
	,sum(case when object_type = 'precheckout' and action_type = 'has_data' then 1 else 0 end) as precheckout_has_data
	,sum(case when object_type = 'precheckout' and action_type = 'page_empty' then 1 else 0 end) as precheckout_page_empty
	,sum(case when object_type = 'page_view' and action_type = 'checkout_complete' then 1 else 0 end) as checkout_complete
	,sum(case when object_type = 'checkout' and action_type not in('billing_info','shipping_info') then 1 else 0 end) as checkout_other
	,sum(case when object_type = 'checkout' and action_type like '%failure%' then 1 else 0 end) as checkout_failure
	,sum(case when object_type = 'order' and action_type = 'insert' then 1 else 0 end) as insert_order
from etl.pixel_raw2 pixel
left outer join rtrbi.browser_history history
 on history.browser_id = pixel.browser_id
 and pixel.datetime_cst between history.id_from_date and history.id_to_date
where log_source = 'pixel'
group by 1,2,3,4,5,6;

delete from rtrbi.daily_user_funnels where device_id>100;
commit;

update rtrbi.daily_user_funnels 
set order_total =orders.order_total,
num_orders= orders.num_orders
from          (    
								select p.datetime_cst::DATE as order_date, -- ao.order_date 
                                nullifzero(ao.uid) as uid,
                                p.browser_id,
                                p.session_id,
                                sum(order_total-li_amount) as order_total,
                                count(distinct ao.order_id) as num_orders
                            from etl.order_line_items ao
                                inner join etl.pixel_raw2 p
                                on ao.order_id = nullif(regexp_substr(p.json_data,'order_id[\":]*(\w*)', 1, 1, 'b',1),'')::int
                            where 1=1 
                                and li_type = 'tax'                            
                                and p.action_type='insert'
                                and p.object_type='order'
                                and p.log_source = 'pixel'                                
                            group by 1,2,3,4            

              ) orders
                 where 1=1 
                 and daily_user_funnels.checkout_complete > 0
                 and orders.browser_id    = daily_user_funnels.browser_id
                 and orders.session_id    = daily_user_funnels.session_id
                 and orders.order_date = daily_user_funnels.asofdate_est;
commit;



drop table rtrbi.user_multidevice_map;

create table rtrbi.user_multidevice_map
(
	asofdate_est date,
	uid_discovered int,
	mobile_iphone tinyint,
	mobile_android tinyint,
	mobile_other tinyint,
	tablet_ipad tinyint,
	tablet_android tinyint,
	tablet_other tinyint,
	desktop tinyint,
	ios_app tinyint
) unsegmented  all nodes;

alter table rtrbi.user_multidevice_map owner to etl;
grant all on rtrbi.user_multidevice_map to analytics_role;
grant all on rtrbi.user_multidevice_map to etl_role;



insert into rtrbi.user_multidevice_map
select 
	fun.asofdate_est,
	fun.uid_discovered,
	max(case when device_id=20 then 1 else 0 end) as mobile_iphone,
	max(case when device_id=10 then 1 else 0 end) as mobile_android,
	max(case when device_id in (21,30,40,50,60) then 1 else 0 end) as mobile_other,
	max(case when device_id in (22) then 1 else 0 end) as tablet_ipad,
	max(case when device_id in (11,12) then 1 else 0 end) as tablet_android,
	max(case when device_id in (31,41,61) then 1 else 0 end) as tablet_other,
	max(case when device_id in (1,70) then 1 else 0 end) as desktop,
	max(case when device_id=23 then 1 else 0 end) as ios_app
from rtrbi.daily_user_funnels fun
where 1=1
	and fun.uid_discovered is not null
	and fun.device_id <100
group by 1,2;
commit;

select refresh('rtrbi.user_multidevice_map');
select refresh('rtrbi.daily_user_funnels');
select analyze_statistics('rtrbi.daily_user_funnels');
select analyze_statistics('rtrbi.user_multidevice_map');

--where 1=1
--and log_file like concat(concat(concat(concat('/var/data/pixel/',year(sysdate-1)),to_char(sysdate-1,'MM')),to_char(sysdate-1,'DD')),'%')

update etl.pixel_raw2
set device_id = 	
case 
			when http_useragent ilike '%select%' or http_useragent ilike '%order by%' or http_useragent like '-%' or http_useragent ilike '%case%when%' then 999 --garbage
			when http_useragent ilike '%yahoo%slurp%' or http_useragent ilike '%genieo%' or http_useragent ilike '%mon.itor.us%' then 101 --bots
			when http_useragent ilike '%httrack%' or http_useragent ilike '%Pinterest.com%' or http_useragent ilike '%gnip%' or http_useragent ilike 'parsijoo'  then 101 --not attempting to be racist;this is a legit iranian searchengine;
			when http_useragent ilike '%crawl%' or http_useragent ilike '%spider%' or http_useragent ilike '%bot%' or http_useragent ilike '%MSNPTC%' or http_useragent ilike '%facebookexternalhit%'  then 101 --more bots
			when http_useragent ilike  '%panopta%' or http_useragent ilike '%typhoeus%' or http_useragent ilike '%nagios%' or http_useragent ilike '%HTTP-Monitor%' or http_useragent ilike '%newrelicpinger%' or http_useragent ilike '%HttpMonitor%'  then 120 --monitoring
			when http_useragent ilike '%Windows NT%' or http_useragent ilike '%Macintosh%' or http_useragent ilike '%Linux%x86%' or http_useragent ilike '%Linux%i686%' or http_useragent like '%CrOS%' then 1 --desktop
			when http_useragent ilike '%silk%' then 12 -- kindle android
			when (http_useragent ilike '%blackberry%' or http_useragent ilike '%bb10%') and http_useragent not ilike '%playbook%' then 30 --bberry phones
			when http_useragent ilike '%playbook%' then 31 --bbery tablets
			when http_useragent ilike '%android%' and http_useragent not ilike '%mobile%' then 11 -- android tablets
			when http_useragent ilike '%android%' and http_useragent ilike '%mobile%' then 10 --android mobile
			when http_useragent ilike 'RTR iPhone App%' then 23 --our ios app
			when http_useragent ilike '%hp-tablet%' or http_useragent ilike '%webos%' then 61 --Other tablet (webos) 
			when http_useragent like '%iPad%' then 22 
			when http_useragent like '%iPod%' then 21
			when http_useragent like '%iPhone%' and http_useragent <> 'RTR iPhone App 1.0' then 20 --iphones accessing msite and not app
			when http_useragent ilike '%windows%' and (http_useragent ilike '%phone%' or http_useragent ilike '%mobile%'  or http_useragent ilike '%iemobile%' or http_useragent ilike '%windows ce%')  then 40 -- windows phones
			when http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%' then 41 -- windows tablets
			when http_useragent ilike '%symbian%' or http_useragent ilike '%symbos%' or http_useragent ilike '%series60%' or http_useragent ilike '%series40%' or http_useragent ilike '%s60%nokia%' or http_useragent ilike '%s40%nokia%' then 50 -- symbian phones
			when http_useragent ilike '%mini%' or http_useragent ilike '%mobile%' or http_useragent ilike '%midp%' or http_useragent ilike '%mmp%' or http_useragent ilike '% wap%' or http_useragent ilike '%obigo%'  then 60 --other mobile devices
			when http_useragent ilike '%python%' or http_useragent ilike '%ruby%' or http_useragent ilike '%java/%' or http_useragent ilike '%ia_archiver%' or http_useragent ilike '%linkdex.com%' then 101 --even more bots
			when http_useragent ilike '%playstation%' or http_useragent ilike '%xbox%' or http_useragent ilike '%nintendo%' then 70 -- consoles
			else 100 end --bot suspects
	 as device_id



CREATE PROJECTION browser_history_funnel (
 browser_id ENCODING RLE,
 uid ENCODING RLE,
 id_from_date ENCODING RLE,
 id_to_date ENCODING RLE
)
AS
 SELECT browser_id,
        uid,
        id_from_date,
        id_to_date
 FROM rtrbi.browser_history
 ORDER BY browser_id,
          uid,
          id_to_date
UNSEGMENTED ALL NODES;
select * from rtrbi.browser_history where browser_id = '650b4ecf8b47d2c7fe7a274272aa1d25'
select refresh('rtrbi.browser_history');

select * from projections where projection_name like 'browser_history%'
select 
commit;
v_monitor.sessions 
select * from sessions
select close_session('vertica02.analytics-22229:0x18cbb4');
select node_names,object_name,lock_mode,lock_scope from LOCKS; 
select browser_id,session_id,datetime_cst,uid,next_uid,conditional_true_event(uid<>next_uid and session_id <> next_session_id) over (partition by browser_id order by datetime_cst,uid)  from (
select distinct browser_id,session_id,uid, datetime_cst ,lead(uid) over (partition by browser_id order by datetime_cst,uid) as next_uid, lead(session_id) over (partition by browser_id order by datetime_cst,uid) as next_session_id
from etl.pixel_raw2 where browser_id = '650b4ecf8b47d2c7fe7a274272aa1d25'
)xx
order by 3,4
)

select * from etl.pixel_raw2 limit 10

select * from rtrbi.browser_history limit 10

select max(log_file) from etl.pixel_raw2
explain update etl.pixel_raw2
set device_id = 70 
where http_useragent ilike '%playstation%' or http_useragent ilike '%xbox%' or http_useragent ilike '%nintendo%'
select start_refresh();
create projection select * from etl.pixel_raw2 limit 10
(
datetime_cst encoding DELTAVAL,
http_useragent encoding rle,
device_id encoding rle
)
as select datetime_cst,http_useragent,device_id from etl.pixel_raw2
order by http_useragent
segmented by hash(http_useragent) all nodes


select distinct log_file from etl.pixel_raw2 where datetime_cst >= '2013-11-10' order by 1
delete from etl.pixel_raw2 where log_file ='/var/data/pixel/20131114-post.log.gz'
commit;



limit 10
select * from etl.pixel_raw2 raw where not exists (select 1 from etl.browser_history_for_id bh where bh.browser_id =raw.browser_id) limit 10

select * from rtrbi.browser_history  limit 10
create table etl.z_temp_user_discovery as
select * from etl.pixel_raw2 limit 10
where browser_id = 

select * from rtrbi.daily_user_funnels fun  where uid_discovered is null limit 1000
select * from rtrbi.daily_user_funnels fun  where uid_discovered is null limit 1000 and uid is null limit 1000
select pixel.uid, history.uid as uid_discovered  from etl.z_temp_user_discovery pixel
 inner join etl.browser_history_for_id history
on history.browser_id = pixel.browser_id
 and pixel.datetime_cst between history.id_from_date and history.id_to_date
where log_source = 'pixel' order by ts;
select rebalance_cluster()
select * from rtrbi.browser_history where uid = 1664397 first_seen = last_seen
select * from etl.browser_history_for_id limit 20
create table etl.browser_history_for_id 
as
select 
browser_id
,uid
,coalesce(lag(first_seen) over(partition by browser_id order by first_seen), '2009-01-01' ) as id_from_date
,first_seen id_to_date
from rtrbi.browser_history 
where browser_id = '00283910afa38e3c4a0060179bdc39fb'

select * from analytics.users where uid in (5164868,6586764,6586986,4324374)
limit 10
set 
set @ 
select concat(concat(concat(concat('/var/data/pixel/',year(sysdate-1)),to_char(sysdate-1,'MM')),to_char(sysdate-1,'DD')),'%')
set timezone to 'US/Eastern';
set timezone to 'UTC';
select current_date();
select current_time;
commit;
select getdate()

select 
date_part('month', asofdate_est)
,device
,sum(view_node) as sumviews
,sum(rezo_search) as rezosearch
,sum(rezo_select) as rezoselect
,sum(precheckout) as precheckout
,sum(rezo_add_to_cart) as rezoaddtocart
,sum(checkout_complete) as checkoutcomplete
,sum(checkout_complete)*100/sum(view_node) as chckoutaspercentofviews
from rtrbi.daily_user_funnels duf
inner join rtrbi.devices d
on duf.device_id = d.device_id
where duf.device_id <=100
group by 1,2
order by 1,8 desc,2

grant all on et to etl_role

alter table etl.unit
add column permanent_showroom_id int

select * from etl.unit
select refresh('rtrbi.daily_user_funnels');
select refresh('etl.test_nonbot');
select * from  rtrbi.daily_user_funnels order by first_timestamp_est desc limit 10

select * from  etl.pixel_raw2 
order by datetime_cst desc 
limit 10



create table etl.user_level_data as 
select 
	asofdate_est
	,funnels.uid
	,uid_discovered
	,funnels.browser_id
	,funnels.session_id
	,device_id
	,max(last_timestamp_est)
	first_timestamp_est)  as timeSpent
	,sum(order_total) as totalSpent
	,sum(num_orders) as ordersMade
from rtrbi.daily_user_funnels funnels
         left outer join 
            (    
									select p.datetime_cst::DATE as order_date, -- ao.order_date 
                                nullifzero(ao.uid) as uid,
                                p.browser_id,
                                p.session_id,
                                sum(order_total-li_amount) as order_total,
                                count(distinct ao.order_id) as num_orders
                            from etl.order_line_items ao
                                inner join etl.pixel_raw2 p
                                on ao.order_id = nullif(regexp_substr(p.json_data,'order_id[\":]*(\w*)', 1, 1, 'b',1),'')::int
                            where 1=1 
                                and li_type = 'tax'                            
                                and p.action_type='insert'
                                and p.object_type='order'
                                and p.log_source = 'pixel'                                
                            group by 1,2,3,4            

              ) orders
                 on 1=1
								 and orders.uid = funnels.uid
                 and orders.browser_id    = funnels.browser_id
                 and orders.session_id    = funnels.session_id
                 and orders.order_date = funnels.asofdate_est
group by 1,2,3,4,5,6
 
create table rtrbi.user_multidevice_map
(
asofdate_est date,
uid_discovered int,
mobile_iphone tinyint,
mobile_android tinyint,
mobile_other tinyint,
tablet_ipad tinyint,
tablet_android tinyint,
tablet_other tinyint,
desktop tinyint,
ios_app tinyint
) unsegmented  all nodes;

alter table rtrbi.user_multidevice_map owner to etl;
grant all on rtrbi.user_multidevice_map to analytics_role;
grant all on rtrbi.user_multidevice_map to etl_role;


insert into rtrbi.user_multidevice_map
select distinct asofdate_est, -- fun.asofdate_est
fun.uid_discovered,
max(case when device_id=20 then 1 else 0 end) as mobile_iphone,
max(case when device_id=10 then 1 else 0 end) as mobile_android,
max(case when device_id in (21,30,40,50,60) then 1 else 0 end) as mobile_other,
max(case when device_id in (22) then 1 else 0 end) as tablet_ipad,
max(case when device_id in (11,12) then 1 else 0 end) as tablet_android,
max(case when device_id in (31,41) then 1 else 0 end) as tablet_other,
max(case when device_id=1 then 1 else 0 end) as desktop,
max(case when device_id=23 then 1 else 0 end) as ios_app
from rtrbi.daily_user_funnels fun
where 1=1
and fun.uid_discovered is not null
and fun.device_id <100
group by 1,2;
commit;
                    
select  cast('00:10:00' as interval SECOND


select week_start
	,mobile_iphone
	,mobile_android
	,mobile_other
	,tablet_ipad
	,tablet_android
	,tablet_other
	,desktop
	,ios_app
	,browser_id
	,session_id
	,uid
	,uid_discovered
	,totalSpent
	,timeSpent
	,INTERVAL timeSpent SECOND
	,ordersMade
from
	(
		select 	*
		,case when device_id=20 then 1 else 0 end as mobile_iphone
		,case when device_id=10 then 1 else 0 end as mobile_android
		,case when device_id in (21,30,40,50,60) then 1 else 0 end as mobile_other
		,case when device_id in (22) then 1 else 0 end as tablet_ipad
		,case when device_id in (11,12) then 1 else 0 end as tablet_android
		,case when device_id in (31,41) then 1 else 0 end as tablet_other
		,case when device_id=1 then 1 else 0 end as desktop
		,case when device_id=23 then 1 else 0 end as ios_app
	  from etl.user_level_data
	) brolevel
inner join etl.dim_fiscal_dates dates
	on brolevel.asofdate_est = dates.asof_date
where 1=1
	and week_start >=	'2013-09-01'
	and device_id <100
	
limit 10
set timezone to 'us/eastern'
select 
dates.week_start,
case 
when (http_useragent ilike '%android%' and http_useragent ilike '%mobile%') 
	or (http_useragent like '%iPhone%' and http_useragent <> 'RTR iPhone App 1.0')
	or (http_useragent like '%iPod%')
	or ((http_useragent ilike '%blackberry%' or http_useragent ilike '%bb10%') and http_useragent not ilike '%playbook%')
	or (http_useragent ilike '%windows%' and (http_useragent ilike '%phone%' or http_useragent ilike '%mobile%'  or http_useragent ilike '%iemobile%'))
	or (http_useragent ilike  '%brandingbrand%') then 'MSite'
when (http_useragent ilike '%silk%') 
	or (http_useragent ilike '%android%' and http_useragent not ilike '%mobile%')
	or (http_useragent like '%iPad%')
	or (http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%')
	or (http_useragent ilike '%playbook%') then 'Tablet'
when http_useragent ilike 'RTR iPhone App%' then 'ios'
else 'desk'
end as device
,count(distinct session_id )
from etl.pixel_raw2 brolevel
inner join etl.dim_fiscal_dates dates
	on date(brolevel.datetime_cst) = dates.asof_date
	where week_start >= '2013-09-01'
	and http_useragent not ilike '%select%' 
	and http_useragent not ilike '%order by%' 
	and http_useragent not like '-%' 
	and http_useragent not ilike '%case%when%' 
  and http_useragent not ilike '%crawl%' and http_useragent not ilike '%spider%' and http_useragent not ilike '%bot%'
group by 1,2	
order by 1,2

select 
week_start
	,mobile_iphone
	,mobile_android
	,mobile_other
	,tablet_ipad
	,tablet_android
	,tablet_other
	,desktop
	,ios_app
	,count(distinct browser_id) as visitors
	,count(distinct session_id) as visits
	,count(distinct uid) as logged_in_users
	,count(distinct uid_discovered) as all_discovered_users
	,sum(order_total) as revenue
	,sum(last_timestamp_est-first_timestamp_est)/count(distinct session_id) as avgTimeSpent
	,sum(num_orders) as total_num_orders
	,sum(num_orders)/count(distinct session_id)*100 as CVR          
	,sum(order_total)/sum(num_orders)  as AOV 
 from
	(
		select 	*
		,case when device_id=20 then 1 else 0 end as mobile_iphone
		,case when device_id=10 then 1 else 0 end as mobile_android
		,case when device_id in (21,30,40,50,60) then 1 else 0 end as mobile_other
		,case when device_id in (22) then 1 else 0 end as tablet_ipad
		,case when device_id in (11,12) then 1 else 0 end as tablet_android
		,case when device_id in (31,41) then 1 else 0 end as tablet_other
		,case when device_id=1 then 1 else 0 end as desktop
		,case when device_id=23 then 1 else 0 end as ios_app
	  from rtrbi.daily_user_funnels 
	  where 1=1
		and asofdate_est >=	'2013-09-01'
		and device_id <100
	) brolevel
inner join etl.dim_fiscal_dates dates
	on brolevel.asofdate_est = dates.asof_date
group by 1,2,3,4,5,6,7,8,9
order by 1,2,3,4,5,6,7,8,9


select 
userbyweek.week_start
	,mobile_iphone
	,mobile_android
	,mobile_other
	,tablet_ipad
	,tablet_android
	,tablet_other
	,desktop
	,ios_app
	,count(distinct browser_id) as visitors
	,count(distinct session_id) as visits
	,count(distinct userbyweek.uid_discovered) as all_discovered_users
	,sum(order_total) as revenue
	,sum(case when device_id = 1 then last_timestamp_est-first_timestamp_est else null end)/count(distinct case when device_id = 1 then session_id else null end) as avg_time_desktop
	,sum(case when device_id = 1 then null else last_timestamp_est-first_timestamp_est end)/count(distinct case when device_id = 1 then null else session_id end) as avg_time_other		
	,sum(num_orders) as total_num_orders
	,sum(num_orders)/count(distinct session_id)*100 as CVR          
	,sum(order_total)/sum(num_orders)  as AOV 
from(
			select 
															week_start,
															uid_discovered,
			                        max(mobile_iphone) as mobile_iphone,
			                        max(mobile_android) as mobile_android,
			                        max(mobile_other) as mobile_other,
			                        max(tablet_ipad) as tablet_ipad,
			                        max(tablet_android) as tablet_android,
			                        max(tablet_other) as tablet_other,
			                        max(desktop) as desktop,
			                        max(ios_app) as ios_app
			 from etl.user_multidevice_map map
			inner join etl.dim_fiscal_dates dates
			on map.asofdate_est = dates.asof_date
			where week_start >=	'2013-09-01'
			group by 1,2
)userbyweek
inner join rtrbi.daily_user_funnels brolevel
on userbyweek.uid_discovered = brolevel.uid_discovered
inner join etl.dim_fiscal_dates dates
	on brolevel.asofdate_est = dates.asof_date
	and dates.week_start = userbyweek.week_start
where brolevel.asofdate_est >=	'2013-09-01'
	and device_id <100
group by 1,2,3,4,5,6,7,8,9
order by 1,2,3,4,5,6,7,8,9


drop table rtrbi.user_multidevice_map;

create table rtrbi.user_multidevice_map
(
	asofdate_est date,
	uid_discovered int,
	mobile_iphone tinyint,
	mobile_android tinyint,
	mobile_other tinyint,
	tablet_ipad tinyint,
	tablet_android tinyint,
	tablet_other tinyint,
	desktop tinyint,
	ios_app tinyint
) unsegmented  all nodes;

alter table rtrbi.user_multidevice_map owner to etl;
grant all on rtrbi.user_multidevice_map to analytics_role;
grant all on rtrbi.user_multidevice_map to etl_role;
commit;

insert into select * from rtrbi.user_multidevice_map
select 
	fun.asofdate_est,
	fun.uid_discovered,
	max(case when device_id=20 then 1 else 0 end) as mobile_iphone,
	max(case when device_id=10 then 1 else 0 end) as mobile_android,
	max(case when device_id in (21,30,40,50,60) then 1 else 0 end) as mobile_other,
	max(case when device_id in (22) then 1 else 0 end) as tablet_ipad,
	max(case when device_id in (11,12) then 1 else 0 end) as tablet_android,
	max(case when device_id in (31,41) then 1 else 0 end) as tablet_other,
	max(case when device_id=1 then 1 else 0 end) as desktop,
	max(case when device_id=23 then 1 else 0 end) as ios_app
from rtrbi.daily_user_funnels fun
where 1=1
	and fun.uid_discovered is not null
	and fun.device_id <100
group by 1,2;
commit;


select * from etl.pixel_raw2
where 
uid=2731433
and
datetime_cst> '2013-10-04'
order by datetime_cst
select * from sessions; 
select * from etl.browser2user limit 10
select * from etl.pixel_raw2 where log_file like '%postq%' limit 100

select * from etl.pixel_raw2 where is_mobile = true and http_useragent in (select http_useragent from etl.agent2device where device_id = 1)  limit 100

select * 
from etl.pixel_raw2 
where 1=1

and (

 url ilike '%search%' 
or context ilike '%search%' 
or action_type ilike '%search%'
--or http_referrer = '%search%'
)
and log_source = 'pixel'
and object_type = 'page_view'
--and parser_type != 'nov_2011_home_search'
limit 100

select * from  etl.pixel_raw2 
where 1=1
--and date(datetime_cst) = '2013-01-07'
and log_file like  '%postq%' limit 100
 2013-01-07		
uid = 1726000
select refresh('etl.agent2device');
select distinct device,device_id from etl.agent2device order by device_id limit 1000

,sum(case when url like '%rtr_home%' or context like '%rtr_home%' then 1 else 0 end) as search


,sum(case when url like '%rtr_home%' or context like '%rtr_home%' then 1 else 0 end) as upsell_add_to_cart



create table etl.audit_package_inbound
(
id int,
tracking_id int,
tracking_number varchar(40),
group_id  int,
urgency_displayed varchar(50),
potential_urgent_units int,
usern varchar(64),
timestamp timestamptz
)segmented by (group_id) all nodes;

grant all on etl.audit_package_inbound to analytics_role




create table rtrbi.devices
as
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

grant all on schema rtrbi to etl_role
select * from rtrbi.devices
select * from users
select 
date(datetime_cst) as asofdate
,is_mobile
,uid

--select * 
from etl.pixel_raw2
where 
uid = 4417186 and date(datetime_cst) = '2013-04-26'
and object_type = 'page_view'
group by 1,2,3
limit 10



select browser_id,count(*) from rtrbi.browser_history group by 1 having count(*)>1 
select * from rtrbi.browser_history where browser_id = '00283910afa38e3c4a0060179bdc39fb'
select * from rtrbi.devices


select uid, 

count( distinct (
case 
when device_id in (10,20,21,30,40,50,60) then 'mobile'
when device_id in (11,22,31,41) then 'tablet'
when device_id=1 then 'desktop'
when device_id=23 then 'ios_app'
when device_id not in (10,20,21,30,40,50,60,11,22,31,41,1,23) then 'other'
end)) as num_devices,
max( case when device_id in (10,20,21,30,40,50,60) then 1 else 0 end) as mobile_device,
max( case when device_id in (11,22,31,41) then 1 else 0 end) as tablet_device,
max( case when device_id=1 then 1 else 0 end) as desktop,
max( case when device_id=23 then 1 else 0 end) as ios_app,
max( case when device_id not in (10,20,21,30,40,50,60,11,22,31,41,1,23) then 1 else 0 end) as other

from rtrbi.daily_user_funnels 
group by uid

 SELECT projection_name, anchor_table_name, is_prejoin, is_up_to_date 
FROM projections WHERE is_up_to_date = false;

alter table rtrbi.daily_user_funnels

update rtrbi.daily_user_funnels  
set order_total =orders.order_total,
num_orders= orders.num_orders
select orders.uid,daily_user_funnels.uid,orders.num_orders,daily_user_funnels.num_orders,daily_user_funnels.checkout_complete
,* from          (    
								select p.datetime_cst::DATE as order_date, -- ao.order_date 
                                nullifzero(ao.uid) as uid,
                                p.browser_id,
                                p.session_id,
                                sum(order_total-li_amount) as order_total,
                                count(distinct ao.order_id) as num_orders
                           from etl.order_line_items ao
                                inner join etl.pixel_raw2 p
                                on ao.order_id = nullif(regexp_substr(p.json_data,'order_id[\":]*(\w*)', 1, 1, 'b',1),'')::int
                            where 1=1 
                                and li_type = 'tax'                            
                                and p.action_type='insert'
                                and p.object_type='order'
                                and p.log_source = 'pixel' 
and                                date(datetime_cst) in ('2013-10-07','2013-10-08') 
and p.uid in (6353078,7597576) 
and browser_id in ('183566a8-468c-44d4-9fb0-742bbc3842f5-1381118890','2B141ED7-B443-431D-9C8C-023FA9B2D5C0')                               
                            group by 1,2,3,4            
              ) orders
              inner join rtrbi.daily_user_funnels 
                 on 1=1 
                 and daily_user_funnels.checkout_complete =1 
                 and orders.browser_id    = daily_user_funnels.browser_id
                 and orders.session_id    = daily_user_funnels.session_id
                 and orders.order_date = daily_user_funnels.asofdate_est;
commit;


create table etl.test_nonbot 
as 
select distinct http_useragent,count(*) as browscount
from etl.pixel_raw2 
where http_useragent not ilike '%select%' and http_useragent not ilike '%order by%' and http_useragent not like '-%' 
and http_useragent not  ilike '%case%when%' 
and http_useragent not ilike '%crawl%' and http_useragent not ilike '%spider%' and http_useragent not ilike '%bot%' 
group by 1

alter table etl.test_nonbot
add column device int


update etl.test_nonbot
set device = 
case 
when http_useragent ilike '%select%' or http_useragent ilike '%order by%' or http_useragent like '-%' or http_useragent ilike '%case%when%' then 999
when http_useragent ilike '%yahoo%slurp%' or http_useragent ilike '%genieo%' or http_useragent ilike '%mon.itor.us%' then 101
when http_useragent ilike '%httrack%' or http_useragent ilike '%Pinterest.com%' or http_useragent ilike '%gnip%' or http_useragent ilike 'parsijoo'  then 101 --not attempting to be racist;this is a legit iranian searchengine; probably their badly written bot
when http_useragent ilike '%crawl%' or http_useragent ilike '%spider%' or http_useragent ilike '%bot%' or http_useragent ilike '%MSNPTC%' or http_useragent ilike '%facebookexternalhit%'  then 101
when http_useragent ilike  '%panopta%' or http_useragent ilike '%typhoeus%' or http_useragent ilike '%nagios%' or http_useragent ilike '%HTTP-Monitor%' or http_useragent ilike '%newrelicpinger%' or http_useragent ilike '%HttpMonitor%'  then 120 --monitoring
when http_useragent ilike '%silk%' then 12
when (http_useragent ilike '%blackberry%' or http_useragent ilike '%bb10%') and http_useragent not ilike '%playbook%' then 30
when http_useragent ilike '%playbook%' then 31
when http_useragent ilike '%android%' and http_useragent not ilike '%mobile%' then 11
when http_useragent ilike '%android%' and http_useragent ilike '%mobile%' then 10
when http_useragent ilike 'RTR iPhone App%' then 23
when http_useragent ilike '%hp-tablet%' or http_useragent ilike '%webos%' then 61 --Other tablet
when http_useragent like '%iPad%' then 22
when http_useragent like '%iPod%' then 21
when http_useragent like '%iPhone%' and http_useragent <> 'RTR iPhone App 1.0' then 20
when http_useragent ilike '%windows%' and (http_useragent ilike '%phone%' or http_useragent ilike '%mobile%'  or http_useragent ilike '%iemobile%' or http_useragent ilike '%windows ce%')  then 40
when http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%' then 41
when http_useragent ilike '%symbian%' or http_useragent ilike '%symbos%' or http_useragent ilike '%series60%' or http_useragent ilike '%series40%' or http_useragent ilike '%s60%nokia%' or http_useragent ilike '%s40%nokia%' then 50
when (http_useragent ilike '%mini%' or http_useragent ilike '%mobile%' or http_useragent ilike '%midp%' or http_useragent ilike '%mmp%' or http_useragent ilike '% wap%' or http_useragent ilike '%obigo%' ) then 60
when http_useragent ilike '%python%' or http_useragent ilike '%ruby%' or http_useragent ilike '%java/%' or http_useragent ilike '%ia_archiver%' or http_useragent ilike '%linkdex.com%' then 101
when http_useragent ilike '%Windows NT%' or http_useragent ilike '%Macintosh%' or http_useragent ilike '%Linux%x86%' or http_useragent ilike '%Linux%i686%' or http_useragent like '%CrOS%' then 1
when http_useragent ilike '%playstation%' or http_useragent ilike '%xbox%' or http_useragent ilike '%nintendo%' then 70 -- consoles
else 100 end --bot suspects
select * from rtrbi.devices

commit;
select min(asofdate_est) from rtrbi.daily_user_funnels


select distinct device 
from 
etl.test_nonbot order by device
where device in (90)

select *,length(http_useragent) 
from 
etl.test_nonbot 
--and http_useragent not ilike '%mozilla/_._%(%'
where device = 888 --and length(http_useragent) <=50
order by browscount desc 
limit 2000

where 
(http_useragent ilike '% wap%' 
or http_useragent ilike '%pocket%' 
)
and device = 888
order by http_useragent limit 2000

select 
	date
	,ga.transactions as ga_transactions
	,ga.revenue as ga_revenue
	,scoop_transactions
	,scoop_revenue
	,pixel_transactions
	,pixel_revenue
	,pixel_insertorders
from
	(
		select date,sum(transactions) as transactions,sum(revenue) as revenue 
		from rtrbi.google_analytics_metrics 
		where period_type = 'daily' and date >= '2013-10-01' 
		group by 1
		) ga
inner join 
	(
	select order_date, count(distinct order_id) as scoop_transactions,sum(order_total) as scoop_revenue
	from (
				select distinct order_date,order_total,order_id 
				from etl.order_summary where order_date >= '2013-10-01'
				)xx group by 1
	) oli
	on oli.order_date = ga.date
inner join 
	(
	select asofdate_est,sum(num_orders) as pixel_transactions, sum(insert_order) as pixel_insertorders ,sum(order_total) as pixel_revenue 
	from rtrbi.daily_user_funnels 
	where asofdate_est >= '2013-10-01'
	group by 1
	) pixel
	on pixel.asofdate_est = ga.date
order by 1

select * from (
	select 
															dates.week_start, dates.week_end,
															uid_discovered,
			                        max(mobile_iphone) as weekly_mobile_iphone,
			                        max(mobile_android) as weekly_mobile_android,
			                        max(mobile_other) as weekly_mobile_other,
			                        max(tablet_ipad) as weekly_tablet_ipad,
			                        max(tablet_android) as weekly_tablet_android,
			                        max(tablet_other) as weekly_tablet_other,
			                        max(desktop) as weekly_desktop,
			                        max(ios_app) as weekly_ios_app
			 from rtrbi.user_multidevice_map map
			inner join etl.dim_fiscal_dates dates
			on map.asofdate_est = dates.asof_date
			where dates.week_start >=	'2013-09-01'
			group by 1,2,3
			)week
			where uid_discovered = 1116458

select * from etl.pixel_raw2 
where datetime_cst >= '2013-09-30'
and nullif(regexp_substr(json_data,'order_id[\":]*(\w*)', 1, 1, 'b',1),'')::int in (
1149834
,1149958
,1167018
,1127184
,1154890
,1159206
,1165934
,1154168
,1139784
,1167080
,1145130
,1147726
,1176280
,1155142
,1155228
,1132974
,1177278
,1188490
,1145696
)

select * from tables where name like ''
select datetime_cst,nullif(regexp_substr(json_data,'order_id[\":]*(\w*)', 1, 1, 'b',1),'')::int as order_id 
from etl.pixel_raw2 
where ip_address = '71.249.207.194'
and action_type='insert'
and object_type='order'
and log_source = 'pixel' 
and datetime_cst >= '2013-09-30' 
and datetime_cst <='2013-11-17'
