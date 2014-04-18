


create table select * from select * from rtrbi.daily_user_funnels where uid =0 order by asofdate_est desc limit 10
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
)
segmented by hash(uid) all nodes;

--update etl.pixel_raw2 
--set uid = null
--where uid = 0

desc etl.pixel_raw2
grant all on rtrbi.daily_user_funnels to analytics_role
grant all on rtrbi.daily_user_funnels to etl_role
alter table rtrbi.daily_user_funnels owner to etl
select * from rtrbi.daily_user_funnels where (uid =0 or uid is null) and uid_discovered is not null limit 10

set timezone to 'US/Eastern'



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
,case 
when http_useragent ilike '%select%' or http_useragent ilike '%order by%' or http_useragent like '-%' or http_useragent ilike '%case%when%' then 999
when http_useragent ilike '%crawl%' or http_useragent ilike '%spider%' or http_useragent ilike '%bot%' then 101
when http_useragent ilike '%silk%' then 12
when (http_useragent ilike '%blackberry%' or http_useragent ilike '%bb10%') and http_useragent not ilike '%playbook%' then 30
when http_useragent ilike '%playbook%' then 31
when http_useragent ilike '%android%' and http_useragent not ilike '%mobile%' then 11
when http_useragent ilike '%android%' and http_useragent ilike '%mobile%' then 10
when http_useragent ilike 'RTR iPhone App%' then 23
when http_useragent like '%iPad%' then 22
when http_useragent like '%iPod%' then 21
when http_useragent like '%iPhone%' and http_useragent <> 'RTR iPhone App 1.0' then 20
when http_useragent ilike '%windows%' and (http_useragent ilike '%phone%' or http_useragent ilike '%mobile%'  or http_useragent ilike '%iemobile%')  then 40
when http_useragent ilike '%windows%' and http_useragent ilike '%touch%' and http_useragent not ilike '%phone%' then 41
when http_useragent ilike '%symbian%' or http_useragent ilike '%symbos%' or http_useragent ilike '%series60%' or http_useragent ilike '%series40%' or http_useragent ilike '%s60%nokia%' or http_useragent ilike '%s40%nokia%' then 50
when http_useragent ilike  '%brandingbrand%' or is_mobile = true then 60
else 1 end 
as device_id
,min(datetime_cst) as first_timestamp_est
,max(datetime_cst) as last_timestamp_est
,count(*) as action_count 
,sum(case when (url like '%rtr_home%' or context like '%rtr_home%' or action_type = 'rtr_home') and object_type = 'page_view' then 1 else 0 end) as rtrhome
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
where 1=1
--and pixel.uid is not null
--and history.uid is null
--and pixel.browser_id = 'd95b62187b84077e061556d5932c042e'
--and log_file like concat(concat(concat(concat('/var/data/pixel/',year(sysdate-1)),to_char(sysdate-1,'MM')),to_char(sysdate-1,'DD')),'%')
and log_source = 'pixel'
group by 1,2,3,4,5,6
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

grant all on rtrbi.daily_user_funnels to etl_role

alter table etl.unit
add column permanent_showroom_id int

select * from etl.unit
select refresh('rtrbi.daily_user_funnels');
select refresh('etl.user_multidevice_map');

create table etl.user_level_data as 
select 
	asofdate_est
	,funnels.uid
	,uid_discovered
	,funnels.browser_id
	,funnels.session_id
	,device_id
	,sum(last_timestamp_est-first_timestamp_est)  as timeSpent
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
 


									create table etl.user_multidevice_map as
                   select distinct asofdate_est, -- fun.asofdate_est
                        fun.uid, 
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
--                    fun.asofdate_est between '2013-10-13' and '2013-10-19'
                    and fun.uid is not null
										and fun.device_id <100
                    group by 1,2,3
                    



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
	,sum(totalSpent) as revenue
	,sum(ordersMade) as total_num_orders
	,sum(ordersMade)/count(distinct session_id)*100 as CVR          
	,sum(totalSpent)/sum(ordersMade)  as AOV 
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
where week_start >=	'2013-09-01'
	and device_id <100
group by 1,2,3,4,5,6,7,8,9
order by 1,2,3,4,5,6,7,8,9

create table etl.user_multidevice_map as
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
--                    fun.asofdate_est between '2013-10-13' and '2013-10-19'
										and uid_discovered is not null
										and fun.device_id <100
                    group by 1,2


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
