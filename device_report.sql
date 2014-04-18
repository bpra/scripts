select userdevice.asofdate_est
                ,mobile_iphone
                ,mobile_android
                ,mobile_other
                ,tablet_ipad
                ,tablet_android
                ,tablet_other
                ,desktop
                ,ios_app
                ,sum(num_browsers) as visitors
                ,sum(num_sessions) as visits
                ,count(distinct visitinfo.uid) as logged_in_users
--              ,count(distinct visitinfo.uid_discovered) as all_discovered_users
          ,sum(order_total) as revenue
          ,sum(num_orders) as total_num_orders
          ,sum(num_orders)/sum(num_sessions)*100 as CVR
          ,sum(order_total)/sum(num_orders)  as AOV
            from (
                                select distinct 'week1' as asofdate_est, -- fun.asofdate_est
                        fun.uid, 
--                        fun.uid_discovered,
                        max(case when device_id=20 then 1 else 0 end) as mobile_iphone,
                        max(case when device_id=10 then 1 else 0 end) as mobile_android,
                        max(case when device_id in (21,30,40,50,60) then 1 else 0 end) as mobile_other,
                        max(case when device_id in (22) then 1 else 0 end) as tablet_ipad,
                        max(case when device_id in (11,12) then 1 else 0 end) as tablet_android,
                        max(case when device_id in (31,41) then 1 else 0 end) as tablet_other,
                        max(case when device_id=1 then 1 else 0 end) as desktop,
                        max(case when device_id=23 then 1 else 0 end) as ios_app
                  from rtrbi.daily_user_funnels fun
                    where fun.asofdate_est between '2013-10-13' and '2013-10-19'
                    and fun.device_id <100
                    group by 1,2 --,3


                ) userdevice
      inner join 
           (
                          select  'week1' as asofdate_est, -- fun.asofdate_est
                        fun.uid, 
--                      fun.uid_discovered,
                        count(distinct fun.session_id) as num_sessions, 
                        count(distinct fun.browser_id) as num_browsers
                    from rtrbi.daily_user_funnels fun
                    where fun.asofdate_est between '2013-10-13' and '2013-10-19'
                    and fun.device_id <100
                    group by 1,2 --,3
                    
                  ) visitinfo
                on userdevice.uid = visitinfo.uid
--                and userdevice.uid_discovered = visitinfo.uid
         left outer join 
            (    select 'week1' as order_date, -- ao.order_date 
                                ao.uid,
--                                p.browser_id,
--                                p.session_id,
                                sum(order_total-li_amount) as order_total,
                                count(distinct ao.order_id) as num_orders
                            from etl.order_line_items ao
--                                inner join etl.pixel_raw2 p
--                                on regexp_substr(p.json_data,'order_id[\":]*(\w*)', 1, 1, 'b',1) = ao.order_id
--                                and ao.order_date = p.datetime_cst::DATE
                            where ao.order_date between '2013-10-13' and '2013-10-19'  
                                and li_type = 'tax'                            
--                                and p.action_type='insert'
--                                and p.object_type='order'
--                                and p.log_source = 'pixel'                                
                            group by 1,2--,3,4

              ) orders
                 on orders.uid = userdevice.uid

            
group by 1,2,3,4,5,6,7,8,9
having count(distinct visitinfo.uid)>1
order by 
userdevice.asofdate_est
,desktop
,ios_app desc
,mobile_iphone desc
,tablet_ipad desc
,mobile_android desc
,tablet_android desc
,mobile_other desc
,tablet_other desc



-- browser view/ visitor view


select userdevice.asofdate_est
                ,mobile_iphone
                ,mobile_android
                ,mobile_other
                ,tablet_ipad
                ,tablet_android
                ,tablet_other
                ,desktop
                ,ios_app
                ,sum(num_browsers) as visitors
                ,sum(num_sessions) as visits
                ,count(distinct visitinfo.uid) as logged_in_users
--              ,count(distinct visitinfo.uid_discovered) as all_discovered_users
          ,sum(order_total) as revenue
          ,sum(num_orders) as total_num_orders
          ,sum(num_orders)/sum(num_sessions)*100 as CVR          
          ,sum(order_total)/sum(num_orders)  as AOV
            from (
                  select distinct 'week1' as asofdate_est, -- fun.asofdate_est
                        fun.uid, 
                        fun.browser_id,
                        max(case when device_id=20 then 1 else 0 end) as mobile_iphone,
                        max(case when device_id=10 then 1 else 0 end) as mobile_android,
                        max(case when device_id in (21,30,40,50,60) then 1 else 0 end) as mobile_other,
                        max(case when device_id in (22) then 1 else 0 end) as tablet_ipad,
                        max(case when device_id in (11,12) then 1 else 0 end) as tablet_android,
                        max(case when device_id in (31,41) then 1 else 0 end) as tablet_other,
                        max(case when device_id=1 then 1 else 0 end) as desktop,
                        max(case when device_id=23 then 1 else 0 end) as ios_app
                  from rtrbi.daily_user_funnels fun
                    where fun.asofdate_est between '2013-10-13' and '2013-10-19'
                    and fun.device_id <100
                    group by 1,2,3


                ) userdevice
      inner join 
           (
                          select  'week1' as asofdate_est, -- fun.asofdate_est
                        fun.uid, 
                        fun.browser_id,
                        count(distinct fun.session_id) as num_sessions, 
                        count(distinct fun.browser_id) as num_browsers
                    from rtrbi.daily_user_funnels fun
                    where fun.asofdate_est between '2013-10-13' and '2013-10-19'
                    and fun.device_id <100
                    group by 1,2,3
                    
                  ) visitinfo
                on userdevice.uid = visitinfo.uid
                and userdevice.browser_id = visitinfo.browser_id
         left outer join 
            (    select 'week1' as order_date, -- ao.order_date 
                                ao.uid,
                                p.browser_id,
--                                p.session_id,
                                sum(order_total-li_amount) as order_total,
                                count(distinct ao.order_id) as num_orders
                            from etl.order_line_items ao
                                inner join etl.pixel_raw2 p
                                on regexp_substr(p.json_data,'order_id[\":]*(\w*)', 1, 1, 'b',1) = ao.order_id
                                and ao.order_date = p.datetime_cst::DATE
                            where ao.order_date between '2013-10-13' and '2013-10-19'  
                                and li_type = 'tax'                            
                                and p.action_type='insert'
                                and p.object_type='order'
                                and p.log_source = 'pixel'                                
                            group by 1,2,3--,4

              ) orders
                 on orders.uid = userdevice.uid
                 and orders.browser_id    = userdevice.browser_id

            
            group by 1,2,3,4,5,6,7,8,9
--            having  mobile_iphone+mobile_android+mobile_other+tablet_ipad+tablet_android+tablet_other+desktop+ios_app=1
order by 
userdevice.asofdate_est
,desktop
,ios_app desc
,mobile_iphone desc
,tablet_ipad desc
,mobile_android desc
,tablet_android desc
,mobile_other desc
,tablet_other desc