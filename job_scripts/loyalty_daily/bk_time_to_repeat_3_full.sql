-- Loyalty Table Special Version that excludes clearance and includes canceled orders
SET time_zone = 'US/Eastern';

Drop Table if exists analytics.bk_clr;

-- Old method

Create Table analytics.bk_clr as
Select distinct order_id
from rtr_prod0808.uc_order_products uop
where  uop.model like 'clearance%';

-- New method

Insert into analytics.bk_clr
Select distinct order_id
from rtr_prod0808.uc_order_products uop
join rtr_prod0808.reservation_booking rb on uop.booking_id=rb.id
where merchandise_category='CLEARANCE';

alter table analytics.bk_clr add primary key (order_id); 

Drop Table if exists analytics.bk_non_clr;

Create Table analytics.bk_non_clr as
Select distinct uo.order_id
from rtr_prod0808.uc_orders uo
left outer join analytics.bk_clr bk
on uo.order_id=bk.order_id
where bk.order_id is null; 

alter table analytics.bk_non_clr add primary key (order_id);

Drop Table if exists analytics.bk_cust_counter_3;

Create Table analytics.bk_cust_counter_3 as
Select 
uo.uid,
bb.channel,
case when rj.city in ('NEW YORK','BROOKLYN','STATEN ISLAND','BRONX', 'QUEENS') and rj.state = 'New York' then 'NEW YORK' 
  when rj.city = 'WASHINGTON' and rj.state = 'District of Columbia' then 'WASHINGTON'
  when rj.city = 'LOS ANGELES' and rj.state = 'California' then 'LOS ANGELES'
  when rj.city = 'MIAMI' and rj.state = 'Florida' then 'MIAMI'
  when rj.city = 'CHICAGO' and rj.state = 'Illinois' then 'CHICAGO'
  when rj.city = 'SAN FRANCISCO' and rj.state = 'California' then 'SAN FRANCISCO'
  when rj.city = 'HOUSTON' and rj.state = 'Texas' then 'HOUSTON'
  when rj.city = 'DALLAS' and rj.state = 'Texas' then 'DALLAS'
  when rj.city = 'PHILADELPHIA' and rj.state = 'Pennsylvania' then 'PHILADELPHIA'
  when rj.city = 'ATLANTA' and rj.state = 'Georgia' then 'ATLANTA'
  when rj.city = 'BOSTON' and rj.state = 'Massachusetts' then 'BOSTON'
  else null end as city,
case when ue.dob is null then null else
from_unixtime(ue.dob,'%Y-%m-%d') end as birthday,
case when ue.dob is null then null else
truncate((ue.created-ue.dob)/(60*60*24*365),2) end as join_age,
case when ue.dob is null then null else
truncate((uo.created-ue.dob)/(60*60*24*365),2) end as 1st_ord_age,
from_unixtime(min(ue.created),'%Y-%m') join_mth,
from_unixtime(min(uo.created),'%Y-%m') cust_mth,
min(uo.created)+(90*24*60*60) first_plus_90,
min(uo.created)+(180*24*60*60) first_plus_180,
min(uo.created)+(365*24*60*60) first_plus_365,
min(uo.created) first_ord,
count(distinct uo.order_id) orders
from rtr_prod0808.uc_orders uo
inner join rtrbi.users ue
on uo.uid=ue.uid
inner join 
analytics.bk_non_clr clr
on uo.order_id=clr.order_id
inner join
rtrbi.order_attributes oa
on uo.order_id=oa.order_id
left outer join
analytics.bb_channel bb
on uo.uid=bb.uid
left outer join
analytics.bb_geo rj
on uo.uid=rj.uid
where uo.order_status in ('payment_received','canceled')
and uo.primary_email not like '%renttherunway.com'
and ue.is_rtr_email=0
and oa.promo_surprise_dress is null
and oa.promo_outfit_of_the_month is null
and oa.promo_office_order is null
and oa.promo_free_dress is null
group by uid; 

alter table analytics.bk_cust_counter_3 add primary key (uid);

Drop Table if exists analytics.bk_order_to_rank_3;

Create Table analytics.bk_order_to_rank_3 as
Select uo.uid,
uo.created,
uo.order_total,
line_items.*
from rtrbi.order_summary uo
join
(Select
ui.order_id,
sum(if( ui.li_type='gift_certificate', ui.li_amount, 0) ) as gift_certificate,
sum(if( ui.li_type='coupon', ui.li_amount, 0) ) as coupon,
sum(if( ui.li_type='rtr_credits_redeemed', ui.li_amount, 0) ) as rtr_credits_redeemed,
sum(if( ui.li_type='rtr_credits_refunded', ui.li_amount, 0) ) as rtr_credits_refunded,
sum(if( ui.li_type='rtr_refund', ui.li_amount, 0) ) as rtr_refund,
sum(if( ui.li_type='shipping', ui.li_amount, 0) ) as shipping,
sum(if( ui.li_type='Tax', ui.li_amount, 0) ) as Tax,
sum(if( ui.li_type='Insurance', ui.li_amount, 0) ) as Insurance
from rtrbi.order_line_items ui
group by order_id) as line_items
on uo.order_id=line_items.order_id
inner join rtrbi.order_attributes oa on uo.order_id=oa.order_id
inner join analytics.bk_non_clr nc on uo.order_id=nc.order_id
where uo.order_status in ('payment_received','canceled')
and uo.primary_email not like '%renttherunway.com'
and uo.uid>1
and uo.is_rtr_email=0
and oa.promo_surprise_dress is null
and oa.promo_outfit_of_the_month is null
and oa.promo_office_order is null
and oa.promo_free_dress is null;

Drop Table if exists analytics.bk_ranked_ords_3;

Set @rownum:=0;
Set @last_uid:=-1; 

Create table analytics.bk_ranked_ords_3 as
Select
bk.uid,
bk.order_id,
bk.created,
from_unixtime(bk.created,'%Y-%m-%d') order_date,
from_unixtime(bk.created,'%Y-%m') order_mth,
@rownum := case when @last_uid = uid then @rownum + 1
else 1 end AS rank,
@last_uid:= uid,
bk.order_total,
bk.gift_certificate,
bk.coupon,
bk.rtr_credits_redeemed,
bk.rtr_credits_refunded,
bk.rtr_refund,
bk.shipping,
bk.Tax,
bk.Insurance
from analytics.bk_order_to_rank_3 bk
order by uid, created;

Drop Table if exists analytics.bk_time_to_repeat_3;

Create Table analytics.bk_time_to_repeat_3 as
Select
bk1.*,
bk2.order_id,
bk2.created,
bk2.order_date,
bk2.order_mth,
bk2.rank,
bk2.order_total,
bk2.gift_certificate,
bk2.coupon,
bk2.rtr_credits_redeemed,
bk2.rtr_credits_refunded,
bk2.rtr_refund,
bk2.shipping,
bk2.Tax,
bk2.Insurance,
bk2.order_total-(bk2.gift_certificate+bk2.rtr_credits_redeemed+bk2.rtr_credits_refunded+bk2.rtr_refund+bk2.Insurance+bk2.shipping+bk2.coupon+bk2.tax) product_total,
bk2.order_total-(bk2.gift_certificate+bk2.rtr_credits_redeemed+bk2.rtr_credits_refunded+bk2.rtr_refund+bk2.tax) adjusted_order_total,
bk2.order_total-coalesce(bk2.tax,0) tax_exc_total
from analytics.bk_cust_counter_3 bk1
inner join analytics.bk_ranked_ords_3 bk2
on bk1.uid=bk2.uid; 

alter table analytics.bk_time_to_repeat_3 add primary key (order_id);

Create index u3_dx on analytics.bk_time_to_repeat_3 (uid);

Create index c3_dx on analytics.bk_time_to_repeat_3 (created); 
