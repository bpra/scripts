-- Loyalty Table Special Version that excludes clearance and includes canceled orders
set time zone = 'US/Eastern';

-- ORDERS of CLEARANCE PRODUCTS

-- Old method of declaring clearance
drop table if exists rtrbi.loyalty_clearance_temp;
create Table rtrbi.loyalty_clearance_temp as
select distinct order_id
from etl.uc_order_products uop
where uop.model ilike 'clearance%';

-- New method of declaring clearance products
insert into rtrbi.loyalty_clearance_temp
select distinct order_id
from etl.uc_order_products uop
join etl.reservation_booking rb 
    on uop.booking_id = rb.id
where merchandise_category = 'CLEARANCE';

alter table rtrbi.loyalty_clearance_temp add primary key (order_id); 

-- NON CLEARANCE ORDERS
drop table if exists rtrbi.loyalty_non_clearance_temp;
create table rtrbi.loyalty_non_clearance_temp as
select distinct uo.order_id
from etl.uc_orders uo
left outer join rtrbi.loyalty_clearance_temp bk
    on uo.order_id  = bk.order_id
where bk.order_id is null; 

alter table rtrbi.loyalty_non_clearance_temp add primary key (order_id);

-- CUSTOMER COUNTER
drop table if exists rtrbi.loyalty_customer_counter;

create table rtrbi.loyalty_customer_counter as
select 
    uo.uid,
    min(case when ue.dob_date is null then null else
        ue.dob_date end) as birthday,
    min(case when ue.dob_date is null then null else
        round((ue.created-extract(epoch from ue.dob_date))/(60*60*24*365.25),2.0) end) as join_age,
    min(case when ue.dob_date is null then null else
        round((uo.created-extract(epoch from ue.dob_date))/(60*60*24*365.25),2.0) end) as first_order_age,
    to_char(to_timestamptz(min(ue.created)),'YYYY-MM') join_month,
    min(fiscal_join.fiscal_month_id) as fiscal_join_month_id,
    to_char(to_timestamptz(min(uo.created)),'YYYY-MM') cust_month,
    min(fiscal_cust.fiscal_month_id) as fiscal_cust_month_id,
    to_timestamptz(min(uo.created)) first_order,
    to_timestamptz(min(uo.created))::date first_order_date,
    count(distinct uo.order_id) orders
from etl.uc_orders uo
inner join etl.users ue
    on uo.uid = ue.uid
inner join rtrbi.loyalty_non_clearance_temp clr
    on uo.order_id = clr.order_id
inner join rtrbi.order_attributes oa
    on uo.order_id = oa.order_id
inner join rtrbi.dates fiscal_join
    on to_timestamptz(ue.created)::date = fiscal_join.asofdate
inner join rtrbi.dates fiscal_cust
    on to_timestamptz(uo.created)::date = fiscal_cust.asofdate
where uo.order_status in ('payment_received','canceled')
    and uo.primary_email not like '%renttherunway.com'
    and ue.is_rtr_email=0
    and oa.promo_surprise_dress is null
    and oa.promo_outfit_of_the_month is null
    and oa.promo_office_order is null
    and oa.promo_free_dress is null
group by 1; 

alter table rtrbi.loyalty_customer_counter add primary key (uid);

-- 

drop table if exists rtrbi.loyalty_orders_temp;

create table rtrbi.loyalty_orders_temp as
select 
    uo.uid,
    to_timestamptz(uo.created) created,
    round(uo.order_total,3.0) order_total,
    line_items.*
from etl.order_summary uo
join (
    select
        ui.order_id,
        sum(case when ui.li_type='gift_certificate' then ui.li_amount else 0 end) as gift_certificate,
        sum(case when ui.li_type='coupon' then ui.li_amount else 0 end) as coupon,
        sum(case when ui.li_type='rtr_credits_redeemed' then ui.li_amount else 0 end) as rtr_credits_redeemed,
        sum(case when ui.li_type='rtr_credits_refunded' then ui.li_amount else 0 end) as rtr_credits_refunded,
        sum(case when ui.li_type='rtr_refund' then ui.li_amount else 0 end) as rtr_refund,
        sum(case when ui.li_type='shipping' then ui.li_amount else 0 end) as shipping,
        sum(case when ui.li_type='tax' then ui.li_amount else 0 end) as tax,
        sum(case when ui.li_type='insurance' then ui.li_amount else 0 end) as insurance
    from etl.order_line_items ui
    group by order_id
    ) as line_items
    on uo.order_id=line_items.order_id
inner join rtrbi.order_attributes oa 
    on uo.order_id=oa.order_id
inner join rtrbi.loyalty_non_clearance_temp nc 
    on uo.order_id=nc.order_id
where uo.order_status in ('payment_received','canceled')
    and uo.primary_email not like '%renttherunway.com'
    and uo.uid > 1
    and uo.is_rtr_email = 0
    and oa.promo_surprise_dress is null
    and oa.promo_outfit_of_the_month is null
    and oa.promo_office_order is null
    and oa.promo_free_dress is null;

-- ORDER RANKINGS

Drop Table if exists rtrbi.loyalty_orders_ranked;

create table rtrbi.loyalty_orders_ranked as
select
    bk.uid,
    bk.order_id,
    bk.created,
    to_char(bk.created,'YYYY-MM-dd') order_date,
    to_char(bk.created,'YYYY-MM') order_month,
    fiscal_order.fiscal_month_id as fiscal_order_month_id,
    rank() over (partition by uid order by bk.created) AS rank,
    bk.order_total,
    bk.gift_certificate,
    bk.coupon,
    bk.rtr_credits_redeemed,
    bk.rtr_credits_refunded,
    bk.rtr_refund,
    bk.shipping,
    bk.tax,
    bk.insurance
from rtrbi.loyalty_orders_temp bk
inner join rtrbi.dates fiscal_order
    on fiscal_order.asofdate = bk.created::date
order by uid, created;

-- TIME TO REPEAT 

drop table if exists rtrbi.loyalty_time_to_repeat;

create table rtrbi.loyalty_time_to_repeat as
select
    bk1.*,
    bk2.order_id,
    -- bk2.created,
    bk2.order_date::date,
    bk2.order_month,
    bk2.rank,
    bk2.order_total,
    bk2.gift_certificate,
    bk2.coupon,
    bk2.rtr_credits_redeemed,
    bk2.rtr_credits_refunded,
    bk2.rtr_refund,
    bk2.shipping,
    bk2.tax,
    bk2.insurance,
    -- bk2.order_total-(bk2.gift_certificate+bk2.rtr_credits_redeemed+bk2.rtr_credits_refunded+bk2.rtr_refund+bk2.Insurance+bk2.shipping+bk2.coupon+bk2.tax) product_total,
    -- bk2.order_total-(bk2.gift_certificate+bk2.rtr_credits_redeemed+bk2.rtr_credits_refunded+bk2.rtr_refund+bk2.tax) adjusted_order_total,
    bk2.order_total-coalesce(bk2.tax,0) tax_exc_total,
    (bk2.order_total-coalesce(bk2.tax,0)) - (bk2.rtr_credits_redeemed + bk2.gift_certificate) as commission_total,
    (bk2.order_total-coalesce(bk2.tax,0)) - (bk2.rtr_credits_redeemed + bk2.gift_certificate + bk2.coupon) as undiscounted_gross
from rtrbi.loyalty_customer_counter bk1
inner join rtrbi.loyalty_orders_ranked bk2
    on bk1.uid=bk2.uid;

alter table rtrbi.loyalty_time_to_repeat add primary key (order_id);

grant all on rtrbi.loyalty_orders_ranked to analytics_role with grant option;
grant all on rtrbi.loyalty_orders_ranked to etl_role with grant option;

grant all on rtrbi.loyalty_time_to_repeat to analytics_role with grant option;
grant all on rtrbi.loyalty_time_to_repeat to etl_role with grant option;

grant all on rtrbi.loyalty_customer_counter to analytics_role with grant option;
grant all on rtrbi.loyalty_customer_counter to etl_role with grant option;
