\set ON_ERROR_STOP on

drop table if exists etl.sku_revenue_allocation;

select distinct ord.order_id, ord.order_date, ord.order_total, ord.non_rental_product_total, 
       orig.order_original_total_price,
       rb.order_status, rb.group_id, rb.reservation_booking_id, rb.expected_begin_date, rb.last_possible_begin_date, rb.rental_begin_date, 
       rb.product_type, rb.sku, rb.style, rb.list_price, rb.cost, rb.sell_price, rb.style_active_units,
       prt.treatment as pricing_test_treatment
into etl.sku_revenue_allocation
from (  -- ----------------------------------------------- Part 1: To Calculate Order Value & Order Valuse w/o Saleable Items
select ord.order_id, date(ord.orderDate-interval '5 hour') as order_date, sum(ord.amount) as order_total, 
	   sum(case when prdt.product_type in ('B','GC','PP') then ord.amount else 0 end) as non_rental_product_total
from rtrbi.ot_orderdata as ord
inner join rtrbi.ot_lineitemtype lit
on ord.lineItemTypeId = lit.lineItemTypeId
left join rtrbi.ot_product as prd
on ord.productId = prd.productId
left join rtrbi.ot_product_type as prdt
on prd.product_type = prdt.product_type
where ord.viewId = 'ZD' 
  and ord.lineItemTypeId != 7
-- and date(ord.orderDate-interval '5 hour') >= '2014-01-05'
group by 1,2) as ord
inner join ( -- ---------------------------------------------- Part 2: To Calculate total original prices of rentable items in the order
select uco.order_id, sum(case when pr.product_type in ('A','D') then pr.sell_price else 0 end) as order_original_total_price
from etl.uc_orders as uco
inner join etl.users as u
on uco.uid = u.uid
inner join etl.rtr_order_groups as rog
on uco.order_id = rog.order_id
inner join etl.reservation_booking as rb
on rog.group_id = rb.order_group_id
inner join etl.reservation_booking_detail as rbd
on rb.id = rbd.reservation_booking_id
inner join ( 
select distinct its.from_date, prd.product_type, prd.style, prd.sku, prd.list_price, prd.cost, its.sell_price
from etl.inventory_time_series as its
inner join rtrbi.ot_product as prd
on its.sku = prd.sku
where prd.product_type in ('B','GC','PP','A','D')
  and its.on_site = 1) as pr
on date(to_timestamptz(uco.created)-interval '5 hour') = pr.from_date and
   rb.sku = pr.sku
where uco.order_status in ('payment_received', 'canceled')
  and rog.group_id > 0 
  and rb.merchandise_category = 'RENTAL'
  and rb.status + rb.type = 0
  and u.mail not like '%renttherunway.com'
group by 1) as orig
on ord.order_id = orig.order_id
inner join (  -- ------------------------------------------------ Part 3: Booking Level information of order, including 'A', 'D' and Saleables
select distinct date(to_timestamptz(uco.created)-interval '5 hour') as order_date, uco.order_id, uco.order_status,
	   rog.group_id, rb.id as reservation_booking_id, rb.expected_begin_date, rb.last_possible_begin_date, rbd.rental_begin_date,
	   pr.product_type, rb.sku, pr.style, pr.list_price, pr.cost, pr.sell_price, pr.active_units as style_active_units
from etl.uc_orders as uco
inner join etl.users as u
on uco.uid = u.uid
inner join etl.rtr_order_groups as rog
on uco.order_id = rog.order_id
inner join etl.reservation_booking as rb
on rog.group_id = rb.order_group_id
inner join etl.reservation_booking_detail as rbd
on rb.id = rbd.reservation_booking_id
inner join rtrbi.ot_product as prd
on rb.sku = prd.sku
inner join (
select its.from_date, prd.product_type, prd.style, prd.list_price, prd.cost, its.sell_price, sum(its.active_units) as active_units
from etl.inventory_time_series as its
inner join rtrbi.ot_product as prd
on its.sku = prd.sku
where prd.product_type in ('B','GC','PP','A','D')
  and its.on_site = 1
group by 1,2,3,4,5,6) as pr
on date(to_timestamptz(uco.created)-interval '5 hour') = pr.from_date and
   prd.style = pr.style
where uco.order_status in ('payment_received', 'canceled')
  and rog.group_id > 0 
  and rb.merchandise_category = 'RENTAL'
  and rb.status + rb.type = 0
  and u.mail not like '%renttherunway.com'
union
select distinct date(to_timestamptz(uco.created)-interval '5 hour') as order_date, uco.order_id, uco.order_status,
	   rog.group_id, rb.id as reservation_booking_id, rb.expected_begin_date, rb.last_possible_begin_date, rbd.rental_begin_date,
	   p.type as product_type, rb.sku, p.style, p.list_price, p.cost, p.sell_price, 0 as style_active_units
from etl.uc_orders as uco
inner join etl.users as u
on uco.uid = u.uid
inner join etl.rtr_order_groups as rog
on uco.order_id = rog.order_id
inner join etl.reservation_booking as rb
on rog.group_id = rb.order_group_id
inner join etl.reservation_booking_detail as rbd
on rb.id = rbd.reservation_booking_id
inner join analytics.products_iid as pi
on rb.sku = pi.sku
inner join analytics.products as p
on pi.style = p.style
where uco.order_status in ('payment_received', 'canceled')
  and rog.group_id > 0 
  and rb.merchandise_category = 'BULK'
  and rb.status + rb.type = 0
  and u.mail not like '%renttherunway.com') as rb
on orig.order_id = rb.order_id
left join analytics.vr_pricing_stylelist as prt
on rb.style = prt.styleName and
   prt.asof_date = date(date_trunc('week',current_date)-interval '1 day');
commit;   

grant all on etl.sku_revenue_allocation to analytics_role with grant option;
