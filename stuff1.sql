select productId,count(*) from ot_product group by 1 having count(*)>1
select * from rtrbi.ot_ordertransactiondetails where productId is null and lineitemtypeid in (1,12)
select * from 

insert into rtrbi.ot_ordertransactiondetails
select 
	order_id,
	group_id,
	uid,
	12 as lineItemtypeId,
	op.productId,
	run_timestamp as modifieddate,
	'OC' as statusId,
	1 as storeId,
	case 
		when merchandise_category = 'RENTAL' then 1
		when merchandise_category = 'BULK' then 2
		when merchandise_category = 'CLEARANCE' then 3
		when merchandise_category = 'GIFTCARD' then 2
		else NULL
	end as productordertypeId,
	order_date as order_date,
	qty as qty,
	price_paid as amount
from
(
	select distinct 
		rbs.order_id
		,rbs.group_id
		,rbs.uid
		, case when uop.title like 'Classic Gift Card%' then 'RTR_GC_P' 
		       when uop.title like 'E-Card%' then 'RTR_GC_E' 
		  end as sku
		,rbs.run_timestamp
		,rbs.merchandise_category
		,os.order_date
		,rbs.qty
		,case when rbs.price_paid is null then uop.price else rbs.price_paid end as price_paid
		,uop.order_product_id
	from rtrbi.order_simplereservation_reservation_items_snapshot rbs 
	inner join rtr_prod0808.uc_order_products uop
		on rbs.order_id =uop.order_id
	 inner join rtrbi.order_summary os
		on rbs.order_id = os.order_id	

	where 1=1
	and os.order_date = date(date_add(curdate(),interval -1 day))
	and merchandise_category = 'GIFTCARD'
	and model = ''

)xx
inner join ot_product op
	on binary op.sku= binary xx.sku
where op.activeFlag=1 
; 
select * from analytics.cc_products where type = 'U' and model like 'XX%'
select * from analytics.rtrbi_daily_google_stats where date(run_timestamp) = curdate()
select max(asof_date) from analytics.rtrbi_daily_google_stats
select * from ot_lineitemchangeaudit 
where processed=0 
and change_UTCtimestamp<= cast(@processTime as datetime);
create table select * from analytics.rtrbi_daily_ios_google_stats
(
asof_date date primary key,
new_visits	decimal(32,0),
new_pageViews	decimal(32,0),
new_bounces	decimal(32,0),
new_timeOnSite	decimal(32,0),
new_visitors	decimal(32,0),
new_goal6Completions	decimal(32,0),
new_goal6ConversionRate	double,
returning_visits	decimal(32,0),
returning_pageViews	decimal(32,0),
returning_bounces	decimal(32,0),
returning_timeOnSite	decimal(32,0),
returning_visitors	decimal(32,0),
returning_goal6Completions	decimal(32,0),
returning_goal6ConversionRate	double,
run_timestamp	datetime);

select ga.asof_date,ga.new_visits + ga.returning_visits,ios.new_visits + ios.returning_visits
from analytics.rtrbi_daily_ios_google_stats ios
inner join analytics.rtrbi_daily_google_stats ga
on ga.asof_date = ios.asof_date
where ga.asof_date >='2013-10-01'

;
select
 format(ios.new_visits + ios.returning_visits,0) as total_ios_visits
,format(ga.new_visits + ga.returning_visits,0) as total_web_visits
,format(ga.new_visits + ga.returning_visits + ios.new_visits + ios.returning_visits,0) as total_visits
,format(100 * o.orders / (ga.new_visits + ga.returning_visits + ios.new_visits + ios.returning_visits),2) as conversion

from analytics.rtrbi_daily_ios_google_stats ios
inner join analytics.rtrbi_daily_google_stats ga
on ga.asof_date = ios.asof_date
,(select
 count(*) as orders
from rtr_prod0808.uc_orders uco
inner join rtrbi.order_attributes a on uco.order_id = a.order_id and a.promo_surprise_dress is null
where date(from_unixtime(uco.created)) = date_add(curdate(),interval -1 day)
  and uco.order_status = 'payment_received'
  and uco.primary_email not like '%@renttherunway.com') o
where ga.asof_date = date_add(curdate(),interval -1 day)
;

select 
	rbs.order_id,
	rbs.group_id,
	os.uid,
	1 as lineItemtypeId,
	op.productId,
	rbs.rb_modified_date as modifieddate,
	'OC' as statusId,
	1 as storeId,
	case 
		when rb_merchandise_category = 'RENTAL' then 1
		when rb_merchandise_category = 'BULK' then 2
		when rb_merchandise_category = 'CLEARANCE' then 3
		else NULL
	end as productordertypeId,
	os.order_date as order_date,
	uop.qty as qty,
	real_price as amount
#select * 
from rtrbi.order_reservation_booking_snapshot rbs 
inner join rtr_prod0808.uc_order_products uop
	on rbs.rb_id =uop.booking_id
inner join rtrbi.ot_product op
	on op.sku=rbs.rb_sku
inner join rtrbi.order_summary os
	on rbs.order_id = os.order_id	

where 1=1
and os.order_date = date(date_add(curdate(),interval -1 day))
;

select 
	uo.order_id,
	NULL as group_id,
	uo.uid,
	case 
		when uli.type = 'coupon' then 8
		when uli.type = 'tax' and (title = 'Tax' or title = 'Subscription Tax') then 7
		when uli.type = 'tax' and title = 'Insurance' then 6
		when uli.type = 'ptdiscount' then 102
		when uli.type = 'sub_packages' then 101
		when uli.type = 'rtr_credits_redeemed' then 10 
		when uli.type = 'rtr_credits_refunded' then 9
		when uli.type = 'rtr_refund' then 11
		when uli.type = 'shipping' then 
										(case when title like '%Same%' then 14
											  when title like '%standard%' then 17
											  when title like '%Next Day%' then 15
											  when title like '%Saturday%' then 16
											  when title = 'Same Day Delivery' then 14
											  when title = 'Standard Delivery' then 17
											  when title = 'Standard Delivery' then 17
											  when title = 'insurance' then 6
											  else 17
										  end)
		when uli.type = 'gift_certificate' then 13
		when uli.type = 'generic' and title ='insurance' then 6
		when uli.type = 'generic' and title like '%tax%' then 7
	end

	  as lineItemtypeId,
	NULL as productId,
	from_unixtime(uo.modified) as modifieddate,
	'OC' as statusId,
	1 as storeId,
	NULL as productordertypeId,
	os.order_date as order_date,
	NULL as qty,
	amount as amount
#select * from 
from rtr_prod0808.uc_orders uo
inner join rtr_prod0808.uc_order_line_items uli 
	on uo.order_id = uli.order_id 
inner join rtrbi.order_summary os
	on os.order_id=uo.order_id
inner join rtrbi.z_orderstoprocess otp
	on otp.order_id = uo.order_id
where otp.processed = 0
;

##insert from the reservation booking snapshot
insert into rtrbi.ot_ordertransactiondetails
select 
	rbs.order_id,
	rbs.group_id,
	os.uid,
	1 as lineItemtypeId,
	op.productId,
	rbs.rb_modified_date as modifieddate,
	'OC' as statusId,
	1 as storeId,
	case 
		when rb_merchandise_category = 'RENTAL' then 1
		when rb_merchandise_category = 'BULK' then 2
		when rb_merchandise_category = 'CLEARANCE' then 3
		else NULL
	end as productordertypeId,
	os.order_date as order_date,
	uop.qty as qty,
	real_price as amount
#select * 
from rtrbi.order_reservation_booking_snapshot rbs 
inner join rtr_prod0808.uc_order_products uop
	on rbs.rb_id =uop.booking_id
inner join rtrbi.ot_product op
	on op.sku=rbs.rb_sku
inner join rtrbi.order_summary os
	on rbs.order_id = os.order_id	

where 1=1
	and os.order_date >='2013-07-17'
	and op.activeFlag=1 
	#and rbs.order_id = 693980 
;






## Insert initial snapshot line items 
insert into rtrbi.ot_ordertransactiondetails
select 
	uo.order_id,
	NULL as group_id,
	uo.uid,
	case 
		when uli.li_type = 'coupon' then 8
		when uli.li_type = 'tax' or uli.li_type = 'subscription tax' then 7
		when uli.li_type = 'insurance' then 6
		when uli.li_type = 'ptdiscount' then 102
		when uli.li_type = 'sub_packages' then 101
		when uli.li_type = 'rtr_credits_redeemed' then 10 
		when uli.li_type = 'rtr_credits_refunded' then 9
		when uli.li_type = 'rtr_refund' then 11
		when uli.li_type = 'shipping' then 17
		when uli.li_type = 'gift_certificate' then 13
		when uli.li_type = 'generic' then 7
	end
	  as lineItemtypeId,
	NULL as productId,
	from_unixtime(uli.run_timestamp) as modifieddate,
	'ZD' as statusId,
	1 as storeId,
	NULL as productordertypeId,
	uo.order_date as order_date,
	NULL as qty,
	li_amount as amount
from rtrbi.order_summary uo
inner join rtrbi.order_line_items uli 
	on uo.order_id = uli.order_id 
	and uo.order_date = date(date_add(curdate(),interval -1 day))

;

set @processTime := now();


drop table if exists rtrbi.z_orderstoprocess;

create table rtrbi.z_orderstoprocess
as
select distinct order_id from ot_lineitemchangeaudit 
where processed=0 
and change_UTCtimestamp<= cast(@processTime as datetime);

start transaction;

delete from rtrbi.ot_ordertransactiondetails
where order_id in (select order_id from rtrbi.z_orderstoprocess);

insert into rtrbi.ot_ordertransactiondetails
select 
	uo.order_id,
	NULL as group_id,
	uo.uid,
	case 
		when uli.type = 'coupon' then 8
		when uli.type = 'tax' and (title = 'Tax' or title = 'Subscription Tax') then 7
		when uli.type = 'tax' and title = 'Insurance' then 6
		when uli.type = 'ptdiscount' then 102
		when uli.type = 'sub_packages' then 101
		when uli.type = 'rtr_credits_redeemed' then 10 
		when uli.type = 'rtr_credits_refunded' then 9
		when uli.type = 'rtr_refund' then 11
		when uli.type = 'shipping' then 
										(case when title like '%Same%' then 14
											  when title like '%standard%' then 17
											  when title like '%Next Day%' then 15
											  when title like '%Saturday%' then 16
											  when title = 'Same Day Delivery' then 14
											  when title = 'Standard Delivery' then 17
											  when title = 'Standard Delivery' then 17
											  when title = 'insurance' then 6
											  else 17
										  end)
		when uli.type = 'gift_certificate' then 13
		when uli.type = 'generic' and title ='insurance' then 6
		when uli.type = 'generic' and title like '%tax%' then 7
	end

	  as lineItemtypeId,
	NULL as productId,
	from_unixtime(uo.modified) as modifieddate,
	'OC' as statusId,
	1 as storeId,
	NULL as productordertypeId,
	os.order_date as order_date,
	NULL as qty,
	amount as amount
#select * from 
from rtr_prod0808.uc_orders uo
inner join rtr_prod0808.uc_order_line_items uli 
	on uo.order_id = uli.order_id 
inner join rtrbi.order_summary os
	on os.order_id=uo.order_id
inner join rtrbi.z_orderstoprocess otp
	on otp.order_id = uo.order_id
;

update ot_lineitemchangeaudit 
set processed = 1 
where change_UTCtimestamp<= cast(@processTime as datetime)
and processed = 0;

commit;

delete from ot_lineitemchangeaudit 
where processed = 1
and change_UTCtimestamp<= date_add(curdate(),interval -7 day)


select * from rtrbi.ot_lineitemchangeaudit
delete from 

select * from ot_ordertransactiondetails 
where order_id in (select order_id from z_orderchangelist)
create table z_orderchangelist
as
select order_id,from_unixtime(modified) from rtr_prod0808.uc_orders where from_unixtime(modified)>=curdate())xx

create index IX on z_orderchangelist (order_id)

;create table ot_lineitemchangeaudit
(
order_id bigint,
change_UTCtimestamp datetime,
processed bit
);
select now()
select * from rtrbi.ot_lineitemchangeaudit
create index IX_ot_lineitemchangeaudit_order_id_processed
on ot_lineitemchangeaudit(order_id,processed)

delimiter $$
create trigger TR_lineitemchange_captureoninsert 
after insert on rtr_prod0808.uc_order_line_items
for each row begin

insert into rtrbi.ot_lineitemchangeaudit values(new.order_id,now(),0);

end;$$

delimiter $$
create trigger TR_lineitemchange_captureondelete 
after delete on rtr_prod0808.uc_order_line_items
for each row begin

insert into rtrbi.ot_lineitemchangeaudit values(old.order_id,now(),0);


end;$$

delimiter $$
create trigger TR_lineitemchange_captureonupdate 
after update on rtr_prod0808.uc_order_line_items
for each row begin

insert into rtrbi.ot_lineitemchangeaudit values(old.order_id,now(),0);


end;$$
select * from rtrbi.ot_lineitemchangeaudit

select 
	day(orderDate),
    year(orderDate),
    month(orderDate),
    format(sum(amount), 0) as total_sales,
    format(count(distinct ot.order_id), 0) as total_orders,
    format(sum(amount) / count(distinct ot.order_id),
        2) as aov
from
    ot_ordertransactiondetails ot
        inner join
    rtr_prod0808.uc_orders uo ON ot.order_id = uo.order_id
        inner join
    rtrbi.order_summary os ON os.order_id = uo.order_id
        left join
    rtrbi.order_attributes oa ON uo.order_id = oa.order_id
where
    uo.order_status in ('canceled' , 'payment_received')
        and os.primary_email not like '%renttherunway.com'
        and os.uid > 1
        and lineitemTypeId not in (7 , 9, 11)
        and ((statusId = 'OC'
        and lineitemTypeId in (1 , 12))
        or (statusId = 'ZD'
        and lineitemTypeId not in (1 , 12, 103)))
        and oa.promo_surprise_dress is null
        and (productordertypeId <> 3
        or productordertypeId is null)
and orderDate >= '2013-10-01'
group by 1 , 2, 3;

select 
	day(ot.order_date),
    year(ot.order_date),
    month(ot.order_date),
    format(sum(amount), 0) as total_sales,
    format(count(distinct ot.order_id), 0) as total_orders,
    format(sum(amount) / count(distinct ot.order_id),
        2) as aov
from
   Z_test_ot_ordertransactiondetails ot
        inner join
    rtr_prod0808.uc_orders uo ON ot.order_id = uo.order_id
        inner join
    rtrbi.order_summary os ON os.order_id = uo.order_id
        left join
    rtrbi.order_attributes oa ON uo.order_id = oa.order_id
where
    uo.order_status in ('canceled' , 'payment_received')
        and os.primary_email not like '%renttherunway.com'
        and os.uid > 1
        and lineitemTypeId not in (7 , 9, 11)
        and ((statusId = 'OC'
        and lineitemTypeId in (1 , 12))
        or (statusId = 'ZD'
        and lineitemTypeId not in (1 , 12, 103)))
        and oa.promo_surprise_dress is null
        and (productordertypeId <> 3
        or productordertypeId is null)
and ot.order_date >= '2013-01-01'
group by 1 , 2 , 3;

select 
year(asof_date),month(asof_date), day(asof_date)
,format(sum(scoop_order_value_less_tax), 0) as total_sales
,format(sum(scoop_order_count),0) as total_orders
,format(sum(scoop_order_value_less_tax) / sum(scoop_order_count), 2) as aov
from analytics.rtrbi_daily_order_stats
where asof_date>='2013-01-01'
group by 1,2,3;

select * from ot_lineitemtype












select * from Z_test_ot_ordertransactiondetails
where order_id not in (
select order_id from ot_ordertransactiondetails where orderDate>='2013-10-01')
















insert into  rtrbi.Z_test_ot_ordertransactiondetails

select 
	order_id,
	group_id,
	uid,
	12 as lineItemtypeId,
	op.productId,
	run_timestamp as modifieddate,
	'OC' as statusId,
	1 as storeId,
	case 
		when merchandise_category = 'RENTAL' then 1
		when merchandise_category = 'BULK' then 2
		when merchandise_category = 'CLEARANCE' then 3
		when merchandise_category = 'GIFTCARD' then 2
		else NULL
	end as productordertypeId,
	order_date as order_date,
	qty as qty,
	price_paid as amount
from
(
	select distinct 
		rbs.order_id
		,rbs.group_id
		,rbs.uid
		, case when uop.title like 'Classic Gift Card%' then 'RTR_GC_P' 
		       when uop.title like 'E-Card%' then 'RTR_GC_E' 
		  end as sku
		,rbs.run_timestamp
		,rbs.merchandise_category
		,os.order_date
		,rbs.qty
		,case when rbs.price_paid is null then uop.price else rbs.price_paid end as price_paid
		,uop.order_product_id
	from rtrbi.order_simplereservation_reservation_items_snapshot rbs 
	inner join rtr_prod0808.uc_order_products uop
		on rbs.order_id =uop.order_id
	 inner join rtrbi.order_summary os
		on rbs.order_id = os.order_id	

	where 1=1
		and uo.order_date >= '2013-09-01'
	    and uo.order_date < '2013-10-01'
		#and rbs.order_date <'2013-07-17'
		#and rbs.order_id in (693980,116399,255101,257941) 
		#and os.order_date >='2011-01-01' 
		#and os.order_date <'2011-01-01'
		and merchandise_category = 'GIFTCARD'
		and model = ''

)xx
inner join ot_product op
	on binary op.sku= binary xx.sku
where op.activeFlag=1 
; 



##insert from the reservation booking snapshot
insert into rtrbi.Z_test_ot_ordertransactiondetails
select 
	rbs.order_id,
	rbs.group_id,
	os.uid,
	1 as lineItemtypeId,
	op.productId,
	rbs.rb_modified_date as modifieddate,
	'OC' as statusId,
	1 as storeId,
	case 
		when rb_merchandise_category = 'RENTAL' then 1
		when rb_merchandise_category = 'BULK' then 2
		when rb_merchandise_category = 'CLEARANCE' then 3
		else NULL
	end as productordertypeId,
	os.order_date as order_date,
	uop.qty as qty,
	real_price as amount
#select * 
from rtrbi.order_reservation_booking_snapshot rbs 
inner join rtr_prod0808.uc_order_products uop
	on rbs.rb_id =uop.booking_id
inner join rtrbi.ot_product op
	on op.sku=rbs.rb_sku
inner join rtrbi.order_summary os
	on rbs.order_id = os.order_id	

where 1=1
	and uo.order_date >= '2013-09-01'
	and uo.order_date < '2013-10-01'
	and op.activeFlag=1 
	#and rbs.order_id = 693980 
;





## Insert initial snapshot line items 
insert into  rtrbi.Z_test_ot_ordertransactiondetails
as
select 
	uo.order_id,
	NULL as group_id,
	uo.uid,
	case 
		when uli.li_type = 'coupon' then 8
		when uli.li_type = 'tax' or uli.li_type = 'subscription tax' then 7
		when uli.li_type = 'insurance' then 6
		when uli.li_type = 'ptdiscount' then 102
		when uli.li_type = 'sub_packages' then 101
		when uli.li_type = 'rtr_credits_redeemed' then 10 
		when uli.li_type = 'rtr_credits_refunded' then 9
		when uli.li_type = 'rtr_refund' then 11
		when uli.li_type = 'shipping' then 17
		when uli.li_type = 'gift_certificate' then 13
		when uli.li_type = 'generic' then 7
	end
	  as lineItemtypeId,
	NULL as productId,
	from_unixtime(uli.run_timestamp) as modifieddate,
	'ZD' as statusId,
	1 as storeId,
	NULL as productordertypeId,
	uo.order_date as order_date,
	NULL as qty,
	li_amount as amount
#select *
from rtrbi.order_summary uo
inner join rtrbi.order_line_items uli 
	on uo.order_id = uli.order_id 
	and uo.order_date >= '2013-09-01'
	and uo.order_date < '2013-10-01'

;


##insert current line items
insert into rtrbi.Z_test_ot_ordertransactiondetails
select 
	uo.order_id,
	NULL as group_id,
	uo.uid,
	case 
		when uli.type = 'coupon' then 8
		when uli.type = 'tax' and (title = 'Tax' or title = 'Subscription Tax') then 7
		when uli.type = 'tax' and title = 'Insurance' then 6
		when uli.type = 'ptdiscount' then 102
		when uli.type = 'sub_packages' then 101
		when uli.type = 'rtr_credits_redeemed' then 10 
		when uli.type = 'rtr_credits_refunded' then 9
		when uli.type = 'rtr_refund' then 11
		when uli.type = 'shipping' then 
										(case when title like '%Same%' then 14
											  when title like '%standard%' then 17
											  when title like '%Next Day%' then 15
											  when title like '%Saturday%' then 16
											  when title = 'Same Day Delivery' then 14
											  when title = 'Standard Delivery' then 17
											  when title = 'Standard Delivery' then 17
											  when title = 'insurance' then 6
											  else 17
										  end)
		when uli.type = 'gift_certificate' then 13
		when uli.type = 'generic' and title ='insurance' then 6
		when uli.type = 'generic' and title like '%tax%' then 7
	end

	  as lineItemtypeId,
	NULL as productId,
	from_unixtime(uo.modified) as modifieddate,
	'OC' as statusId,
	1 as storeId,
	NULL as productordertypeId,
	os.order_date as order_date,
	NULL as qty,
	amount as amount
#select * from 
from rtr_prod0808.uc_orders uo
inner join rtr_prod0808.uc_order_line_items uli 
	on uo.order_id = uli.order_id 
inner join rtrbi.order_summary os
	on os.order_id=uo.order_id
	and os.order_date >= '2013-10-01'
	#and os.order_date >= '2011-01-01'


;


select * from rtr_prod0808.uc_orders 
where order_id not in (select order_id from rtrbi.order_line_items)

select
 format(count(distinct group_id),0) as expected_shipments

from rtr_prod0808.rtr_order_groups as rog
inner join rtr_prod0808.uc_orders as uco on uco.order_id = rog.order_id
inner join rtr_prod0808.uc_order_products as ucop on uco.order_id = ucop.order_id
inner join rtr_prod0808.reservation_booking as srr on ucop.booking_id = srr.id

where uco.order_status in ('payment_received','staff_order')
and date(srr.expected_begin_date) = curdate();

select
 format(count(distinct group_id),0) as expected_shipments

from rtr_prod0808.rtr_order_groups as rog
inner join rtr_prod0808.uc_orders as uco on uco.order_id = rog.order_id
inner join rtr_prod0808.uc_order_products as ucop on uco.order_id = ucop.order_id
inner join rtr_prod0808.simplereservation_reservation as srr on ucop.product_rid = srr.rid

where uco.order_status in ('payment_received','staff_order')
and date(from_unixtime(srr.begin)) = curdate();

select * from rtr_prod0808.reservation_booking where user_id =  3510689 limit 100
select from_unixtime(begin),from_unixtime(end) from rtr_prod0808.simplereservation_reservation where uid =  3510689
select from_unixtime(begin),from_unixtime(end) from rtr_prod0808.simplereservation_reservation


select * from information_schema.tables where table_name like 'tracking_code'

select * from rtr_prod0808.tracking_code limit 10

select * 
from rtr_prod0808.fulfillment 
where status = 'READY_FOR_PICKUP' 
limit 10


select count(*) 
from rtr_prod0808.fulfillment f
inner join rtr_prod0808.reservation_booking rb
on f.booking_id = rb.id
where f.status = 'READY_FOR_PICKUP' 
and expected_begin_date = '2014-02-03'
