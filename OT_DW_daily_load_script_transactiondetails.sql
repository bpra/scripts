##insert from the reservation booking snapshot for yesterday
delete from rtrbi.ot_ordertransactiondetails where orderDate = date(date_add(curdate(),interval -1 day)) and lineItemtypeId = 1;
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
and os.order_date = date(date_add(curdate(),interval -1 day))
;

##insert giftcards purchased
delete from rtrbi.ot_ordertransactiondetails where orderDate = date(date_add(curdate(),interval -1 day)) and lineItemtypeId = 12;
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
#select * 	
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


## Insert snapshot line items 
delete from rtrbi.ot_ordertransactiondetails where orderDate = date(date_add(curdate(),interval -1 day)) and lineItemtypeId not in (1,12) and statusId = 'ZD';
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
#select * 	
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
and change_UTCtimestamp<= date_add(curdate(),interval -7 day);