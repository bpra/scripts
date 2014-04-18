
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
		#and os.order_date >='2013-01-01' 
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


insert into rtrbi.ot_ordertransactiondetails
select 
	rbs.order_id,
	rbs.group_id,
	os.uid,
	1 as lineItemtypeId,
	op.productId,
	rbs.run_timestamp as modifieddate,
	'OC' as statusId,
	1 as storeId,
	case 
		when merchandise_category = 'RENTAL' then 1
		when merchandise_category = 'BULK' then 2
		when merchandise_category = 'CLEARANCE' then 3
		else NULL
	end as productordertypeId,
	os.order_date as order_date,
	qty as qty,
	price_paid as amount

from rtrbi.order_simplereservation_reservation_items_snapshot rbs 
inner join rtrbi.ot_product op
	on op.sku= rbs.sku
inner join rtrbi.order_summary os
	on rbs.order_id = os.order_id	
where 1=1
	#and os.order_date >='2013-01-01' 
	#and os.order_date <'2013-07-17'
	#and order_id = 808382
	#and os.order_date >='2011-01-01' 
	and os.order_date <'2011-01-01'
	and op.activeFlag=1 
	#and os.order_date ='2012-12-31'
; 


## insert old clearance items.
insert into rtrbi.ot_ordertransactiondetails
select 
	rbs.order_id,
	rbs.group_id,
	os.uid,
	1 as lineItemtypeId,
	op.productId,
	rbs.run_timestamp as modifieddate,
	'OC' as statusId,
	1 as storeId,
	3 #CLEARANCE
	 as productordertypeId,
	os.order_date as order_date,
	qty as qty,
	price_paid as amount
#select * 
from rtrbi.order_simplereservation_reservation_items_snapshot rbs 
inner join rtrbi.order_summary os
	on rbs.order_id = os.order_id	
inner join analytics.z_clearProducts clear
	on rbs.sku=clear.sku
inner join ot_product op
	on op.sku= clear.cleaned_sku
where 1=1
	#and os.order_date >='2013-01-01' 
	#and os.order_date <'2013-07-17'
	and is_clearancesku =1
	and op.activeFlag=1 
	#and order_id = 808382
	#and os.order_date >='2011-01-01' 
	and os.order_date <'2011-01-01'
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
#select *
from rtrbi.order_summary uo
inner join rtrbi.order_line_items uli 
	on uo.order_id = uli.order_id 
	#and uo.order_date >= '2013-01-01'
	#and uo.order_date >= '2011-01-01'
	and uo.order_date < '2011-01-01'
;


##insert current line items
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
	#and os.order_date >= '2013-01-01'
	#and os.order_date >= '2011-01-01'
	and os.order_date < '2011-01-01'

;


create index IX_PK_candidate on ot_ordertransactiondetails(uid,order_id,lineItemTypeId,productId,statusId,storeId,productOrderTypeId,orderDate,group_id);


##insert snapshot order total
insert into rtrbi.ot_ordertransactiondetails
select 
	oli.order_id,
	NULL as group_id,
	oli.uid,
	103 as lineItemtypeId,
	NULL as productId,
	run_timestamp as modifieddate,
	'ZD' as statusId,
	1 as storeId,
	NULL as productordertypeId,
	order_date as order_date,
	NULL as qty,
	order_total as amount
	#select * 
from rtrbi.order_summary oli
where order_date>='2013-01-01';