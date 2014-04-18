set time_zone = 'America/New_York';


-- set starting and ending points
select
 @start_date := date_sub(curdate(),interval 1 day)
,@end_date := date_sub(curdate(),interval 1 day)
;
start transaction;
-- @DATASET {"id":"temp uc_orders"}
-- build a temp uc_orders table so we don't lock replication
drop table if exists analytics.rtrbi_daily_uc_orders_temp;

create table analytics.rtrbi_daily_uc_orders_temp
like rtr_prod0808.uc_orders;

alter table analytics.rtrbi_daily_uc_orders_temp
 add column is_rtr_email tinyint unsigned default 0;

insert into analytics.rtrbi_daily_uc_orders_temp
select
 uco.*
,case when uco.primary_email like '%renttherunway.com' then 1 else 0 end as is_rtr_email

from rtr_prod0808.uc_orders uco

where uco.order_status in ('payment_received','canceled','staff_order')

and not exists
(select 1
from rtrbi.order_summary x
where uco.order_id = x.order_id);

-- calculate amounts paid from uc_order_products
drop table if exists analytics.rtrbi_daily_uc_order_products_temp_key;

create table analytics.rtrbi_daily_uc_order_products_temp_key
(
  `order_product_id` int(10) unsigned NOT NULL DEFAULT '0',
  `booking_id` int(11) NOT NULL DEFAULT '0',
  primary key(order_product_id),
  unique key(booking_id)
);

-- insert keys for primary booking
insert into analytics.rtrbi_daily_uc_order_products_temp_key
(booking_id,order_product_id)
select
 ucop.booking_id
,max(ucop.order_product_id) as order_product_id
from analytics.rtrbi_daily_uc_orders_temp t
inner join rtr_prod0808.uc_order_products ucop on t.order_id = ucop.order_id
where ucop.booking_id > 0
group by 1;

-- calculate amounts paid from uc_order_products
drop table if exists analytics.rtrbi_daily_uc_order_products_temp;

create table analytics.rtrbi_daily_uc_order_products_temp
(
  `order_product_id` int(10) unsigned NOT NULL DEFAULT '0',
  `order_id` int(10) unsigned NOT NULL DEFAULT '0',
  `nid` int(10) unsigned NOT NULL DEFAULT '0',
  `model` varchar(255) CHARACTER SET utf8 NOT NULL DEFAULT '',
  `sku` varchar(255) DEFAULT NULL,
  `merchandise_category` enum('RENTAL','CLEARANCE','BULK') DEFAULT 'RENTAL',
  `combo_type` varchar(3) DEFAULT NULL,
  `special_type` varchar(3) DEFAULT NULL,
  `type` varchar(2) DEFAULT NULL,
  `product_rid` int(11) NOT NULL,
  `primary_rid` int(11) NOT NULL,
  `booking_id` int(11) NOT NULL DEFAULT '0',
  `primary_booking_id` int(11) NOT NULL DEFAULT '0',
  `qty` double(10,2) unsigned NOT NULL DEFAULT '0.00',
  `price` decimal(15,3) NOT NULL DEFAULT '0.000',
  `sell_price` decimal(15,3) NOT NULL,
  `backup_size` varchar(1) CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  `backup_style` varchar(1) CHARACTER SET utf8 NOT NULL DEFAULT 'N',
  `price_paid` double(20,3) DEFAULT NULL,
  `rental_price` decimal(15,3) DEFAULT NULL,
	key(order_product_id),
	unique key(booking_id),
  key(primary_booking_id),
  key(product_rid),
  key(primary_rid)
)
;

insert into analytics.rtrbi_daily_uc_order_products_temp
select 
 ucop.order_product_id
,ucop.order_id
,ucop.nid
,ucop.model
,rb.sku
,rb.merchandise_category
,case 
	when rb.merchandise_category = 'CLEARANCE' then concat('C',p.type)
	when p.special_type != 'C' then p.combo_type
	else p.type end as combo_type
,case 
	when rb.merchandise_category = 'CLEARANCE' then 'C'
	when p.special_type != 'C' then p.special_type
	else '' end as special_type
,p.type
,ucop.product_rid
,ucop.product_rid as primary_rid
,ucop.booking_id
,ucop.booking_id as primary_booking_id
,ucop.qty
,ucop.price
,p.sell_price
,0 as backup_size
,if(ucop.backup='Y',1,0) as backup_style
,case 
	when rb.merchandise_category = 'CLEARANCE' then ucop.price
--	when p.type = 'D' and coalesce(srr.backup_rid,0) <> 0 then 0 -- backup size
	when p.type = 'D' and ucop.backup = 'Y' then ucop.price -- backup style
	when p.type = 'D' then ucop.qty * ucop.price -- normal dress rental
	when p.type = 'A' then ucop.qty * ucop.price -- normal accessory rental
	when p.type = 'U' then ucop.price -- saleable
	else null end -- should never get here
 as price_paid
,case when rb.merchandise_category != 'CLEARANCE' then p.sell_price -- all rentable or upsell
 else ucop.price end as rental_price -- clearance
from analytics.rtrbi_daily_uc_orders_temp t
inner join rtr_prod0808.uc_order_products ucop on t.order_id = ucop.order_id
inner join analytics.rtrbi_daily_uc_order_products_temp_key k on ucop.order_product_id = k.order_product_id
inner join rtr_prod0808.reservation_booking rb on ucop.booking_id = rb.id
inner join rtrbi.products_master_iid p on rb.sku = p.sku
;

-- add backup bookings
insert into analytics.rtrbi_daily_uc_order_products_temp
select 
 ucop.order_product_id
,ucop.order_id
,ucop.nid
,ucop.model
,rb.sku
,rb.merchandise_category
,case 
	when rb.merchandise_category = 'CLEARANCE' then concat('C',p.type)
	when p.special_type != 'C' then p.combo_type
	else p.type end as combo_type
,case 
	when rb.merchandise_category = 'CLEARANCE' then 'C'
	when p.special_type != 'C' then p.special_type
	else '' end as special_type
,p.type
,ucop.product_backup_rid as product_rid
,ucop.product_rid as primary_rid
,ucop.backup_booking_id as booking_id
,ucop.booking_id as primary_booking_id
,ucop.qty
,ucop.price
,p.sell_price
,1 as backup_size
,if(ucop.backup='Y',1,0) as backup_style
,case 
	when p.type = 'D' then 0 -- backup size
	else null end -- should never get here
 as price_paid
,ucop.price as rental_price -- clearance
from analytics.rtrbi_daily_uc_orders_temp t
inner join rtr_prod0808.uc_order_products ucop on t.order_id = ucop.order_id and ucop.backup_booking_id > 0
inner join analytics.rtrbi_daily_uc_order_products_temp_key k on ucop.order_product_id = k.order_product_id
inner join rtr_prod0808.reservation_booking rb on ucop.backup_booking_id = rb.id
inner join rtrbi.products_master_iid p on rb.sku = p.sku
;

-- old srr snapshot
insert ignore into rtrbi.order_simplereservation_reservation_items_snapshot
(
 order_date
,ship_out_date
,rent_begin_date
,uid
,primary_email
,is_rtr_email
,order_id
,order_status
,group_id
,rid
,backup_rid
,nid
,iid
,style_name
,size
,sku
,combo_type
,special_type
,type
,backup_size
,backup_style
,qty
,price
,sell_price
,price_paid
,rental_price
,merchandise_category
,booking_id
)
-- gift cards
select
 date(from_unixtime(uco.created)) as order_date
,date(from_unixtime(coalesce(ugc.mail_date,uco.created))) as ship_out_date
,date(from_unixtime(coalesce(ugc.mail_date,uco.created))) as rent_begin_date

,uco.uid
,uco.primary_email
,uco.is_rtr_email
,uco.order_id
,uco.order_status
,coalesce(rog.group_id,-uco.order_id) as group_id

,ucop.order_product_id as rid
,null as backup_rid
,null as nid
,null as iid
,null as style_name
,null as size
,null as sku

,'GC' as combo_type
,'G' as special_type
,'G' as type
,0 as backup_size
,0 as backup_style

,ucop.qty
,ucop.price
,ucop.price as sell_price
,ucop.price as price_paid
,ucop.price as rental_price

,'GIFTCARD' as merchandise_category
,null as booking_id

from analytics.rtrbi_daily_uc_orders_temp uco
inner join rtr_prod0808.uc_order_products ucop on uco.order_id = ucop.order_id and ucop.nid = 0 and ucop.product_rid = 0

left join rtr_prod0808.uc_gift_certificates ugc on ugc.order_id = ucop.order_id
and trim(substring(ucop.title,instr(ucop.title,':')+1,length(ucop.title))) = ugc.cert_code

left join rtr_prod0808.rtr_order_group_giftcards rogg on rogg.gid = ugc.certificate_id
left join rtr_prod0808.rtr_order_groups rog on ucop.order_id = rog.order_id and rog.group_id = rogg.group_id

UNION ALL
-- other items
select distinct
 date(from_unixtime(uco.created)) as order_date
,date(from_unixtime(srr.begin)) as ship_out_date
,date(from_unixtime(srr.rent_begin)) as rent_begin_date

,uco.uid
,uco.primary_email
,uco.is_rtr_email
,uco.order_id
,uco.order_status
,rog.group_id

,srr.rid
,srr.backup_rid
,sri.nid
,sri.iid
,trim(ucp.model) as style_name
,trim(sri.size) as size
,concat(trim(ucp.model),'_',trim(sri.size)) as sku

,p.combo_type
,p.special_type
,p.type
,case when coalesce(srr.backup_rid,0) <> 0 then 1 else 0 end as backup_size
,case when ucop.backup = 'Y' then 1 else 0 end as backup_style

,ucop.qty
,ucop.price
,p.sell_price
,case when p.special_type <> 'C' then
    case when p.type = 'D' and coalesce(srr.backup_rid,0) <> 0 then 0 -- backup size
         when p.type = 'D' and ucop.backup = 'Y' then ucop.price -- backup style
         when p.type = 'D' then ucop.qty * ucop.price -- normal dress rental
         when p.type = 'A' then ucop.qty * ucop.price -- normal accessory rental
         when p.type = 'U' then ucop.price -- saleable
         else null end -- should never get here
 else ucop.price end as price_paid -- clearance
,case when p.special_type <> 'C' then p.sell_price -- all rentable or upsell
 else ucop.price end as rental_price -- clearance

,case
	when p.combo_type in ('U','XU') then 'BULK'
	when p.combo_type in ('A','D','XA','XD') then 'RENTAL'
	when p.combo_type in ('CA','CD','CC','CU') then 'CLEARANCE'
	when p.combo_type = 'GC' then 'GIFTCARD'
	else null
 end as merchandise_category
,case when srr.backup_rid is not null then ucop.backup_booking_id else ucop.booking_id end as booking_id

from analytics.rtrbi_daily_uc_orders_temp uco

inner join rtr_prod0808.uc_order_products ucop on uco.order_id = ucop.order_id
inner join rtr_prod0808.rtr_order_groups rog on uco.order_id = rog.order_id

inner join rtr_prod0808.rtr_order_group_reservations rogr on rog.group_id = rogr.group_id and rogr.rid = ucop.product_rid
inner join rtr_prod0808.simplereservation_reservation srr on coalesce(if(srr.backup_rid = 0,null,srr.backup_rid),srr.rid) = rogr.rid 

inner join rtr_prod0808.simplereservation_item sri on srr.item_id = sri.iid
inner join rtr_prod0808.uc_products ucp on sri.nid = ucp.nid
left join rtrbi.products_master p on sri.nid = p.nid
;

-- update merchandise category from reservation booking
update rtrbi.order_simplereservation_reservation_items_snapshot srr
inner join analytics.rtrbi_daily_uc_orders_temp uco on srr.order_id = uco.order_id
inner join rtr_prod0808.reservation_booking rb on srr.booking_id = rb.id

set srr.merchandise_category = rb.merchandise_category
where srr.booking_id is not null
;

-- recalculate price using merchandise category
update rtrbi.order_simplereservation_reservation_items_snapshot srr
inner join analytics.rtrbi_daily_uc_orders_temp uco on srr.order_id = uco.order_id

set 
 srr.price_paid =
 	case
 		when srr.merchandise_category = 'CLEARANCE' then srr.price -- clearance
 		when srr.type = 'D' and srr.backup_size = 1 then 0 -- backup size
 		when srr.type = 'D' and srr.backup_style = 1 then srr.price -- backup style
 		when srr.type = 'D' then srr.qty * srr.price -- normal dress rental
 		when srr.type = 'A' then srr.qty * srr.price -- normal accessory rental
 		when srr.type = 'U' then srr.price -- saleable
 		else null -- should never get here
 	end
,srr.rental_price = 
	case
		when srr.merchandise_category = 'CLEARANCE' then srr.sell_price
		else srr.price
	end
;

insert ignore into rtrbi.order_reservation_booking_snapshot
(
 order_date
,uid
,primary_email
,is_rtr_email
,order_id
,order_status
,group_id

,combo_type
,special_type
,type

,qty
,price
,sell_price
,backup_size
,backup_style
,price_paid
,rental_price

,product_rid

,rb_id
,rb_creation_date
,rb_destination_zip
,rb_expected_begin_date
,rb_expected_end_date
,rb_hold_expiration_date
,rb_sku
,rb_source_zip
,rb_status
,rb_type
,rb_primary_booking_id
,rb_modified_date
,rb_late_end_date
,rb_late_end_date2
,rb_merchandise_category
,rb_clearance_barcode
,rb_last_possible_begin_date

,rbd_id
,rbd_customer_arrival_date
,rbd_customer_shipment_date
,rbd_item_available_date
,rbd_rental_begin_date
,rbd_rental_end_date
,rbd_warehouse_received_date
,rbd_warehouse_shipment_date
,rbd_shipping_method_id
,rbd_creation_date
,rbd_consumer_shipping_charge_name
,rbd_modified_date
)
select
 date(from_unixtime(uco.created)) as order_date

,uco.uid
,uco.primary_email
,uco.is_rtr_email
,uco.order_id
,uco.order_status
,rog.group_id

,ucop.combo_type
,ucop.special_type
,ucop.type

,ucop.qty
,ucop.price
,ucop.sell_price
,ucop.backup_size
,ucop.backup_style
,ucop.price_paid
,ucop.rental_price

,ucop.product_rid

,rb.id as rb_id
,rb.creation_date as rb_creation_date
,rb.destination_zip as rb_destination_zip
,rb.expected_begin_date as rb_expected_begin_date
,rb.expected_end_date as rb_expected_end_date
,rb.hold_expiration_date as rb_hold_expiration_date
,rb.sku as rb_sku
,rb.source_zip as rb_source_zip
,rb.status as rb_status
,rb.type as rb_type
,rb.primary_booking_id as rb_primary_booking_id
,rb.modified_date as rb_modified_date
,rb.late_end_date as rb_late_end_date
,rb.late_end_date2 as rb_late_end_date2
,rb.merchandise_category as rb_merchandise_category
,rb.clearance_barcode as rb_clearance_barcode
,rb.last_possible_begin_date as rb_last_possible_begin_date

,rbd.id as rbd_id
,rbd.customer_arrival_date as rbd_customer_arrival_date
,rbd.customer_shipment_date as rbd_customer_shipment_date
,rbd.item_available_date as rbd_item_available_date
,rbd.rental_begin_date as rbd_rental_begin_date
,rbd.rental_end_date as rbd_rental_end_date
,rbd.warehouse_received_date as rbd_warehouse_received_date
,rbd.warehouse_shipment_date as rbd_warehouse_shipment_date
,rbd.shipping_method_id as rbd_shipping_method_id
,rbd.creation_date as rbd_creation_date
,rbd.consumer_shipping_charge_name as rbd_consumer_shipping_charge_name
,rbd.modified_date as rbd_modified_date

from analytics.rtrbi_daily_uc_orders_temp uco
inner join rtr_prod0808.rtr_order_groups rog on uco.order_id = rog.order_id
inner join rtr_prod0808.reservation_booking rb on rog.group_id = rb.order_group_id 
inner join analytics.rtrbi_daily_uc_order_products_temp ucop on rb.id = ucop.booking_id
and 
(
	(uco.order_status != 'canceled' and rb.status = 0)
	or
	(uco.order_status = 'canceled')
)
inner join rtr_prod0808.reservation_booking_detail rbd on rb.id = rbd.reservation_booking_id
inner join rtrbi.products_master_iid p on rb.sku = p.sku
;

-- @DATASET {"id":"order_group_summary"}
-- summarize order group items
insert ignore into rtrbi.order_group_summary
(
 order_date
,ship_out_date
,rent_begin_date
,uid
,primary_email
,is_rtr_email
,order_id
,order_status
,group_id

,shipping_method_id
,return_shipping_method_id

,total_item_count

,dress_count
,backup_size_count
,backup_style_count
,accessory_count
,upsell_count

,clearance_dress_count
,clearance_accessory_count
,clearance_other_count

,special_dress_count
,special_accessory_count
,special_upsell_count

,gift_card_count
,unknown_product_type_count

,total_price_paid

,dress_price_paid
,backup_size_price_paid
,backup_style_price_paid
,accessory_price_paid
,upsell_price_paid

,clearance_dress_price_paid
,clearance_accessory_price_paid
,clearance_other_price_paid

,special_dress_price_paid
,special_accessory_price_paid
,special_upsell_price_paid

,gift_card_price_paid
,unknown_product_type_price_paid

,total_rental_price

,dress_rental_price
,backup_size_rental_price
,backup_style_rental_price
,accessory_rental_price
,upsell_rental_price

,clearance_dress_rental_price
,clearance_accessory_rental_price
,clearance_other_rental_price

,special_dress_rental_price
,special_accessory_rental_price
,special_upsell_rental_price

,gift_card_rental_price
,unknown_product_type_rental_price

,run_timestamp
)
select
 s.order_date
,min(s.rbd_warehouse_shipment_date) as ship_out_date
,min(s.rbd_rental_begin_date) as rent_begin_date

,s.uid
,s.primary_email
,s.is_rtr_email
,s.order_id
,s.order_status
,s.group_id

,rog.shipping_method
,rog.return_shipping_method_id

,sum(1) as total_item_count

,sum(case when s.combo_type = 'D' then 1 else 0 end) as dress_count
,sum(s.backup_size) as backup_size_count
,sum(s.backup_style) as backup_style_count
,sum(case when s.combo_type = 'A' then 1 else 0 end) as accessory_count
,sum(case when s.combo_type = 'U' then 1 else 0 end) as upsell_count

,sum(case when s.combo_type = 'CD' then 1 else 0 end) as clearance_dress_count
,sum(case when s.combo_type = 'CA' then 1 else 0 end) as clearance_accessory_count
,sum(case when s.combo_type in ('CU','CC') then 1 else 0 end) as clearance_other_count

,sum(case when s.combo_type = 'XD' then 1 else 0 end) as special_dress_count
,sum(case when s.combo_type = 'XA' then 1 else 0 end) as special_accessory_count
,sum(case when s.combo_type = 'XU' then 1 else 0 end) as special_accessory_count

,sum(case when s.combo_type = 'GC' then 1 else 0 end) as gift_card_count
,sum(case when s.combo_type is null then 1 else 0 end) as unknown_product_type_count

,sum(s.price_paid) as total_item_price_paid

,sum(case when s.combo_type = 'D' then s.price_paid else 0 end) as dress_price_paid
,sum(case when s.backup_size = 1 then s.price_paid else 0 end) as backup_size_price_paid
,sum(case when s.backup_style = 1 then s.price_paid else 0 end) as backup_style_price_paid
,sum(case when s.combo_type = 'A' then s.price_paid else 0 end) as accessory_price_paid
,sum(case when s.combo_type = 'U' then s.price_paid else 0 end) as upsell_price_paid

,sum(case when s.combo_type = 'CD' then s.price_paid else 0 end) as clearance_dress_price_paid
,sum(case when s.combo_type = 'CA' then s.price_paid else 0 end) as clearance_accessory_price_paid
,sum(case when s.combo_type in ('CU','CC') then s.price_paid else 0 end) as clearance_other_price_paid

,sum(case when s.combo_type = 'XD' then s.price_paid else 0 end) as special_dress_price_paid
,sum(case when s.combo_type = 'XA' then s.price_paid else 0 end) as special_accessory_price_paid
,sum(case when s.combo_type = 'XU' then s.price_paid else 0 end) as special_accessory_price_paid

,sum(case when s.combo_type = 'GC' then s.price_paid else 0 end) as gift_card_price_paid
,sum(case when s.combo_type is null then s.price_paid else 0 end) as unknown_product_type_price_paid

,sum(s.rental_price) as total_item_rental_price

,sum(case when s.combo_type = 'D' then s.rental_price else 0 end) as dress_rental_price
,sum(case when s.backup_size = 1 then s.rental_price else 0 end) as backup_size_rental_price
,sum(case when s.backup_style = 1 then s.rental_price else 0 end) as backup_style_rental_price
,sum(case when s.combo_type = 'A' then s.rental_price else 0 end) as accessory_rental_price
,sum(case when s.combo_type = 'U' then s.rental_price else 0 end) as upsell_rental_price

,sum(case when s.combo_type = 'CD' then s.rental_price else 0 end) as clearance_dress_rental_price
,sum(case when s.combo_type = 'CA' then s.rental_price else 0 end) as clearance_accessory_rental_price
,sum(case when s.combo_type in ('CU','CC') then s.rental_price else 0 end) as clearance_other_rental_price

,sum(case when s.combo_type = 'XD' then s.rental_price else 0 end) as special_dress_rental_price
,sum(case when s.combo_type = 'XA' then s.rental_price else 0 end) as special_accessory_rental_price
,sum(case when s.combo_type = 'XU' then s.rental_price else 0 end) as special_accessory_rental_price

,sum(case when s.combo_type = 'GC' then s.rental_price else 0 end) as gift_card_rental_price
,sum(case when s.combo_type is null then s.rental_price else 0 end) as unknown_product_type_rental_price

,now() as run_timestamp

from rtrbi.order_reservation_booking_snapshot s 
inner join analytics.rtrbi_daily_uc_orders_temp uco on uco.order_id = s.order_id
inner join rtr_prod0808.rtr_order_groups rog on s.group_id = rog.group_id

group by order_id,group_id;

-- @DATASET {"id":"order_line_items"}
insert ignore into rtrbi.order_line_items
(
 uid
,primary_email
,is_rtr_email
,order_id
,order_date
,order_status
,order_total

,li_type
,li_amount
,li_count

,run_timestamp
)
select
 uco.uid
,uco.primary_email
,uco.is_rtr_email
,uco.order_id
,date(from_unixtime(uco.created)) as order_date
,uco.order_status
,uco.order_total

,lower(case when li.type = 'tax' then li.title else li.type end) as li_type
,sum(li.amount) as li_amount
,count(*) as li_count

,now() as run_timestamp

from rtr_prod0808.uc_order_line_items li
inner join analytics.rtrbi_daily_uc_orders_temp uco on li.order_id = uco.order_id
group by 
 uco.order_id
,li_type
;

-- @DATASET {"id":"daily_order_summary"}
insert ignore into rtrbi.order_summary 
(
 uid
,primary_email
,is_rtr_email
,order_id
,order_date
,created
,order_status
,order_total
,product_count
,clearance_count

,run_timestamp
)
select
 uco.uid
,uco.primary_email
,uco.is_rtr_email
,uco.order_id
,date(from_unixtime(uco.created)) as order_date
,uco.created
,uco.order_status
,uco.order_total
,uco.product_count
,gs.clearance_count

,now() as run_timestamp 

from analytics.rtrbi_daily_uc_orders_temp uco
left join
(
select 
 gs.order_id
,sum(gs.clearance_dress_count + gs.clearance_accessory_count + gs.clearance_other_count) as clearance_count
from rtrbi.order_group_summary gs
inner join analytics.rtrbi_daily_uc_orders_temp uco on gs.order_id = uco.order_id 
group by 1
) gs on uco.order_id = gs.order_id
;

-- @DATASET {"id":"order attributes"}
insert ignore into rtrbi.order_attributes
(
 uid
,primary_email
,is_rtr_email
,order_id
,order_date
,order_status

,run_timestamp
)
select
 uco.uid
,uco.primary_email
,uco.is_rtr_email
,uco.order_id
,date(from_unixtime(uco.created)) as order_date
,uco.order_status

,now() as run_timestamp

from analytics.rtrbi_daily_uc_orders_temp uco;

-- update order status
update rtrbi.order_attributes uco
inner join rtr_prod0808.uc_orders o on uco.order_id = o.order_id
set
 uco.order_status = o.order_status;

-- update promo codes
update rtrbi.order_attributes uco
inner join rtr_prod0808.rtr_promotion_redemption p on uco.order_id = p.order_id

set
 uco.promo_id = p.promo_id
,uco.promo_code = p.code
;

-- update special promo codes
update rtrbi.order_attributes uco
inner join rtrbi.special_promo_codes c on uco.promo_id = c.promo_id

set
 uco.promo_outfit_of_the_month = case when c.promo_type = 'Outfit of the Month' then c.code else null end
,uco.promo_surprise_dress = case when c.promo_type = 'Surprise Dress' then c.code else null end
,uco.promo_bogo = case when c.promo_type = 'BOGO' then c.code else null end
,uco.promo_free_dress = case when c.promo_type = 'FREEDRESS' then c.code else null end
,uco.promo_office_order = case when c.promo_type = 'OFFICE' then c.code else null end
,uco.promo_other_free = case when c.promo_type = 'Other Free' then c.code else null end
;

-- update special products
update rtrbi.order_attributes uco
inner join rtr_prod0808.uc_order_products ucop on uco.order_id = ucop.order_id and ucop.booking_id > 0
inner join rtr_prod0808.reservation_booking rb on ucop.booking_id = rb.id
inner join rtrbi.products_master_iid p on rb.sku = p.sku and p.special_product is not null
set
 uco.product_mystery_bling = case when p.special_product = 'Mystery Bling' then p.sku else null end 
,uco.product_outfit_of_the_month = case when p.special_product in ('Outfit of the Month','Summer Style Pass') then p.sku else null end 
,uco.product_free_accessory = case when p.special_product = 'Free Accessory' then p.sku else null end 
,uco.product_free_dress = case when p.special_product = 'Free Dress' then p.sku else null end 
,uco.product_pop_up_shop = case when p.special_product = 'Pop Up Shop' then p.sku else null end 
;

-- update customer order flag - exclude RTR emails except for surprise dress
update rtrbi.order_attributes uco
set uco.is_customer_order =
case when uco.primary_email like 'surprisedress%@renttherunway.com' then 1
     when uco.is_rtr_email = 0 then 1
     else 0 end
;

-- @DATASET {"id":"daily order stats"}
insert ignore into analytics.rtrbi_daily_order_stats
(
 asof_date

,user_count
,order_count
,total_order_value
,avg_order_value

,gross_user_count
,gross_order_count
,gross_order_value
,gross_avg_order_value

,all_user_count
,all_order_count
,all_total_order_value
,all_avg_order_value

,all_gross_user_count
,all_gross_order_count
,all_gross_order_value
,all_gross_avg_order_value

,tax_value
,gross_tax_value

,run_timestamp
)
select
 asof_date

,sum(user_count) as user_count
,sum(order_count) as order_count
,sum(total_order_value) as total_order_value
,sum(avg_order_value) as avg_order_value

,sum(gross_user_count) as gross_user_count
,sum(gross_order_count) as gross_order_count
,sum(gross_order_value) as gross_order_value
,sum(gross_avg_order_value) as gross_avg_order_value

,sum(all_user_count) as all_user_count
,sum(all_order_count) as all_order_count
,sum(all_total_order_value) as all_total_order_value
,sum(all_avg_order_value) as all_avg_order_value

,sum(all_gross_user_count) as all_gross_user_count
,sum(all_gross_order_count) as all_gross_order_count
,sum(all_gross_order_value) as all_gross_order_value
,sum(all_gross_avg_order_value) as all_gross_avg_order_value

,sum(tax_value) as tax_value
,sum(gross_tax_value) as gross_tax_value

,now() as run_timestamp

from (
select
 date(from_unixtime(uco.created)) as asof_date

,0 as user_count
,0 as order_count
,0 as total_order_value
,0 as avg_order_value

,0 as gross_user_count
,0 as gross_order_count
,0 as gross_order_value
,0 as gross_avg_order_value

,count(distinct case when uco.order_status in ('payment_received') then uco.uid else null end) as all_user_count
,count(distinct case when uco.order_status in ('payment_received') then uco.order_id else null end) as all_order_count
,sum(case when uco.order_status in ('payment_received') then uco.order_total else 0 end) as all_total_order_value
,round(sum(case when uco.order_status in ('payment_received') then uco.order_total else 0 end) 
       / count(distinct case when uco.order_status in ('payment_received') then uco.order_id else null end), 2 ) as all_avg_order_value

,count(distinct case when uco.order_status in ('payment_received','canceled') then uco.uid else null end) as all_gross_user_count
,count(distinct case when uco.order_status in ('payment_received','canceled') then uco.order_id else null end) as all_gross_order_count
,sum(case when uco.order_status in ('payment_received','canceled') then uco.order_total else 0 end) as all_gross_order_value
,round(sum(case when uco.order_status in ('payment_received','canceled') then uco.order_total else 0 end) 
       / count(distinct case when uco.order_status in ('payment_received','canceled') then uco.order_id else null end), 2 ) as all_gross_avg_order_value

,0 as tax_value
,0 as gross_tax_value

from rtrbi.order_summary uco
inner join rtrbi.order_attributes a on uco.order_id = a.order_id

where uco.order_status in ('payment_received','canceled')
  and uco.order_date = date_sub(curdate(),interval 1 day)
  and uco.clearance_count = 0

  -- include all orders

group by 1
UNION ALL
select
 uco.order_date as asof_date

,count(distinct case when uco.order_status in ('payment_received') then uco.uid else null end) as user_count
,count(distinct case when uco.order_status in ('payment_received') then uco.order_id else null end) as order_count
,sum(case when uco.order_status in ('payment_received') then uco.order_total else 0 end) as total_order_value
,round(sum(case when uco.order_status in ('payment_received') then uco.order_total else 0 end) 
       / count(distinct case when uco.order_status in ('payment_received') then uco.order_id else null end), 2 ) as avg_order_value

,count(distinct case when uco.order_status in ('payment_received','canceled') then uco.uid else null end) as gross_user_count
,count(distinct case when uco.order_status in ('payment_received','canceled') then uco.order_id else null end) as gross_order_count
,sum(case when uco.order_status in ('payment_received','canceled') then uco.order_total else 0 end) as gross_order_value
,round(sum(case when uco.order_status in ('payment_received','canceled') then uco.order_total else 0 end) 
       / count(distinct case when uco.order_status in ('payment_received','canceled') then uco.order_id else null end), 2 ) as gross_avg_order_value

,0 as all_user_count
,0 as all_order_count
,0 as all_total_order_value
,0 as all_avg_order_value

,0 as all_gross_user_count
,0 as all_gross_order_count
,0 as all_gross_order_value
,0 as all_gross_avg_order_value

,0 as tax_value
,0 as gross_tax_value

from rtrbi.order_summary uco
inner join rtrbi.order_attributes a on uco.order_id = a.order_id 

where uco.order_status in ('payment_received','canceled')
  and uco.order_date = date_sub(curdate(),interval 1 day)
  and uco.clearance_count = 0

  -- exclude RTR emails and surprise dress promo orders
  and uco.is_rtr_email = 0
  and a.promo_surprise_dress is null

group by 1
UNION ALL
select
 li.order_date

,0 as user_count
,0 as order_count
,0 as total_order_value
,0 as avg_order_value

,0 as gross_user_count
,0 as gross_order_count
,0 as gross_order_value
,0 as gross_avg_order_value

,0 as all_user_count
,0 as all_order_count
,0 as all_total_order_value
,0 as all_avg_order_value

,0 as all_gross_user_count
,0 as all_gross_order_count
,0 as all_gross_order_value
,0 as all_gross_avg_order_value

,sum(case when li.order_status in ('payment_received') then li_amount else 0 end) as tax_value
,sum(case when li.order_status in ('payment_received','canceled') then li_amount else 0 end) as gross_tax_value

from rtrbi.order_line_items li
inner join rtrbi.order_attributes a on li.order_id = a.order_id 
inner join rtrbi.order_summary uco on li.order_id = uco.order_id and uco.clearance_count = 0

where li.order_date = date_sub(curdate(),interval 1 day)
  and li.li_type = 'tax'

  -- exclude RTR emails and surprise dress promo
  and li.is_rtr_email = 0
  and a.promo_surprise_dress is null

group by 1
) x
group by asof_date;

update analytics.rtrbi_daily_order_stats
set 
 scoop_source = 'gross_order_value'
,scoop_user_count = gross_user_count
,scoop_order_count = gross_order_count
,scoop_order_value = gross_order_value
,scoop_avg_order_value = gross_avg_order_value

,scoop_tax_value = gross_tax_value
,scoop_order_value_less_tax = scoop_order_value - scoop_tax_value
,scoop_avg_order_value_less_tax = scoop_order_value_less_tax / scoop_order_count

where asof_date = date_sub(curdate(),interval 1 day)
  and scoop_source is null
;

-- cleanup
drop table if exists analytics.rtrbi_daily_uc_orders_temp;

-- update data ready
replace into rtrbi.data_ready(data_set,run_date)
select 'rtrbi.order_simplereservation_reservation_items_snapshot',curdate()
UNION
select 'rtrbi.order_reservation_booking_snapshot',curdate()
UNION
select 'rtrbi.order_group_summary',curdate()
UNION
select 'rtrbi.order_summary',curdate()
UNION
select 'rtrbi.order_attributes',curdate()
UNION
select 'rtrbi.order_line_items',curdate()
;

commit;
