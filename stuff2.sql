select
 date_format(date_sub(from_unixtime(srr.rent_begin), interval weekday(from_unixtime(srr.rent_begin)) day),'%c/%e/%Y') as week_starting
,format(100 * count(distinct uco.order_id) / yest.total_orders, 0) as order_percent

from rtr_prod0808.uc_orders as uco
inner join rtrbi.order_attributes a 
	on uco.order_id = a.order_id 
	and a.promo_surprise_dress is null
inner join rtr_prod0808.uc_order_products as ucop 
	on uco.order_id = ucop.order_id
inner join rtr_prod0808.simplereservation_reservation as srr 
	on ucop.product_rid = srr.rid

inner join
(select
  count(*) as total_orders
from rtr_prod0808.uc_orders uco
inner join rtrbi.order_attributes a on uco.order_id = a.order_id and a.promo_surprise_dress is null
where uco.order_status = 'payment_received'
	and uco.primary_email not like '%@renttherunway.com'
	and date(from_unixtime(uco.created)) = date_add(curdate(), interval -1 day)
) yest on 1 = 1

where uco.order_status = 'payment_received'
	and uco.primary_email not like '%@renttherunway.com'
	and date(from_unixtime(uco.created)) = date_add(curdate(), interval -1 day)

group by 1
order by count(distinct uco.order_id) desc
limit 3;

select * from rtr_prod0808.reservation_booking limit 10
select
 date_format(date_sub(from_unixtime(srr.rent_begin), interval weekday(from_unixtime(srr.rent_begin)) day),'%c/%e/%Y') as week_starting
,format(100 * count(distinct uco.order_id) / yest.total_orders, 0) as order_percent

from rtr_prod0808.uc_orders as uco
inner join rtrbi.order_attributes a 
	on uco.order_id = a.order_id 
	and a.promo_surprise_dress is null
inner join rtr_prod0808.uc_order_products as ucop 
	on uco.order_id = ucop.order_id
inner join rtr_prod0808.reservation_booking as srr 
	on ucop.product_rid = srr.rid

inner join
(select
  count(*) as total_orders
from rtr_prod0808.uc_orders uco
inner join rtrbi.order_attributes a on uco.order_id = a.order_id and a.promo_surprise_dress is null
where uco.order_status = 'payment_received'
	and uco.primary_email not like '%@renttherunway.com'
	and date(from_unixtime(uco.created)) = date_add(curdate(), interval -1 day)
) yest on 1 = 1

where uco.order_status = 'payment_received'
	and uco.primary_email not like '%@renttherunway.com'
	and date(from_unixtime(uco.created)) = date_add(curdate(), interval -1 day)

group by 1
order by count(distinct uco.order_id) desc
limit 3;