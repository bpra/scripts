drop table if exists clearance_sales_report_tmp;
drop table if exists clearance_sales_report1_tmp;


create table clearance_sales_report_tmp 
as
(
	select 
		DATE(FROM_UNIXTIME(uo.created)) as AsOfDate
		,pmi.type
		,pmi.model
		,count(uop.qty) as quantitySold
		,sum(uop.price*uop.qty) as salesUSD
	from rtr_prod0808.reservation_booking rb
	inner join rtr_prod0808.uc_order_products uop
		on uop.booking_id = rb.id
	inner join rtr_prod0808.uc_orders uo
		on uo.order_id=uop.order_id
	inner join rtrbi.products_master_iid pmi
		on rb.sku=pmi.sku
	where 1=1
			and merchandise_category= 'clearance'
			and date(from_unixtime(uo.created))>='2013-7-17'
			and rb.status = 0 
			and rb.type =0
			and uo.order_status = 'payment_received'
	group by 1,2,3

);

create table clearance_sales_report1_tmp 
as
(
	select dates.AsOfDate,tots.type,tots.model,clear.quantitySold,clear.salesUSD from 
	
	(select distinct AsOfDate  from clearance_sales_report_tmp) dates
	#on clear.AsOfDate = dates.AsOfDate
	join
	(select 
	pmi.type
	,pmi.model
	,count(1) as Qty
	from rtrbi.z_clearsnap_item_reservation_setting irss
	inner join rtrbi.z_clearsnap_sku_reservation_setting srss
	on srss.id = irss.sku_reservation_setting_id
	inner join rtrbi.products_master_iid pmi
	on srss.sku=pmi.sku
	where pmi.on_site =1
	group by 1,2
	) tots
	left join clearance_sales_report_tmp clear
	on tots.type = clear.type 
	and tots.model = clear.model
	and clear.AsOfDate = dates.AsOfDate

);


create table clearance_sales_report2_tmp 
as
select * from clearance_sales_report1_tmp;


drop table if exists analytics.bp_clearance_report;

create table analytics.bp_clearance_report as
select 
	total.AsOfDate
	,total.type as ProductType
	,total.model as Style
	,total.Qty as totalInventory
	,total.retail_price
	,total.wholesale_price
	,total.rental_price
	,(sales.cumulativeSales/sales.cumulativeQtySold) as pricePerUnit	
	,total.designer
	,total.sub_type
	,sales.quantitySold
	,sales.salesUSD
	,sales.cumulativeQtySold
	,sales.cumulativeSales
	,ras.available_units
	,(cumulativeQtySold/(available_units+cumulativeQtySold)) as sell_through
	,cumulativeQtySold/total.Qty as percentOfTotalInventorySold
	

from

(
select * from 
	(select distinct AsOfDate  from clearance_sales_report_tmp) dates
join
	(select 
	pmi.type
	,pmi.model
	,pmi.list_price as retail_price
	,pmi.cost as wholesale_price
	,pmi.sell_price as rental_price
	,pmi.designer
	,pmi.sub_type
	,count(1) as Qty
 from rtrbi.z_clearsnap_item_reservation_setting irss
	inner join rtrbi.z_clearsnap_sku_reservation_setting srss
	on srss.id = irss.sku_reservation_setting_id
	inner join rtrbi.products_master_iid pmi
	on srss.sku=pmi.sku
	where pmi.on_site =1
	group by 1,2,3,4,5,6,7
	) tots
	
) total

left join
(
select sales.AsOfDate
	   ,sales.type
	   ,sales.model
	   ,sales.quantitySold
	   ,sales.salesUSD 
	   ,sum(case when sales1.quantitySold is null then 0 else sales1.quantitySold end ) as cumulativeQtySold
	   ,sum(case when sales1.salesUSD is null then 0 else sales1.salesUSD end) as cumulativeSales
#select * 
	from clearance_sales_report1_tmp sales
	inner join clearance_sales_report2_tmp sales1
	on sales.AsOfDate>=sales1.AsOfDate
	and sales.type = sales1.type
	and sales.model = sales1.model
#where sales.model = 'EBMPE2'
#order by 3,1,6
group by 1,2,3,4,5

) sales
on total.type = sales.type
and total.model = sales.model
and sales.AsOfDate = total.AsOfDate
left join 
(
select 
	date(run_timestamp) as asof_date
	,pmi.type
	,pmi.model
	,sum(ras.available_units) as available_units
#select * 
from rtrbi.rescal_availability_snapshot ras
inner join rtrbi.products_master_iid pmi
		on ras.sku=pmi.sku
where merchandise_category='clearance'
group by 1,2,3

)ras
on ras.asof_date = sales.AsOfDate
and ras.type = sales.type
and ras.model = sales.model
#where dates.AsOfDate is not null
order by 1,2,3