select 
day(orderDate),
    year(orderDate),
    month(orderDate),
    format(sum(amount), 0) as total_sales,
    format(count(distinct ot.order_id), 0) as total_orders,
    format(sum(amount) / count(distinct ot.order_id),
        2) as aov

from rtrbi.ot_ordertransactiondetails ot
inner join  rtrbi.order_summary os
	on os.order_id=ot.order_id
-- inner join rtrbi.users u
-- 	on ot.uid = u.uid
-- inner join rtrbi.ot_lineitemtype lit
-- 	on ot.lineitemtypeId = lit.lineitemtypeId 
-- inner join rtrbi.dim_fiscal_dates dfd
-- 	on ot.orderdate = dfd.asof_date
 left outer join rtrbi.ot_product as op
 on ot.productid = op.productid
 -- inner join rtrbi.products_master_iid as pmi
-- on op.sku = pmi.sku
inner join rtrbi.order_attributes as oa
on os.order_id = oa.order_id
where   
        os.order_status in ('canceled' , 'payment_received')
        and os.primary_email not like '%renttherunway.com'
        and os.uid > 1
        and ot.lineitemTypeId not in (7 , 9, 11)
        and ((ot.statusId = 'OC' and ot.lineitemTypeId in (1 , 12)) 
			or (ot.statusId = 'ZD' and ot.lineitemTypeId not in (1 , 12, 103)))
        and oa.promo_surprise_dress is null
        and (ot.productordertypeId <> 3 or ot.productordertypeId is null)
		and orderDate='2013-09-01'
-- 		and op.activeflag = 1
-- and ot.order_id = 1042836
		group by 1 , 2, 3
order by month(orderDate),day(orderDate);