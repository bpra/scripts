

select 
    day(orderDate),
    year(orderDate),
    month(orderDate),
    sum(amount) as total_sales,
    count(distinct ot.order_id) as total_orders,
    sum(amount) / count(distinct ot.order_id) as aov
from
    rtrbi.ot_orderdata ot
        inner join
    etl.uc_orders uo ON ot.order_id = uo.order_id
        inner join
    etl.order_summary os ON os.order_id = uo.order_id
where
    uo.order_status in ('canceled' , 'payment_received')
        and os.primary_email not like '%renttherunway.com'
        and os.uid > 1
        and lineitemTypeId not in (7 , 9, 11)
        and viewId = 'ZD'
        and (merchcategoryId <> 3
        or merchcategoryId is null)
and orderDate >= '2014-02-01'
group by 1 , 2, 3
order by month(orderDate),day(orderDate);



select 
    day(orderDate),
    year(orderDate),
    month(orderDate),
    format(sum(amount), 0) as total_sales,
    format(count(distinct ot.order_id), 0) as total_orders,
    format(sum(amount) / count(distinct ot.order_id),
        2) as aov
from
    ot_orderdata ot
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
        and viewId = 'ZD'
        and oa.promo_surprise_dress is null
        and (merchcategoryId <> 3
        or merchcategoryId is null)
and orderDate >= '2014-02-01'
group by 1 , 2, 3
order by month(orderDate),day(orderDate);

select 
year(asof_date),month(asof_date), day(asof_date)
,format(sum(scoop_order_value_less_tax), 0) as total_sales
,format(sum(scoop_order_count),0) as total_orders
,format(sum(scoop_order_value_less_tax) / sum(scoop_order_count), 2) as aov
from analytics.rtrbi_daily_order_stats
where asof_date>='2014-02-01'
group by 1,2,3
order by month(asof_date),day(asof_date);