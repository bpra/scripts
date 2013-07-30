select 
avail.ProductType, a_units+ttlQty as available_units, (ttlQty/(a_units+ttlQty))*100 as sell_through,ttlSales 
from
(select ProductType,sum(available_units) as a_units from analytics.bp_clearance_report 
where AsOfDate = date(now()) - interval 1 day ##Assumes the report is run in the morning; it takes availablity for yesterday
group by 1) avail
join
(select ProductType,sum(quantitySold) as ttlQty,sum(salesUSD) as ttlSales from analytics.bp_clearance_report
group by 1) cumulativeTotal
on avail.ProductType=cumulativeTotal.ProductType
union all
select 'Total', sum(a_units+ttlQty) as available_units, (sum(ttlQty)/(sum(a_units+ttlQty)))*100 as sell_through,sum(ttlSales) 
from
(select ProductType,sum(available_units) as a_units from analytics.bp_clearance_report where AsOfDate = date(now()) - interval 1 day
group by 1) avail
join
(select ProductType,sum(quantitySold) as ttlQty,sum(salesUSD) as ttlSales from analytics.bp_clearance_report
group by 1) cumulativeTotal
on avail.ProductType=cumulativeTotal.ProductType
