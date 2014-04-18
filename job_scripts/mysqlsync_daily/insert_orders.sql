\set ON_ERROR_STOP on
insert into analytics.orders
select o.*, to_timestamptz(odt.created) order_ts, to_timestamptz(odt.modified) modified_ts, oa.promo_id, oa.promo_code
from etl.orders o 
inner join etl.orders_dt odt using (order_id)
left outer join analytics.orders ao using (order_id)
left outer join etl.orders_attr oa using (order_id)
where ao.order_id is null -- not already there
;
commit;

