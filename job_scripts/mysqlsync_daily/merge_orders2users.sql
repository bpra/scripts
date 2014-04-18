\set ON_ERROR_STOP on
update analytics.users
set 
size_order = Y.psize,
style_order = Y.style,
spend_amt = Y.spend_amt,
num_orders = Y.num_orders,
last_order_ts = Y.last_order_ts,
first_order_ts = Y.first_order_ts
from (
	select uid, psize, style, sum(price_paid) spend_amt, max(order_ts) last_order_ts, min(order_ts) first_order_ts, count(distinct order_id) num_orders
	from (
		select uid, price_paid, order_ts, order_id, first_value(size) over (w) as psize, first_value(orders.style) over (w) as style
		from analytics.orders 
		inner join analytics.products using (style)
		where combo_type in ('A', 'D') -- dresses only ??
		window w as (partition by uid order by order_ts desc)
	) X
	group by 1,2,3
) Y
where users.uid = Y.uid;
commit;
