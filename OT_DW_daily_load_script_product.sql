##insert new skus
insert into ot_product(
iid,
sku,
style,
size,
product_type,
designer_id,
designer,
title,
list_price,
cost,
sell_price,
clearance_price,
effectiveDate,
endDate,
activeFlag
)
select 
iid,
sku,
model as style,
size,
case 
	when combo_type='A' then 'A'
	when combo_type='D' then 'D'
	when combo_type='U' then 'B'
	when combo_type='XA' then 'XA'
	when combo_type='XD' then 'XD'
	when combo_type='XU' then 'PP'
end as product_type,
designer_id,
designer,
title,
list_price,
cost,
sell_price,
clearance_price,
date(curdate()) as effectiveDate,
'9999-12-31 23:59:59' as endDate,
1 as activeFlag
from rtrbi.products_master_iid
where sku not in (select sku from ot_product)
and sku not like 'CLEARANCE%';

##update skus that have been deleted/dropped
update ot_product 
set 
    activeFlag = 0,
    endDate = date(curdate())
where
    sku not in (
		select 
		case when sku like 'CLEARANCE_NEW%' then substring(sku,15,length(sku)) 
			 when sku like 'CLEARANCE_%' then substring(sku,11,length(sku))
			 else sku
		end from  rtrbi.products_master_iid
)
and activeFlag=1
#and style not like 'CLEARANCE%' 
and product_type <> 'GC'
 ;

start transaction;
##update skus that have properties other than sku changed. Make them inactive and set the enddate as today (when the change was seen).
update ot_product op
inner join rtrbi.products_master_iid pmi
on pmi.sku=op.sku
set activeFlag = 0,
enddate = date(curdate())
where
activeFlag=1
and effectiveDate <> date(curdate())
and(
pmi.iid <> op.iid
or pmi.model <> op.style
or pmi.size<> op.size
or pmi.designer_id<> op.designer_id
or pmi.list_price<> op.list_price
or pmi.cost<> op.cost
or pmi.sell_price<> op.sell_price
or pmi.clearance_price<> op.clearance_price)
;

##Insert the changed property rows.
insert into ot_product(
iid,
sku,
style,
size,
product_type,
designer_id,
designer,
title,
list_price,
cost,
sell_price,
clearance_price,
effectiveDate,
endDate,
activeFlag
)
select 
pmi.iid,
pmi.sku,
pmi.model as style,
pmi.size,
case 
	when combo_type='A' then 'A'
	when combo_type='D' then 'D'
	when combo_type='U' then 'B'
	when combo_type='XA' then 'XA'
	when combo_type='XD' then 'XD'
	when combo_type='XU' then 'PP'
    when combo_type = 'cd' then 'D' 
	when combo_type ='ca' then 'A'
	when combo_type = 'cu' then 'B'
	when combo_type = 'cc' then 'A'
end as product_type,
pmi.designer_id,
pmi.designer,
pmi.title,
pmi.list_price,
pmi.cost,
pmi.sell_price,
pmi.clearance_price,
date(curdate()) as effectiveDate,
'9999-12-31 23:59:59' as endDate,
1 as activeFlag
#case when pmi.iid <> op.iid then 'IID'
#when pmi.model <> op.style then 'style'
#when pmi.size<> op.size then 'size'
#when pmi.designer_id<> op.designer_id then 'designerid'
#when pmi.list_price<> op.list_price then 'listprice'
#when pmi.cost<> op.cost then 'cost'
#when pmi.sell_price<> op.sell_price then 'sellprice'
#when pmi.clearance_price<> op.clearance_price then 'clearprice'
#end
from rtrbi.products_master_iid pmi
inner join ot_product op
on pmi.sku=op.sku
where
op.activeFlag=0
and op.endDate = date(curdate())
and(
pmi.iid <> op.iid
or pmi.model <> op.style
or pmi.size<> op.size
or pmi.designer_id<> op.designer_id
or pmi.list_price<> op.list_price
or pmi.cost<> op.cost
or pmi.sell_price<> op.sell_price
or pmi.clearance_price<> op.clearance_price)
and not exists (select 1 from ot_product op1 where op.sku =op1.sku and op1.activeFlag=1);

commit;


