create table analytics.z_clearProducts
as
select sku
,
case when sku like 'CLEARANCE_NEW%' then substring(sku,15,length(sku)) 
	 when sku like 'CLEARANCE_%' then substring(sku,11,length(sku))
	 else sku
end as cleaned_sku
,nid,rentable_nid,case when nid=rentable_nid then 0 else 1 end as is_clearancesku,iid,
model as style,size
,case when combo_type = 'cd' then 'D' 
	when combo_type ='ca' then 'A'
	when combo_type = 'cu' then 'B'
	when combo_type = 'cc' then 'A'
end as product_type
,designer_id,designer,title,list_price,cost,sell_price,clearance_price,'2013-09-13' as effectiveDate
,'9999-12-31 23:59:59' as enddate
,1 as activeFlag
from rtrbi.products_master_iid 
where combo_type in ('CD','CA','CU','CC');


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
curdate() as effectiveDate,
'9999-12-31 23:59:59' as endDate,
1 as activeFlag
from rtrbi.products_master_iid
where combo_type not in ('CC','CU','CA','CD')
order by iid;


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
iid
,cleaned_sku
,style
,size
,product_type
,designer_id
,designer
,title
,list_price
,cost
,sell_price
,clearance_price
,effectiveDate
,enddate
,activeflag 
from analytics.z_clearProducts 
where is_clearancesku = 0
and sku not in (select sku from rtrbi.ot_product);


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
select iid
,cleaned_sku
,style
,size
,product_type
,designer_id
,designer
,title
,list_price
,cost
,sell_price
,clearance_price
,effectiveDate
,enddate
,activeflag 
from analytics.z_clearProducts 
where is_clearancesku = 1
and cleaned_sku not in (select sku from rtrbi.ot_product);

insert into ot_product(
#iid,
sku,
style,
#size,
product_type,
#designer_id,
designer,
title,
effectiveDate,
endDate,
activeFlag
)
select * from(
select 
'RTR_GC_P' as sku
,'RTR_GC' as style
,'GC' as product_type
,'Rent the Runway' as designer
,'Gift Card' as title
,'2009-01-01 23:59:59' as effectivedate
,'9999-12-31 23:59:59' as enddate
, 1 as activeflag
union all
select 
'RTR_GC_E'
,'RTR_GC'
,'GC'
,'Rent the Runway'
,'E-Gift Card'
,'2009-01-01 23:59:59'
,'9999-12-31 23:59:59'
,1 
)xx;


create index IX_sku on ot_product(sku);
create index IX_iid on ot_product(iid);