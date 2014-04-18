set time_zone = 'America/New_York';
start transaction;
-- Build master list of all nid,styleName combinations from Drupal and MongoDB
-- get max(iid,nid) for each (sku)
drop table if exists analytics.rtrbitmp_products_master_sku_keys;

create table analytics.rtrbitmp_products_master_sku_keys
(
 iid int unsigned not null default 0
,nid int unsigned not null default 0
,styleName varchar(255) not null default ''
,size varchar(255) not null default ''
,sku varchar(255) not null default ''

,key(sku)
,key(iid)
);

insert into analytics.rtrbitmp_products_master_sku_keys
select
 iid
,nid
,styleName
,size
,concat(styleName,'_',size) as sku
from (
select
 0 as iid
,coalesce(d.attribute_value,0) as nid
,upper(trim(coalesce(c.styleName,''))) as styleName
,trim(coalesce(c.attribute_value,'')) as size
from rtrbi.mongo_product_catalog c
left join rtrbi.mongo_product_catalog d on c.styleName = d.styleName and d.attribute_name = 'legacyNid'
where c.attribute_name = 'sizes'
UNION
select
 sri.iid
,sri.nid
,upper(trim(coalesce(ucp.model,''))) as styleName
,trim(coalesce(sri.size,'')) as size
from rtr_prod0808.simplereservation_item sri
inner join rtr_prod0808.uc_products ucp on sri.nid = ucp.nid
) x
;

drop table if exists analytics.rtrbitmp_products_master_sku_keys_uniq;

create temporary table analytics.rtrbitmp_products_master_sku_keys_uniq
as
select *
from analytics.rtrbitmp_products_master_sku_keys k
where k.iid = 
(select max(x.iid)
from analytics.rtrbitmp_products_master_sku_keys x
where k.sku = x.sku)
;

alter table analytics.rtrbitmp_products_master_sku_keys_uniq
 add primary key(iid,sku)
,add unique key(sku);

-- DATA ISSUE: Some styles have no iid,nid - these won't work while Drupal is alive
delete from analytics.rtrbitmp_products_master_sku_keys_uniq
where iid = 0 or nid = 0
;

delete from rtrbi.products_master_sku_keys
where sku not in (select sku from analytics.rtrbitmp_products_master_sku_keys_uniq);

delete from rtrbi.products_master_style_keys
where styleName not in (select distinct styleName from analytics.rtrbitmp_products_master_sku_keys_uniq);

replace into rtrbi.products_master_sku_keys
select * from analytics.rtrbitmp_products_master_sku_keys_uniq;

replace into rtrbi.products_master_style_keys
select distinct nid,styleName from analytics.rtrbitmp_products_master_sku_keys_uniq;

drop table if exists analytics.rtrbitmp_products_master_sku_keys;
drop table if exists analytics.rtrbitmp_products_master_sku_keys_uniq;

-- update data_ready
replace into rtrbi.data_ready(data_set,run_date)
select 'rtrbi.products_master_sku_keys',curdate()
UNION
select 'rtrbi.products_master_style_keys',curdate()
;
commit;