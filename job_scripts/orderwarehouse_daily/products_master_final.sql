set time_zone = 'America/New_York';
start transaction;
-- build nid/style master
drop table if exists analytics.rtrbitmp_products_master;

create table analytics.rtrbitmp_products_master
like rtrbi.products_master;

insert into analytics.rtrbitmp_products_master
(
 nid
,rentable_nid
,model

,sku_list
,size_list
)
select
 nid
,nid
,styleName as model

,group_concat(sku order by size separator ',') as sku_list
,group_concat(size order by size separator ',') as size_list
from rtrbi.products_master_sku_keys

group by nid
;

-- get product type from node 
update analytics.rtrbitmp_products_master p
inner join rtr_prod0808.node n on p.nid = n.nid
inner join rtr_prod0808.uc_products ucp on p.nid = ucp.nid
left join rtrbi.special_products sp on p.nid = sp.nid

set
 p.node_type = n.type
,p.on_site = n.status
,p.title = trim(replace(n.title,'\t',''))

,p.type = 
  case 
		when sp.special_type is not null then sp.special_type
		when n.type = 'product' then 'D'
		when n.type = 'accessories' then 'A'
		when n.type = 'saleableproduct' then 'U'
		when n.type = 'rtr_clearance' then 'C'
		else '?' end 
,p.special_product = sp.special_product
,p.special_type =
	case 
		when sp.special_type is not null then 'X'
		when n.type = 'rtr_clearance' then 'C' 
		else '' end 

,p.list_price = ucp.list_price
,p.cost = ucp.cost
,p.sell_price = ucp.sell_price
;

-- fix clearance mapping
update analytics.rtrbitmp_products_master p
inner join rtr_prod0808.rtr_clearance_node c on p.nid = c.nid
set
 p.rentable_nid = c.rentable_nid
,p.special_type = 'C'
;

update analytics.rtrbitmp_products_master p
inner join analytics.rtrbitmp_products_master r on p.rentable_nid = r.nid
set
 p.type = r.type
where p.nid != p.rentable_nid;

-- build combo_type
update analytics.rtrbitmp_products_master p
set 
 p.combo_type = trim(concat(p.special_type,p.type))
;

-- update availability date -- dresses
update analytics.rtrbitmp_products_master m
inner join rtr_prod0808.content_type_product as ctp on m.nid = ctp.nid and ctp.vid <> 302
left join rtr_prod0808.node d on d.nid = ctp.field_key_designer_nid
left join rtr_prod0808.node stype on stype.nid = ctp.field_styles_nid

set 
 m.avl_date = case when ctp.nid = 496 then '2009-12-01' else date(str_to_date(ctp.field_avl_from_value,'%Y-%m-%d')) end
,m.designer_id = ctp.field_key_designer_nid
,m.designer = trim(d.title)
,m.sub_type_id = ctp.field_styles_nid
,m.sub_type = trim(stype.title)
,m.season_code = trim(ctp.field_season_code_value)

where m.avl_date is null;

-- update availability date -- accessories
update analytics.rtrbitmp_products_master m
inner join rtr_prod0808.content_type_accessories as cta on m.nid = cta.nid
left join (
	select
		tn.nid,
	tn.tid,
		td.name
	from rtr_prod0808.term_node as tn
	inner join rtr_prod0808.term_data as td on td.tid = tn.tid
	where td.vid = 18
) as d on d.nid = cta.nid
left join (
	select
		tn.nid,
		tn.tid,
		td.name
	from rtr_prod0808.term_node as tn
	inner join rtr_prod0808.term_data as td on td.tid = tn.tid
	where td.vid = 1
) as stype on stype.nid = cta.nid
set 
 m.avl_date = case when cta.nid = 1062 then '2010-04-01' else date(str_to_date(cta.field_access_date_value,'%Y-%m-%d')) end
,m.designer_id = d.tid
,m.designer = trim(d.name)
,m.sub_type_id = stype.tid
,m.sub_type = trim(stype.`name`)

where m.avl_date is null;

-- update availability date -- upsell
update analytics.rtrbitmp_products_master m
inner join rtr_prod0808.content_type_saleableproduct as cts on m.nid = cts.nid
inner join rtr_prod0808.node n on m.nid = n.nid
left join (
	select
		tn.nid,
	tn.tid,
		td.name
	from rtr_prod0808.term_node as tn
	inner join rtr_prod0808.term_data as td on td.tid = tn.tid
	where td.vid = 25
) as d on d.nid = cts.nid
left join (
	select
		tn.nid,
		td.tid,
		td.name
	from rtr_prod0808.term_node as tn
	inner join rtr_prod0808.term_data as td on td.tid = tn.tid
	where td.vid = 22
) as stype on stype.nid = cts.nid

set 
 m.avl_date = date(from_unixtime(n.created))
,m.designer_id = d.tid
,m.designer = trim(d.name)
,m.sub_type_id = stype.tid
,m.sub_type = trim(stype.name)

where m.avl_date is null;

-- update availability date -- clearance
update analytics.rtrbitmp_products_master m
inner join rtr_prod0808.node n on m.nid = n.nid

set 
 m.avl_date = date(from_unixtime(n.created))

where m.special_type = 'C';

-- update rentable node fields for clearance
update analytics.rtrbitmp_products_master m
inner join 
(select
  r.*
from analytics.kh_products_iid_master r 
where r.latest_iid_mapping = 1
  and r.iid = (select max(x.iid) from analytics.kh_products_iid_master x where x.nid = r.nid)) r on m.rentable_nid = r.nid 

set 
 m.designer_id = r.designer_id
,m.designer = r.designer
,m.sub_type_id = r.sub_type_id
,m.sub_type = r.sub_type
,m.season_code = r.season_code

where m.special_type = 'C';

-- update mongo product catalog columns
update analytics.rtrbitmp_products_master m
inner join rtrbi.mongo_products_master mp on m.model = mp.styleName
set
 m.mongo_collection = mp._collection
,m.mongo_id = mp._id
,m.mongo_clearance = mp.clearance
,m.clearance_price = mp.clearancePrice
;

update analytics.rtrbitmp_products_master m
set 
 m.special_type = 'C'
,m.combo_type = concat('C',m.combo_type)
where m.mongo_clearance = 1 and m.special_type != 'C'
;

-- update fashion_value
update analytics.rtrbitmp_products_master p
inner join rtr_prod0808.content_type_product ctp on p.nid = ctp.nid
set p.fashion_value = ctp.field_fashion_value;

-- CUSTOM ATTRIBUTES
-- update custom attributes (alphabetical by attribute_name)
update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.bra_type = a.attribute_value
where a.attribute_name = 'bra_type';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.category_depth = a.attribute_value
where a.attribute_name = 'category_depth';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.fabric_code = a.attribute_value
where a.attribute_name = 'fabric_code';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.in_survey = a.attribute_value
where a.attribute_name = 'in_survey';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.is_bridesmaid_style = a.attribute_value
where a.attribute_name = 'is_bridesmaid_style';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.moda_style = a.attribute_value
where a.attribute_name = 'moda_style';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.plus_style = a.attribute_value
where a.attribute_name = 'plus_style';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.print_type = a.attribute_value
where a.attribute_name = 'print_type';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.season_code = a.attribute_value
where a.attribute_name = 'season_code';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.secondary_color = a.attribute_value
where a.attribute_name = 'secondary_color';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.secondary_type = a.attribute_value
where a.attribute_name = 'secondary_type';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.stretch = a.attribute_value
where a.attribute_name = 'stretch';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.style_out = a.attribute_value
where a.attribute_name = 'style_out';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.sub_type = a.attribute_value
where a.attribute_name = 'sub_type';

update analytics.rtrbitmp_products_master p
inner join rtrbi.products_custom_attributes a on p.nid = a.nid
set p.vendorSKU = a.attribute_value
where a.attribute_name = 'vendorSKU';

-- update designer classification
update analytics.rtrbitmp_products_master p
inner join rtrbi.products_designer_attributes d on p.designer_id = d.designer_id
set p.designer_classification = d.attribute_value
where d.attribute_name = 'designer_classification'
;

-- get models_name
update analytics.rtrbitmp_products_master p
inner join analytics.cc_products_vocabulary v
on p.nid = v.nid and v.vocabulary_name = 'Models'
set p.models_name = v.term_name
;

-- update component list
update analytics.rtrbitmp_products_master p
inner join
(
select 
 c.styleName
,cast(group_concat(d.description order by d.description) as char) as component_list
from rtrbi.products_components c
inner join rtrbi.products_component_lookup d on c.component_id = d.id
group by c.styleName
) c on p.model = c.styleName
set p.component_list = c.component_list;

-- MONGO ATTRIBUTES
-- add some mongo attributes
update analytics.rtrbitmp_products_master p
inner join rtrbi.mongo_products_master mp on p.mongo_id = mp._id
set
 p.fitNotes = mp.fitNotes
,p.image_count = 
  if(trim(coalesce(mp.imgLargeFront,'')) = '',0,1) 
+ if(trim(coalesce(mp.imgLargeSide,'')) = '',0,1)
+ if(trim(coalesce(mp.imgLargeTop,'')) = '',0,1)
+ if(trim(coalesce(mp.imgLargeWithModel,'')) = '',0,1)
,p.body_types = mp.bodyTypes
;

-- update marketing occasions from mongo
update analytics.rtrbitmp_products_master p
inner join (
select 
 p.nid

,group_concat(distinct case when m.attribute_name = 'marketingOccasions' then trim(m.attribute_value) else null end order by trim(m.attribute_value)) as marketing_occasions

from analytics.rtrbitmp_products_master p
inner join rtrbi.mongo_product_catalog m on p.model = m.styleName and m.attribute_name = 'marketingOccasions'

group by p.nid
) g on p.nid = g.nid
set p.marketing_occasions = g.marketing_occasions;

-- update unit counts by nid
drop table if exists analytics.rtrbitmp_products_unit_totals;

create table analytics.rtrbitmp_products_unit_totals
as
select
 k.nid

,sum(t.total_units) as total_units
,sum(t.rtv_deactivations) as rtv_deactivations
,sum(t.short_ship_deactivations) as short_ship_deactivations
,sum(t.other_deactivations) as other_deactivations

,sum(t.auto_late_deactivations) as auto_late_deactivations
,sum(t.auto_evaluate_deactivations) as auto_evaluate_deactivations

,sum(t.active_units) as active_units
,sum(t.clearance_units) as clearance_units
,sum(t.sold_units) as sold_units
,sum(t.qty) as qty
,sum(t.avl_qty) as avl_qty
,sum(t.drupal_active_units) as drupal_active_units
,sum(t.rescal_active_units) as rescal_active_units
,sum(t.rescal_active_barcodes) as rescal_active_barcodes

from rtrbi.products_master_sku_keys k
inner join rtrbi.products_unit_totals t on k.sku = t.sku

group by k.nid
;

alter table analytics.rtrbitmp_products_unit_totals
 add primary key(nid);

update analytics.rtrbitmp_products_master p
inner join analytics.rtrbitmp_products_unit_totals t on p.nid = t.nid
set
 p.total_units = t.total_units
,p.rtv_deactivations = t.rtv_deactivations
,p.short_ship_deactivations = t.short_ship_deactivations
,p.other_deactivations = t.other_deactivations

,p.auto_late_deactivations = t.auto_late_deactivations
,p.auto_evaluate_deactivations = t.auto_evaluate_deactivations

,p.active_units = t.active_units
,p.clearance_units = t.clearance_units
,p.sold_units = t.sold_units
,p.qty = t.qty
,p.avl_qty = t.avl_qty
,p.drupal_active_units = t.drupal_active_units
,p.rescal_active_units = t.rescal_active_units
,p.rescal_active_barcodes = t.rescal_active_barcodes;

drop table if exists analytics.rtrbitmp_products_unit_totals;

-- update attributes
update analytics.rtrbitmp_products_master p
inner join analytics.cc_products_attributes a on p.nid = a.nid
set
 p.embellishment = a.embellishment
,p.length = a.length
,p.neckline = a.neckline
,p.sleeve = a.sleeve
,p.style = a.style
,p.occasion = a.occasion
,p.color = a.color;

-- update from vocabulary
update analytics.rtrbitmp_products_master p
inner join analytics.cc_products_vocabulary v on p.nid = v.nid and v.vocabulary_name = 'Formality'
set
 p.formality = v.term_name
;

-- update url
update analytics.rtrbitmp_products_master p
inner join analytics.cc_products_url u on p.nid = u.nid
set
 p.product_url = u.product_url
,p.image_url = u.image_url
,p.product_description = u.product_description;

-- update received
update analytics.rtrbitmp_products_master p
inner join 
(select 
 k.styleName
,min(r.received_date) as received_date
from rtrbi.products_master_sku_keys k
inner join rtrbi.products_received_dates r on k.sku = r.sku
group by 1) r on p.model = r.styleName
set 
 p.is_received = 1
,p.received_date = r.received_date;

-- update replenishment
update analytics.rtrbitmp_products_master p
inner join rtrbi.products_replenishment_dates r on p.nid = r.nid
set
 p.is_replenishment_style = case when curdate() between r.start_date and r.end_date then 1 else 0 end
,p.replenishment_start_date = r.start_date
,p.replenishment_end_date = r.end_date;

-- update avl_fiscal_month
update analytics.rtrbitmp_products_master p
inner join rtrbi.dim_fiscal_dates d on p.avl_date = d.asof_date
set p.avl_fiscal_month = d.fiscal_year_month;

-- update flags
-- - should_return = item has rent_end_date and should be returned
-- - should_ship = item should have a shipment
update analytics.rtrbitmp_products_master p
left join rtrbi.special_products s on p.nid = s.nid
set
 p.should_ship = coalesce(s.should_ship,1)
,p.should_return = 
	case 
		when p.special_type = 'C' then 0 -- clearance
		when p.type = 'U' then 0 -- upsell
		else coalesce(s.should_return,1)
	end
;

-- update clearance nodes to have data for associated rentable node
update analytics.rtrbitmp_products_master m
inner join analytics.rtrbitmp_products_master p on m.rentable_nid = p.nid and m.rentable_nid != m.nid
set
 m.mongo_collection = p.mongo_collection
,m.mongo_id = p.mongo_id
,m.designer_id = p.designer_id
,m.designer = p.designer
,m.designer_classification = p.designer_classification
,m.title = p.title
,m.sub_type_id = p.sub_type_id
,m.sub_type = p.sub_type
,m.secondary_type = p.secondary_type
,m.season_code = p.season_code
,m.embellishment = p.embellishment
,m.length = p.length
,m.neckline = p.neckline
,m.sleeve = p.sleeve
,m.style = p.style
,m.occasion = p.occasion
,m.color = p.color
,m.secondary_color = p.secondary_color
,m.print_type = p.print_type
,m.fabric_code = p.fabric_code
,m.stretch = p.stretch
,m.body_types = p.body_types
,m.bra_type = p.bra_type
,m.models_name = p.models_name
,m.plus_style = p.plus_style
,m.moda_style = p.moda_style
,m.component_list = p.component_list
,m.marketing_occasions = p.marketing_occasions
,m.fashion_value = p.fashion_value
,m.category_depth = p.category_depth
,m.style_out = p.style_out
,m.vendorSKU = p.vendorSKU
,m.is_received = p.is_received
,m.received_date = p.received_date
,m.is_replenishment_style = p.is_replenishment_style
,m.replenishment_start_date = p.replenishment_start_date
,m.replenishment_end_date = p.replenishment_end_date
,m.in_survey = p.in_survey
,m.is_bridesmaid_style = p.is_bridesmaid_style
,m.product_url = p.product_url
,m.image_url = p.image_url
,m.product_description = p.product_description
,m.image_count = p.image_count
,m.fitNotes = p.fitNotes
;

-- build iid/sku master
drop table if exists analytics.rtrbitmp_products_master_iid;

create table analytics.rtrbitmp_products_master_iid
like rtrbi.products_master_iid;

insert into analytics.rtrbitmp_products_master_iid
(
 combo_type
,special_type
,special_product
,type
,node_type
,iid
,sku
,mongo_collection
,mongo_id
,mongo_clearance
,nid
,rentable_nid
,model
,size
,designer_id
,designer
,designer_classification
,title
,avl_date
,avl_fiscal_month
,on_site
,list_price
,cost
,sell_price
,clearance_price
,sub_type_id
,sub_type
,secondary_type
,season_code
,embellishment
,length
,neckline
,sleeve
,style
,occasion
,color
,secondary_color
,print_type
,fabric_code
,stretch
,body_types
,bra_type
,models_name
,plus_style
,moda_style
,formality
,component_list
,marketing_occasions
,fashion_value
,category_depth
,style_out
,vendorSKU
,is_replenishment_style
,replenishment_start_date
,replenishment_end_date
,in_survey
,should_ship
,should_return
,is_bridesmaid_style
,product_url
,image_url
,product_description
,image_count
,fitNotes
)
select
 p.combo_type
,p.special_type
,p.special_product
,p.type
,p.node_type
,k.iid
,k.sku
,p.mongo_collection
,p.mongo_id
,p.mongo_clearance
,p.nid
,p.rentable_nid
,p.model
,k.size
,p.designer_id
,p.designer
,p.designer_classification
,p.title
,p.avl_date
,p.avl_fiscal_month
,p.on_site
,p.list_price
,p.cost
,p.sell_price
,p.clearance_price
,p.sub_type_id
,p.sub_type
,p.secondary_type
,p.season_code
,p.embellishment
,p.length
,p.neckline
,p.sleeve
,p.style
,p.occasion
,p.color
,p.secondary_color
,p.print_type
,p.fabric_code
,p.stretch
,p.body_types
,p.bra_type
,p.models_name
,p.plus_style
,p.moda_style
,p.formality
,p.component_list
,p.marketing_occasions
,p.fashion_value
,p.category_depth
,p.style_out
,p.vendorSKU
,p.is_replenishment_style
,p.replenishment_start_date
,p.replenishment_end_date
,p.in_survey
,p.should_ship
,p.should_return
,p.is_bridesmaid_style
,p.product_url
,p.image_url
,p.product_description
,p.image_count
,p.fitNotes
from analytics.rtrbitmp_products_master p
inner join rtrbi.products_master_sku_keys k on p.model = k.styleName
;

-- update unit counts
update analytics.rtrbitmp_products_master_iid p
inner join rtrbi.products_unit_totals t on p.iid = t.iid
set
 p.total_units = t.total_units
,p.rtv_deactivations = t.rtv_deactivations
,p.short_ship_deactivations = t.short_ship_deactivations
,p.other_deactivations = t.other_deactivations

,p.auto_late_deactivations = t.auto_late_deactivations
,p.auto_evaluate_deactivations = t.auto_evaluate_deactivations

,p.active_units = t.active_units
,p.clearance_units = t.clearance_units
,p.sold_units = t.sold_units
,p.qty = t.qty
,p.avl_qty = t.avl_qty
,p.drupal_active_units = t.drupal_active_units
,p.rescal_active_units = t.rescal_active_units
,p.rescal_active_barcodes = t.rescal_active_barcodes;

-- update received
update analytics.rtrbitmp_products_master_iid p
inner join rtrbi.products_received_dates r on p.sku = r.sku
set 
 p.is_received = 1
,p.received_date = r.received_date;

-- update master tables
delete from rtrbi.products_master 
where nid not in (select nid from analytics.rtrbitmp_products_master);

replace into rtrbi.products_master
select * from analytics.rtrbitmp_products_master;

delete from rtrbi.products_master_iid
where iid not in (select iid from analytics.rtrbitmp_products_master_iid);

replace into rtrbi.products_master_iid
select * from analytics.rtrbitmp_products_master_iid;

-- cleanup
drop table if exists analytics.rtrbitmp_products_master;
drop table if exists analytics.rtrbitmp_products_master_iid;

-- update data_ready
replace into rtrbi.data_ready(data_set,run_date)
select 'rtrbi.products_master_iid',curdate()
UNION
select 'rtrbi.products_master',curdate()
;

-- update cc_products tables for consistency
drop table if exists analytics.rtrbitmp_cc_products_iid;

create table analytics.rtrbitmp_cc_products_iid
like analytics.cc_products_iid;

insert into analytics.rtrbitmp_cc_products_iid
(
 special_product
,type
,iid
,sku
,nid
,model
,size
,designer_id
,designer
,title
,avl_date
,avl_fiscal_month
,on_site
,retail_price
,rtr_cost
,rental_price
,sub_type_id
,sub_type
,season_code
,color
,fashion_value
,total_units
,active_units
,is_bridesmaid
,is_clearance
)
select
 p.special_product
,p.type
,p.iid
,p.sku
,p.nid
,p.model
,p.size
,p.designer_id
,p.designer
,p.title
,p.avl_date
,p.avl_fiscal_month
,p.on_site
,p.list_price
,p.cost
,p.sell_price
,p.sub_type_id
,p.sub_type
,p.season_code
,p.color
,p.fashion_value
,p.total_units
,p.active_units
,p.is_bridesmaid_style
,0 as is_clearance
from rtrbi.products_master_iid p
where p.special_type != 'C';

-- set clearance flag
update analytics.rtrbitmp_cc_products_iid c
inner join rtrbi.products_master p on c.nid = p.rentable_nid and p.nid != p.rentable_nid
set
 c.is_clearance = 1
;

-- update dates
update analytics.rtrbitmp_cc_products_iid c
set
 c.avl_month = date_format(c.avl_date,'%Y-%m')
,c.days_avl = datediff(curdate(),c.avl_date)
,c.months_avl = period_diff(date_format(curdate(),'%Y%m'),date_format(c.avl_date,'%Y%m'));

update analytics.rtrbitmp_cc_products_iid c
inner join rtrbi.dates_dim d1 on d1.calendar = 'fiscal' and c.avl_date = d1.asof_date
inner join rtrbi.dates_dim d2 on d2.calendar = 'fiscal' and d2.asof_date = curdate()
set
 c.fiscal_months_avl = d2.month_id - d1.month_id;

update analytics.rtrbitmp_cc_products_iid c
inner join rtr_prod0808.node n on c.nid = n.nid
set c.end_date =
	case
		when c.is_clearance = 1 and n.status = 0 and from_unixtime(n.`changed`, '%Y-%m-%d') < '2011-01-01' then '2010-12-08'
		when c.is_clearance = 1 and n.status = 0 then '2011-08-08'
		when n.status = 0 then from_unixtime( n.`changed`, '%Y-%m-%d')
		else curdate()
	end;

update analytics.rtrbitmp_cc_products_iid c
set c.end_month = date_format(c.end_date,'%Y-%m');

-- update price group
update analytics.rtrbitmp_cc_products_iid c
left join rtrbi.products_price_groups g on c.type = g.type and c.rental_price between g.min_price and g.max_price
set c.price_group = coalesce(g.price_group,'Unknown')
;

drop table if exists analytics.rtrbitmp_cc_products;

create table analytics.rtrbitmp_cc_products
like analytics.cc_products;

insert into analytics.rtrbitmp_cc_products
(
 nid
,designer_id
,designer
,title
,model
,type
,sub_type_id
,sub_type
,avl_date
,avl_month
,avl_fiscal_month
,days_avl
,months_avl
,fiscal_months_avl
,retail_price
,rtr_cost
,rental_price
,price_group
,on_site
,is_clearance
,total_units
,active_units
,end_date
,end_month
,season_code
,fashion_value
,is_bridesmaid
,special_product
)
select
 p.nid
,p.designer_id
,p.designer
,p.title
,p.model
,p.type
,p.sub_type_id
,p.sub_type
,min(p.avl_date) as avl_date
,min(p.avl_month) as avl_month
,min(p.avl_fiscal_month) as avl_fiscal_month
,max(p.days_avl) as days_avl
,max(p.months_avl) as months_avl
,max(p.fiscal_months_avl) as fiscal_months_avl
,p.retail_price
,p.rtr_cost
,p.rental_price
,p.price_group
,p.on_site
,p.is_clearance
,sum(p.total_units) as total_units
,sum(p.active_units) as active_units
,max(p.end_date) as end_date
,max(p.end_month) as end_month
,p.season_code
,p.fashion_value
,p.is_bridesmaid
,p.special_product
from analytics.rtrbitmp_cc_products_iid p
group by p.nid;

-- update main tables
delete from analytics.cc_products_iid
where iid not in (select iid from analytics.rtrbitmp_cc_products_iid);

replace into analytics.cc_products_iid
select * from analytics.rtrbitmp_cc_products_iid;

delete from analytics.cc_products
where nid not in (select nid from analytics.rtrbitmp_cc_products);

replace into analytics.cc_products
select * from analytics.rtrbitmp_cc_products;

-- cleanup
drop table if exists analytics.rtrbitmp_cc_products_iid;
drop table if exists analytics.rtrbitmp_cc_products;

-- update data_ready
replace into rtrbi.data_ready(data_set,run_date)
select 'analytics.cc_products',curdate()
UNION
select 'analytics.cc_products_iid',curdate()
;
commit;