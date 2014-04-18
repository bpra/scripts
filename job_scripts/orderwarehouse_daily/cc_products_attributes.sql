set time_zone = 'America/New_York';
start transaction;
-- cc_products_vocabulary (from drupal)
drop table if exists analytics.cctmp_products_vocabulary;

create temporary table analytics.cctmp_products_vocabulary
like analytics.cc_products_vocabulary;

insert ignore into analytics.cctmp_products_vocabulary
select
 p.nid
,v.vid
,t.tid
,v.name as vocabulary_name
,t.name as term_name
from rtrbi.products_master_style_keys p
inner join rtr_prod0808.term_node n on p.nid = n.nid
inner join rtr_prod0808.term_data t on n.tid = t.tid
inner join rtr_prod0808.vocabulary v on t.vid = v.vid
;

update analytics.cc_products_vocabulary v
left join analytics.cctmp_products_vocabulary t using (nid,vid,tid)
set v.vocabulary_name = null
where t.nid is null
;

delete from analytics.cc_products_vocabulary
where vocabulary_name is null
;

replace into analytics.cc_products_vocabulary
select * from analytics.cctmp_products_vocabulary
;

drop table if exists analytics.cctmp_products_vocabulary;

-- CC_PRODUCTS_ATTRIBUTES
-- one record per nid with list of selected attribute values
drop table if exists analytics.cctmp_products_attributes;

create temporary table analytics.cctmp_products_attributes
like analytics.cc_products_attributes;

insert into analytics.cctmp_products_attributes
select
 nid

,group_concat(case when vocabulary_name = 'Embellishment' then trim(term_name) else null end order by term_name) as embellishment
,group_concat(case when vocabulary_name = 'length' then trim(term_name) else null end order by term_name) as length
,group_concat(case when vocabulary_name = 'Neckline' then trim(term_name) else null end order by term_name) as neckline
,group_concat(case when vocabulary_name = 'Sleeve' then trim(term_name) else null end order by term_name) as sleeve
,group_concat(case when vocabulary_name = 'Style' then trim(term_name) else null end order by term_name) as style
,group_concat(case when vocabulary_name = 'Occasion' then trim(term_name) else null end order by term_name) as occasion
,group_concat(case when vocabulary_name = 'Color' then trim(term_name) else null end order by term_name) as color

from analytics.cc_products_vocabulary

where vocabulary_name in 
(
 'Color'
,'Embellishment'
,'length'
,'Neckline'
,'Sleeve'
,'Style'
,'Occasion'
)

group by nid
;

delete from analytics.cc_products_attributes
where nid not in (select nid from analytics.cctmp_products_attributes);

replace into analytics.cc_products_attributes
select * from analytics.cctmp_products_attributes;

drop table if exists analytics.cctmp_products_attributes;

-- CC_PRODUCTS_ATTRIBUTES_ALL
-- table of all attributes by nid
drop table if exists analytics.cctmp_products_attributes_all;

create temporary table analytics.cctmp_products_attributes_all
like analytics.cc_products_attributes_all;

insert into analytics.cctmp_products_attributes_all
select
 nid
,sum(case when vocabulary_name = 'Color' and term_name = 'Black' then 1 else 0 end) as 'Color-Black'
,sum(case when vocabulary_name = 'Color' and term_name = 'Blue' then 1 else 0 end) as 'Color-Blue'
,sum(case when vocabulary_name = 'Color' and term_name = 'Brown' then 1 else 0 end) as 'Color-Brown'
,sum(case when vocabulary_name = 'Color' and term_name = 'Colored' then 1 else 0 end) as 'Color-Colored'
,sum(case when vocabulary_name = 'Color' and term_name = 'Cream' then 1 else 0 end) as 'Color-Cream'
,sum(case when vocabulary_name = 'Color' and term_name = 'Crystal Clear' then 1 else 0 end) as 'Color-Crystal Clear'
,sum(case when vocabulary_name = 'Color' and term_name = 'Gold' then 1 else 0 end) as 'Color-Gold'
,sum(case when vocabulary_name = 'Color' and term_name = 'Green' then 1 else 0 end) as 'Color-Green'
,sum(case when vocabulary_name = 'Color' and term_name = 'Grey' then 1 else 0 end) as 'Color-Grey'
,sum(case when vocabulary_name = 'Color' and term_name = 'Nude' then 1 else 0 end) as 'Color-Nude'
,sum(case when vocabulary_name = 'Color' and term_name = 'Orange' then 1 else 0 end) as 'Color-Orange'
,sum(case when vocabulary_name = 'Color' and term_name = 'Pearl' then 1 else 0 end) as 'Color-Pearl'
,sum(case when vocabulary_name = 'Color' and term_name = 'Pink' then 1 else 0 end) as 'Color-Pink'
,sum(case when vocabulary_name = 'Color' and term_name = 'Print' then 1 else 0 end) as 'Color-Print'
,sum(case when vocabulary_name = 'Color' and term_name = 'Purple' then 1 else 0 end) as 'Color-Purple'
,sum(case when vocabulary_name = 'Color' and term_name = 'Red' then 1 else 0 end) as 'Color-Red'
,sum(case when vocabulary_name = 'Color' and term_name = 'Rose Gold ' then 1 else 0 end) as 'Color-Rose Gold'
,sum(case when vocabulary_name = 'Color' and term_name = 'Silver' then 1 else 0 end) as 'Color-Silver'
,sum(case when vocabulary_name = 'Color' and term_name = 'White' then 1 else 0 end) as 'Color-White'
,sum(case when vocabulary_name = 'Color' and term_name = 'Yellow' then 1 else 0 end) as 'Color-Yellow'
,sum(case when vocabulary_name = 'Embellishment' and term_name = 'Beads' then 1 else 0 end) as 'Embellishment-Beads'
,sum(case when vocabulary_name = 'Embellishment' and term_name = 'Lace' then 1 else 0 end) as 'Embellishment-Lace'
,sum(case when vocabulary_name = 'Embellishment' and term_name = 'Sequins' then 1 else 0 end) as 'Embellishment-Sequins'
,sum(case when vocabulary_name = 'length' and term_name = 'Floor length' then 1 else 0 end) as 'length-Floor length'
,sum(case when vocabulary_name = 'length' and term_name = 'Knee length' then 1 else 0 end) as 'length-Knee length'
,sum(case when vocabulary_name = 'length' and term_name = 'Mid-thigh length' then 1 else 0 end) as 'length-Mid-thigh length'
,sum(case when vocabulary_name = 'length' and term_name = 'Mini' then 1 else 0 end) as 'length-Mini'
,sum(case when vocabulary_name = 'length' and term_name = 'Tea length' then 1 else 0 end) as 'length-Tea length'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Asymmetric' then 1 else 0 end) as 'Neckline-Asymmetric'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Boat' then 1 else 0 end) as 'Neckline-Boat'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Crew' then 1 else 0 end) as 'Neckline-Crew'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Halter' then 1 else 0 end) as 'Neckline-Halter'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Off the shoulder' then 1 else 0 end) as 'Neckline-Off the shoulder'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Scoop' then 1 else 0 end) as 'Neckline-Scoop'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Square' then 1 else 0 end) as 'Neckline-Square'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Strapless' then 1 else 0 end) as 'Neckline-Strapless'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'Sweetheart' then 1 else 0 end) as 'Neckline-Sweetheart'
,sum(case when vocabulary_name = 'Neckline' and term_name = 'V-neck' then 1 else 0 end) as 'Neckline-V-neck'
,sum(case when vocabulary_name = 'Sleeve' and term_name = 'Cap sleeve' then 1 else 0 end) as 'Sleeve-Cap sleeve'
,sum(case when vocabulary_name = 'Sleeve' and term_name = 'Long sleeve' then 1 else 0 end) as 'Sleeve-Long sleeve'
,sum(case when vocabulary_name = 'Sleeve' and term_name = 'One shoulder' then 1 else 0 end) as 'Sleeve-One shoulder'
,sum(case when vocabulary_name = 'Sleeve' and term_name = 'Sleeveless' then 1 else 0 end) as 'Sleeve-Sleeveless'
,sum(case when vocabulary_name = 'Sleeve' and term_name = 'Strapless' then 1 else 0 end) as 'Sleeve-Strapless'
,sum(case when vocabulary_name = 'Sleeve' and term_name = 'Three quarter sleeve' then 1 else 0 end) as 'Sleeve-Three quarter sleeve'
,sum(case when vocabulary_name = 'Style' and term_name = 'Classic' then 1 else 0 end) as 'Style-Classic'
,sum(case when vocabulary_name = 'Style' and term_name = 'Edgy' then 1 else 0 end) as 'Style-Edgy'
,sum(case when vocabulary_name = 'Style' and term_name = 'Feminine' then 1 else 0 end) as 'Style-Feminine'
,sum(case when vocabulary_name = 'Style' and term_name = 'Trendy' then 1 else 0 end) as 'Style-Trendy'
from analytics.cc_products_vocabulary
where vocabulary_name in 
(
 'Color'
,'Embellishment'
,'length'
,'Neckline'
,'Sleeve'
,'Style'
)
group by 1
;

delete from analytics.cc_products_attributes_all
where nid not in (select nid from analytics.cctmp_products_attributes_all)
;

replace into analytics.cc_products_attributes_all
select * from analytics.cctmp_products_attributes_all
;

drop table if exists analytics.cctmp_products_attributes_all;

-- CC_PRODUCTS_URL
-- TODO: Replace this with data from product catalog service: http://productcatalog-lb.prod.renttherunway.it:9001/all/style_name/HL30
drop table if exists analytics.cctmp_products_url;

create temporary table analytics.cctmp_products_url
like analytics.cc_products_url;

insert into analytics.cctmp_products_url
(
 nid
,model

,product_url
,image_url
,full_image_url

,prod_desc
,access_desc

,product_description
)
select 
 p.nid
,p.styleName as model

,trim(concat('http://www.renttherunway.com/',ua.dst)) AS product_url
,null as image_url
,null as full_image_url

,trim(replace(replace(ctp.field_editor_note_value,'\n',''),'\r','')) AS prod_desc
,trim(replace(replace(cta.field_notes_value,'\n',''),'\r','')) AS access_desc

,trim(coalesce(
  replace(replace(ctp.field_editor_note_value,'\n',''),'\r','') 
 ,replace(replace(cta.field_notes_value,'\n',''),'\r','') 
 ,replace(replace(cts.field_ess_editor_note_value,'\n',''),'\r','') 
)) as product_description

from rtrbi.products_master_style_keys p
left join rtr_prod0808.url_alias AS ua on CONCAT('style_name/',p.styleName) = ua.src

left join rtr_prod0808.content_type_product AS ctp on ctp.nid = p.nid and ctp.vid <> 302
left join rtr_prod0808.content_type_accessories AS cta on cta.nid = p.nid
left join rtr_prod0808.content_type_saleableproduct cts on cts.nid = p.nid

where (ua.pid is null or ua.pid = (select max(x.pid) from rtr_prod0808.url_alias x where ua.src = x.src))
;

-- set anything missing to point to node url
update analytics.cctmp_products_url u
set u.product_url = trim(concat('http://www.renttherunway.com/node/',u.nid))
where u.product_url is null;

-- update image_url -- using content_field_product_image - should be all dresses
update analytics.cctmp_products_url u
inner join rtr_prod0808.content_field_product_image i on u.nid = i.nid and i.delta = 0
inner join rtr_prod0808.files f on i.field_product_image_fid = f.fid

set image_url = trim(concat('http://www.renttherunway.com/',f.filepath)) 

where u.image_url is null;

-- update image_url -- using content_field_normal_images - should be everything else except clearance
update analytics.cctmp_products_url u
inner join rtr_prod0808.content_field_normal_images i on u.nid = i.nid and i.delta = 0
inner join rtr_prod0808.files f on i.field_normal_images_fid = f.fid

set image_url = trim(concat('http://www.renttherunway.com/',f.filepath))

where u.image_url is null;

-- update image_url -- using link to rentable_nid - should fix clearance
drop table if exists analytics.cctmp_products_url2;

create temporary table analytics.cctmp_products_url2
like analytics.cctmp_products_url;

insert into analytics.cctmp_products_url2
select * from analytics.cctmp_products_url;

update analytics.cctmp_products_url u
inner join analytics.kh_products_nid_master p on u.nid = p.nid
inner join analytics.cctmp_products_url2 u2 on p.rentable_nid = u2.nid

set u.image_url = u2.image_url

where u.image_url is null;

-- unset product_url for anything missing an image_url - these are probably not valid
update analytics.cctmp_products_url u

set u.product_url = null

where u.image_url is null;

-- store path without the domain
update analytics.cctmp_products_url
set
 product_path = substring(product_url,length(substring_index(product_url,'/',3))+1)
,image_path = substring(image_url,length(substring_index(image_url,'/',3))+1)
,full_image_path = substring(full_image_url,length(substring_index(full_image_url,'/',3))+1);

-- update main table
delete from analytics.cc_products_url
where concat(nid,'-',model) not in 
(select concat(nid,'-',model) from analytics.cctmp_products_url)
;

replace into analytics.cc_products_url
select * from analytics.cctmp_products_url
;

drop table if exists analytics.cctmp_products_url;
drop table if exists analytics.cctmp_products_url2;

-- update data ready
replace into rtrbi.data_ready(data_set,run_date)
select 'analytics.cc_products_vocabulary',curdate()
UNION
select 'analytics.cc_products_attributes_all',curdate()
UNION
select 'analytics.cc_products_url',curdate()
;
commit;