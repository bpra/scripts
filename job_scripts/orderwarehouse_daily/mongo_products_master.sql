set time_zone = 'America/New_York';

start transaction;
-- update mongo_products_master
drop table if exists analytics.rtrbitmp_mongo_products_master;

create temporary table analytics.rtrbitmp_mongo_products_master
like rtrbi.mongo_products_master;

insert into analytics.rtrbitmp_mongo_products_master
(
 _collection
,_id
,_created
,_modified

,active
,ageRanges
,analyticsScore
,availableFrom
,availableTo
,bodyTypes
,canonicalSizes
,category
,checkedForExtras
,className
,clearance
,clearancePrice
,colors
,cost
,designer
,displayName
,embellishments
,extendedAnalyticsScore
,extras
,fitNotes
,fwAnalyticsExperimentsScores
,gaPopularityScore
,imgLargeBack
,imgLargeEditorial
,imgLargeFront
,imgLargeNoModel
,imgLargeSide
,imgLargeTop
,imgLargeWithModel
,legacyChanged
,legacyNid
,legacyProductURL
,legacyStyles
,length
,marketingOccasions
,model
,neckline
,numberDressUpsellsMember
,numberPicturedDressUpsells
,occasionsForFilters
,outfitStyles
,productDetail
,productExtras
,productType
,rentalFee
,retailPrice
,runsShort
,searchableColors
,searchableEmbellishments
,searchableLegacyStyles
,searchableLength
,searchableNeckline
,searchableSleeve
,seasonCode
,sizes
,sizeScaleName
,sleeve
,ssAnalyticsExperimentsScores
,styleForFilters
,styleName
,styleNotes
,stylistTip
,tags
,vendorSKU
)
select 

 max(case when m.attribute_name = 'type' then m.attribute_value else null end) as _collection
,max(case when m.attribute_name = '_id' then m.attribute_value else null end) as _id

,left(max(case when m.attribute_name = '_created' then m.attribute_value else null end),10) as _created
,left(max(case when m.attribute_name = '_modified' then m.attribute_value else null end),10) as _modified

,max(case when m.attribute_name = 'active' then m.attribute_value else null end) as active
,cast(group_concat(case when m.attribute_name = 'ageRanges' then m.attribute_value else null end) as char) as ageRanges
,max(case when m.attribute_name = 'analyticsScore' then m.attribute_value else null end) as analyticsScore
,left(max(case when m.attribute_name = 'availableFrom' then m.attribute_value else null end),10) as availableFrom
,left(max(case when m.attribute_name = 'availableTo' then m.attribute_value else null end),10) as availableTo
,cast(group_concat(case when m.attribute_name = 'bodyTypes' then m.attribute_value else null end) as char) as bodyTypes
,cast(group_concat(case when m.attribute_name = 'canonicalSizes' then m.attribute_value else null end) as char) as canonicalSizes
,max(case when m.attribute_name = 'category' then m.attribute_value else null end) as category
,max(case when m.attribute_name = 'checkedForExtras' then m.attribute_value else null end) as checkedForExtras
,max(case when m.attribute_name = 'className' then m.attribute_value else null end) as className
,max(case when m.attribute_name = 'clearance' then m.attribute_value else null end) as clearance
,max(case when m.attribute_name = 'clearancePrice' then m.attribute_value else null end) as clearancePrice
,cast(group_concat(case when m.attribute_name = 'colors' then m.attribute_value else null end) as char) as colors
,max(case when m.attribute_name = 'cost' then m.attribute_value else null end) as cost
,cast(group_concat(case when m.attribute_name = 'designer' and m.attribute_index = 'primaryKey' then m.attribute_value else null end) as char) as designer
,max(case when m.attribute_name = 'displayName' then m.attribute_value else null end) as displayName
,cast(group_concat(case when m.attribute_name = 'embellishments' then m.attribute_value else null end) as char) as embellishments
,cast(group_concat(case when m.attribute_name = 'extendedAnalyticsScore' then concat(m.attribute_index,':',m.attribute_value) else null end) as char) as extendedAnalyticsScore
,cast(group_concat(case when m.attribute_name = 'extras' then concat(m.attribute_index,':',m.attribute_value) else null end) as char) as extras
,max(case when m.attribute_name = 'fitNotes' then m.attribute_value else null end) as fitNotes
,cast(group_concat(case when m.attribute_name = 'fwAnalyticsExperimentsScores' then m.attribute_value else null end) as char) as fwAnalyticsExperimentsScores
,max(case when m.attribute_name = 'gaPopularityScore' then m.attribute_value else null end) as gaPopularityScore
,max(case when m.attribute_name = 'imgLargeBack' then m.attribute_value else null end) as imgLargeBack
,max(case when m.attribute_name = 'imgLargeEditorial' then m.attribute_value else null end) as imgLargeEditorial
,max(case when m.attribute_name = 'imgLargeFront' then m.attribute_value else null end) as imgLargeFront
,max(case when m.attribute_name = 'imgLargeNoModel' then m.attribute_value else null end) as imgLargeNoModel
,max(case when m.attribute_name = 'imgLargeSide' then m.attribute_value else null end) as imgLargeSide
,max(case when m.attribute_name = 'imgLargeTop' then m.attribute_value else null end) as imgLargeTop
,max(case when m.attribute_name = 'imgLargeWithModel' then m.attribute_value else null end) as imgLargeWithModel
,left(max(case when m.attribute_name = 'legacyChanged' then m.attribute_value else null end),10) as legacyChanged
,max(case when m.attribute_name = 'legacyNid' then m.attribute_value else null end) as legacyNid
,max(case when m.attribute_name = 'legacyProductURL' then m.attribute_value else null end) as legacyProductURL
,cast(group_concat(case when m.attribute_name = 'legacyStyles' then m.attribute_value else null end) as char) as legacyStyles
,max(case when m.attribute_name = 'length' then m.attribute_value else null end) as length
,cast(group_concat(case when m.attribute_name = 'marketingOccasions' then m.attribute_value else null end) as char) as marketingOccasions
,max(case when m.attribute_name = 'model' then m.attribute_value else null end) as model
,max(case when m.attribute_name = 'neckline' then m.attribute_value else null end) as neckline
,max(case when m.attribute_name = 'numberDressUpsellsMember' then m.attribute_value else null end) as numberDressUpsellsMember
,max(case when m.attribute_name = 'numberPicturedDressUpsells' then m.attribute_value else null end) as numberPicturedDressUpsells
,cast(group_concat(case when m.attribute_name = 'occasionsForFilters' then m.attribute_value else null end) as char) as occasionsForFilters
,cast(group_concat(case when m.attribute_name = 'outfitStyles' then m.attribute_value else null end) as char) as outfitStyles
,max(case when m.attribute_name = 'productDetail' then m.attribute_value else null end) as productDetail
,cast(group_concat(case when m.attribute_name = 'productExtras' then m.attribute_value else null end) as char) as productExtras
,max(case when m.attribute_name = 'productType' then m.attribute_value else null end) as productType
,max(case when m.attribute_name = 'rentalFee' then m.attribute_value else null end) as rentalFee
,max(case when m.attribute_name = 'retailPrice' then m.attribute_value else null end) as retailPrice
,max(case when m.attribute_name = 'runsShort' then m.attribute_value else null end) as runsShort
,cast(group_concat(case when m.attribute_name = 'searchableColors' then m.attribute_value else null end) as char) as searchableColors
,cast(group_concat(case when m.attribute_name = 'searchableEmbellishments' then m.attribute_value else null end) as char) as searchableEmbellishments
,cast(group_concat(case when m.attribute_name = 'searchableLegacyStyles' then m.attribute_value else null end) as char) as searchableLegacyStyles
,max(case when m.attribute_name = 'searchableLength' then m.attribute_value else null end) as searchableLength
,max(case when m.attribute_name = 'searchableNeckline' then m.attribute_value else null end) as searchableNeckline
,max(case when m.attribute_name = 'searchableSleeve' then m.attribute_value else null end) as searchableSleeve
,max(case when m.attribute_name = 'seasonCode' then m.attribute_value else null end) as seasonCode
,cast(group_concat(case when m.attribute_name = 'sizes' then m.attribute_value else null end) as char) as sizes
,max(case when m.attribute_name = 'sizeScaleName' then m.attribute_value else null end) as sizeScaleName
,max(case when m.attribute_name = 'sleeve' then m.attribute_value else null end) as sleeve
,cast(group_concat(case when m.attribute_name = 'ssAnalyticsExperimentsScores' then m.attribute_value else null end) as char) as ssAnalyticsExperimentsScores
,cast(group_concat(case when m.attribute_name = 'styleForFilters' then m.attribute_value else null end) as char) as styleForFilters
,m.styleName
,max(case when m.attribute_name = 'styleNotes' then m.attribute_value else null end) as styleNotes
,max(case when m.attribute_name = 'stylistTip' then m.attribute_value else null end) as stylistTip
,cast(group_concat(case when m.attribute_name = 'tags' then m.attribute_value else null end) as char) as tags
,max(case when m.attribute_name = 'vendorSKU' then m.attribute_value else null end) as vendorSKU

from rtrbi.mongo_product_catalog m

group by m.styleName
;

delete from rtrbi.mongo_products_master
where _id not in (select _id from analytics.rtrbitmp_mongo_products_master)
;

replace into rtrbi.mongo_products_master
select * from analytics.rtrbitmp_mongo_products_master
;

drop table if exists analytics.rtrbitmp_mongo_products_master;

-- capture snapshot of mongo_products_master
update rtrbi.mongo_products_master_snapshot
set latest_snapshot = 0
where latest_snapshot = 1;

insert into rtrbi.mongo_products_master_snapshot
(
 asof_date

,_collection
,_id
,_created
,_modified

,active
,ageRanges
,analyticsScore
,availableFrom
,availableTo
,bodyTypes
,canonicalSizes
,category
,checkedForExtras
,className
,clearance
,clearancePrice
,colors
,cost
,designer
,displayName
,embellishments
,extendedAnalyticsScore
,extras
,fitNotes
,fwAnalyticsExperimentsScores
,gaPopularityScore
,imgLargeBack
,imgLargeEditorial
,imgLargeFront
,imgLargeNoModel
,imgLargeSide
,imgLargeTop
,imgLargeWithModel
,legacyChanged
,legacyNid
,legacyProductURL
,legacyStyles
,length
,marketingOccasions
,model
,neckline
,numberDressUpsellsMember
,numberPicturedDressUpsells
,occasionsForFilters
,outfitStyles
,productDetail
,productExtras
,productType
,rentalFee
,retailPrice
,runsShort
,searchableColors
,searchableEmbellishments
,searchableLegacyStyles
,searchableLength
,searchableNeckline
,searchableSleeve
,seasonCode
,sizes
,sizeScaleName
,sleeve
,ssAnalyticsExperimentsScores
,styleForFilters
,styleName
,styleNotes
,stylistTip
,tags
,vendorSKU

,latest_snapshot
)
select 
 curdate() as asof_date

,m._collection
,m._id
,m._created
,m._modified

,m.active
,m.ageRanges
,m.analyticsScore
,m.availableFrom
,m.availableTo
,m.bodyTypes
,m.canonicalSizes
,m.category
,m.checkedForExtras
,m.className
,m.clearance
,m.clearancePrice
,m.colors
,m.cost
,m.designer
,m.displayName
,m.embellishments
,m.extendedAnalyticsScore
,m.extras
,m.fitNotes
,m.fwAnalyticsExperimentsScores
,m.gaPopularityScore
,m.imgLargeBack
,m.imgLargeEditorial
,m.imgLargeFront
,m.imgLargeNoModel
,m.imgLargeSide
,m.imgLargeTop
,m.imgLargeWithModel
,m.legacyChanged
,m.legacyNid
,m.legacyProductURL
,m.legacyStyles
,m.length
,m.marketingOccasions
,m.model
,m.neckline
,m.numberDressUpsellsMember
,m.numberPicturedDressUpsells
,m.occasionsForFilters
,m.outfitStyles
,m.productDetail
,m.productExtras
,m.productType
,m.rentalFee
,m.retailPrice
,m.runsShort
,m.searchableColors
,m.searchableEmbellishments
,m.searchableLegacyStyles
,m.searchableLength
,m.searchableNeckline
,m.searchableSleeve
,m.seasonCode
,m.sizes
,m.sizeScaleName
,m.sleeve
,m.ssAnalyticsExperimentsScores
,m.styleForFilters
,m.styleName
,m.styleNotes
,m.stylistTip
,m.tags
,m.vendorSKU

,1 as latest_snapshot
from rtrbi.mongo_products_master m
;

-- update data ready
replace into rtrbi.data_ready(data_set,run_date)
select 'rtrbi.mongo_products_master',curdate()
;

replace into rtrbi.data_ready(data_set,run_date)
select 'rtrbi.mongo_products_master_snapshot',curdate()
;
commit;
