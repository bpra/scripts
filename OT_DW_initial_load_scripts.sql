insert into ot_store
select * from(
select 1 as storeId ,'Portal - Web' as storeName,'Virtual' as storeType
union all
select 2,'Portal - Mobile','Virtual'
union all
select 3,'App - iOS','Virtual'
union all
select 4,'App - iOS','Virtual'
union all
select 5,'Showroom','Physical'
)xx;



insert into ot_lineitemtype
select * from (
select 1 as lineItemTypeId ,'Product' as lineItemType,NULL as lineItemSubType
union all
#select 2,'Accessory','Rental'
#union all
#select 3,'Dress','Sale'
#union all
#select 4,'Accessory','Sale'
#union all
#select 5,'Bulk','Sale'
#union all
select 6,'Insurance',NULL
union all
select 7,'Tax',NULL
union all
select 8,'Coupon',NULL
union all
select 9,'Credit Refund', NULL
union all
select 10,'Credit Redeemed', NULL
union all
select 11,'Cash Refund',NULL
union all
select 12,'GiftCard Sales', NULL
union all
select 13,'GiftCard Applied', NULL
union all
select 14,'Shipping','SameDay'
union all
select 15,'Shipping','NextDay'
union all
select 16,'Shipping','Saturday'
union all
select 17,'Shipping','Standard'
union all
select 101,'SubPackages',NULL
union all
select 102,'PTDiscount',NULL
union all
select 103,'OrderTotal',NULL

)xx;


insert into ot_status
select * from 
(
	select 'ZD' as status_id,'Zero Day' as statusName
	union all
	select 'OC' as status_id,'Current' as statusName
	union all
	select 'CR','Customer Remove'
	union all
	select 'CA','Customer Add'
	union all
	select 'AR','Agent Remove'
	union all
	select 'AA','Agent Add'
)xx;

insert into ot_productordertype
select * from 
(
	select 1 as productordertypeId,'Rental' as productordertypeName
	union all
	select 2,'Sale'
	union all
	select 3,'Clearance'
)xx;
