drop table if exists ot_store;
create table ot_store
(
	storeId int NOT NULL PRIMARY KEY,
	storeName varchar(30),
	storeType varchar(15)
);


drop table if exists ot_lineitemtype;
create table ot_lineitemtype
(
lineItemTypeId int NOT NULL PRIMARY KEY,
lineItemType varchar(40),
lineItemSubType varchar(40)
);


drop table if exists ot_product;
create table ot_product
(
productId bigint NOT NULL AUTO_INCREMENT PRIMARY KEY,
iid bigint,
sku varchar(40) NOT NULL,
style varchar(40),
size varchar(10),
product_type varchar(3) NOT NULL,
designer_id int,
designer varchar(255),
title varchar(255),
list_price decimal(15,3),
cost decimal(15,3),
sell_price decimal(15,3),
clearance_price decimal(15,3),
effectiveDate datetime,
endDate datetime,
activeFlag bit
);

drop table if exists ot_status;
create table ot_status
(
status_id	varchar(2) NOT NULL PRIMARY KEY
,statusName	varchar(25)
);

drop table if exists ot_productordertype;
create table ot_productordertype
(
	productordertypeId	int NOT NULL PRIMARY KEY
	,productordertypeName	varchar(15)
);


drop table if exists ot_ordertransactiondetails;
create table ot_ordertransactiondetails
(
	order_id bigint not null,
	group_id bigint,
	uid bigint not null,
	lineItemTypeId int not null,
	productId bigint,
	modifiedDate datetime,
	statusId varchar(3),
	storeId int,
	productOrderTypeId int,
	orderDate datetime not null,
	qty double(10,2) unsigned,
	amount decimal(15,3)

);