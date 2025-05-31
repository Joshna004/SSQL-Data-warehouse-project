create or alter procedure bronze.load_bronze as
begin

PRINT '==================='
print 'LOADING BRONZE LAYER'
PRINT '======================='

--Create table bronze.crm_cust_info (
--cst_id int,
--cst_key nvarchar(50),
--cst_firstname nvarchar(50),
--cst_lastname nvarchar (50),
--cst_material_status nvarchar(50),
--cst_gndr nvarchar(50),
--cst_create_date Date
--
 6BT
 );
create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar (50),
prd_nm nvarchar (50),
prd_cost int,
prd_line nvarchar (50),
prd_start_dt datetime,
prd_end_dt datetime
);

Create table bronze.erp_px_Cat_g1v2(
     id NVARCHAR(50),
     cat NVARCHAR(50),
     subcat nvarchar (50),
     maintenance nvarchar(50)
);

create table bronze.erp_loc_a101(
     cid nvarchar(50),
     cntry nvarchar(50)
);

create table bronze.erp_cust_azz12(
     cid nvarchar(50),
     bdate DATE,
     gen nvarchar(50)
);

Create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_ord_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
);


BULK INSERT bronze.crm_sales_details
from 'C:\Users\joshn\Downloads\sales_details.csv'
with (
firstrow =2,
fieldterminator =',',
Tablock
);

BULK INSERT bronze.crm_prd_info
from 'C:\Users\joshn\Downloads\prd_info.csv'
with(
firstrow =2,
fieldterminator =',',
tablock
);


Bulk insert bronze.erp_px_Cat_g1v2
from 'C:\Users\joshn\Downloads\px_cat_g1v2.csv'
with(
firstrow =2,
fieldterminator =',',
tablock
);

Bulk insert bronze.erp_cust_azz12
from 'C:\Users\joshn\Downloads\cust_az12.csv'
with(
firstrow =2,
fieldterminator =',',
tablock
);
end
