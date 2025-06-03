CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
PRINT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>>Inserting Data Into:silver.crm_cust_info';
INSERT INTO SILVER.crm_cust_info(cst_id
      ,cst_key
      ,cst_firstname
      ,cst_lastname
      ,cst_material_status
      ,cst_gndr
      ,cst_create_date)

select
cst_id,
cst_key,
TRIM(cst_firstname)as cst_firstname,
TRIM(cst_lastname)as cst_lastname,
case when UPPER(TRIM (cst_material_status)) ='s' then 'single'
WHEN UPPER (TRIM (cst_material_status)) ='M' then 'Married'
end material_status,
case when UPPER(TRIM (cst_gndr)) ='F' then 'Female'
WHEN UPPER (TRIM (cst_gndr)) ='M' then 'Male'
end cst_gndr,
cst_create_date
from (
select*,
Row_number () over (partition by cst_id order by cst_create_date DESC) as flag_last
from bronze.crm_cust_info
where cst_id is not null
) as sub
where flag_last = 1;
--------------
PRINT '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>>Inserting Data Into:silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
    prd_id, 
    cat_id,
    prd_key, 
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    )
   
SELECT
    prd_id,
    CAST(SUBSTRING(prd_key, 1, 5) AS nvarchar) AS cat_id,
    SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
    prd_nm,
    ISNULL(prd_cost, 0) AS prd_cost,
    CASE UPPER(TRIM(prd_line))  
        WHEN 'M' THEN 'mountain'
        WHEN 'R' THEN 'mountain'
        WHEN 'S' THEN 'Other sales'
        WHEN 'T' THEN 'touring'
        ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    CAST(
        LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
        AS DATE
    ) AS prd_end_dt
FROM bronze.crm_prd_info;

-------------------------
PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>>Inserting Data Into:silver.crm_sales_details';
insert into silver.crm_sales_details (
sls_ord_num,
sls_prd_key,
sls_cust_id,
--sls_order_dt,
--sls_ship_dt,
--sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select 
sls_ord_num,
    sls_prd_key, 
    sls_cust_id,
    --CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
       -- ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
   -- END AS sls_order_dt,
    ---CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
       -- ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    --END AS sls_ship_dt,
   --- CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     --   ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
   -- END AS sls_due_dt,
    ---sales =quantity*price
    ---values must not be null,zero,or negative
    case when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity * abs (sls_price)
    then sls_quantity*abs (sls_price)
    else sls_sales
    end as sls_sales,
sls_quantity,
    CASE WHEN sls_price IS NULL OR SLS_price <= 0 
    then sls_sales/nullif(sls_quantity,0)
    else sls_price
    end as sls_price
    FROM bronze.crm_sales_details; 
    
    --------------------------------------

    PRINT '>> Truncating Table: Silver.erp_cust_azz12';
TRUNCATE TABLE Silver.erp_cust_azz12;
PRINT '>>Inserting Data Into:Silver.erp_cust_azz12';
    INSERT INTO Silver.erp_cust_azz12
(cid,bdate,gen)
select
case when cid like 'nas%'then substring(cid,4,len(cid))
Else cid
end as cid,
case when bdate >getdate() then null 
else bdate
end as bdate,
case when UPPER(TRIM(gen))IN('F,FEMALE')THEN'FEMALE'
WHEN UPPER(TRIM(gen))IN('M','MALE')THEN 'Male'
ELSE 'n/a'
END AS gen
from  bronze.erp_cust_azz12
where cid not in(select distinct cst_key from silver.crm_cust_info)
-------------------------------------------
PRINT '>> Truncating Table: silver.erp_loc_a101 ';
TRUNCATE TABLE silver.erp_loc_a101 ;
PRINT '>>Inserting Data Into:silver.erp_loc_a101 ';
INSERT INTO silver.erp_loc_a101 
(cid,cntry)
select 
replace(cid,'-','')cid,
case when TRIM(cntry)='DE' THEN 'GERMANY'
WHEN TRIM(CNTRY) IN ('US','USA')THEN 'UNITED STATES'
WHEN TRIM (CNTRY)='' OR CNTRY IS NULL THEN 'N/A'
ELSE TRIM(CNTRY)
END AS CNTRY
from bronze.erp_loc_a101
-----------------------------------------------
PRINT '>> Truncating Table: silver.erp_px_Cat_g1v2';
TRUNCATE TABLE silver.erp_px_Cat_g1v2;
PRINT '>>Inserting Data Into:silver.erp_px_Cat_g1v2';
insert into silver.erp_px_Cat_g1v2
(id,cat,subcat,maintenance)
Select id,
cat,
subcat,
maintenance 
From bronze.erp_px_Cat_g1v2
---check unwanted spaces
--select * from bronze.erp_px_Cat_g1v2
--where cat!=TRIM(cat) OR subcat!=TRIM(subcat) OR maintenance !=TRIM(Maintenance)
--data standardization & consistency

 END   
