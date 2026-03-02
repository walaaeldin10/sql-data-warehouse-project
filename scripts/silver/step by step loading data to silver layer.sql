use DataWarehouse

-- this file to help with the file 'proc_load_silver.sql '  to explane step by step the explenation and the checkes for each code 
--=====================================================================================================================================================

-- =============  1st table : bronze.crm_cust_info ========

--=====================================================================================================================================================

-- checking data from nulls and duplication in the primary key 
-- Expectation : no result

select 
cst_id,
count (*)
from bronze.crm_cust_info
group by cst_id 
having count(*) > 1 or cst_id is null 

--=================================================

select* ,
ROW_NUMBER() over ( partition by cst_id order by cst_create_date  desc ) as flage_last 
from bronze.crm_cust_info
where cst_id = 29466 


--===============================================


select * 

	from ( 
		select* ,
		ROW_NUMBER() over ( partition by cst_id order by cst_create_date  desc ) as flage_last 
		from bronze.crm_cust_info
		)t 

	where flage_last != 1  -- to show the duplication 

--========================================================

-- to check only the good ones :

select * 
	from ( 
		select* ,
		ROW_NUMBER() over ( partition by cst_id order by cst_create_date  desc ) as flage_last 
		from bronze.crm_cust_info
		where cst_id is not null
		)t
	where flage_last = 1

	--=============================================================

	-- check for unwanted spaces:
	-- expectation : no result 

	select 
	cst_firstname
	from bronze.crm_cust_info
	where cst_firstname != trim (cst_firstname)

	-- like this we got all the names that have spaces in the front or in the end
	-- we can use this also for the cst_lastname 

	--=============================================================

	-- to delete the spaces :

select 
	cst_id,
    cst_key,
    trim (cst_firstname) as cst_firstname,
    trim (cst_lastname) as cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date    

from  ( 
		select* ,
		ROW_NUMBER() over ( partition by cst_id order by cst_create_date  desc ) as flage_last 
		from bronze.crm_cust_info
		where cst_id is not null
		)t
	where flage_last = 1

	--===========================================================
	--check data standardization and consistancy

	select 
	distinct cst_gndr 
	from bronze.crm_cust_info

	-- aftre that we can change the F to be femail and M to be male like that :

	select 
	cst_id,
    cst_key,
    trim (cst_firstname) as cst_firstname,
    trim (cst_lastname) as cst_lastname,
    cst_marital_status,
    case 
		when upper (trim ( cst_gndr )) = 'F' then 'Female'
		when upper (trim ( cst_gndr )) = 'M' then 'Male'
		else 'N/A'
	end cst_gndr , 
    cst_create_date    

from  ( 
		select* ,
		ROW_NUMBER() over ( partition by cst_id order by cst_create_date  desc ) as flage_last 
		from bronze.crm_cust_info
		where cst_id is not null
		)t
	where flage_last = 1


	-- we can do the same for (cst_marital_status) as we have only S,M and Null 

	select 
	cst_id,
    cst_key,
    trim (cst_firstname) as cst_firstname,
    trim (cst_lastname) as cst_lastname,
     case 
		when upper (trim ( cst_marital_status )) = 'S' then 'Single'
		when upper (trim ( cst_marital_status )) = 'M' then 'Married'
		else 'N/A'
		end cst_marital_status , 
    case 
		when upper (trim ( cst_gndr )) = 'F' then 'Female'
		when upper (trim ( cst_gndr )) = 'M' then 'Male'
		else 'N/A'
	end cst_gndr , 

    cst_create_date    

from  ( 
		select* ,
		ROW_NUMBER() over ( partition by cst_id order by cst_create_date  desc ) as flage_last 
		from bronze.crm_cust_info
		where cst_id is not null
		)t
	where flage_last = 1

	--=====================================================================================

	-- like this we prepared the data so we have to insert it to the Silver layer :

	insert into silver.crm_cust_info ( 
	 cst_id,
    cst_key,
    cst_firstname,
    cst_lastname ,
    cst_marital_status ,
    cst_gndr,
    cst_create_date ) 

	select 
	cst_id,
    cst_key,
    trim (cst_firstname) as cst_firstname,
    trim (cst_lastname) as cst_lastname,
     case 
		when upper (trim ( cst_marital_status )) = 'S' then 'Single'
		when upper (trim ( cst_marital_status )) = 'M' then 'Married'
		else 'N/A'
		end cst_marital_status , 
    case 
		when upper (trim ( cst_gndr )) = 'F' then 'Female'
		when upper (trim ( cst_gndr )) = 'M' then 'Male'
		else 'N/A'
	end cst_gndr , 

    cst_create_date    

from  ( 
		select* ,
		ROW_NUMBER() over ( partition by cst_id order by cst_create_date  desc ) as flage_last 
		from bronze.crm_cust_info
		where cst_id is not null
		)t
	where flage_last = 1


	-- to check data transfer:
	select *
	from silver.crm_cust_info  -- evry thing is ok now we have the clean data in selver layer . 

	-- to check data clean the ID is not repeted :
	
	select 
	cst_id,
	count (*)
	from silver.crm_cust_info
	group by cst_id 
	having count(*) > 1 or cst_id is null 
	-- after excute the code there is no result so the data is clean. 

	-- checking the spaces :

	select 
	cst_firstname
	from silver.crm_cust_info
	where cst_firstname != trim (cst_firstname)  
	
	-- no result so the data is clean the same we can do for the last name :

	select 
	cst_lastname
	from silver.crm_cust_info
	where cst_lastname != trim (cst_lastname)

	-- no result so the data is clean

	-- check for Male and Female :

	select 
	distinct cst_gndr 
	from silver.crm_cust_info  -- the same with the status . no result so the data is clean

--===================================================================================================================================================

-- =============  2nd table : bronze.crm_prd_info ========

--===================================================================================================================================================

-- we will do the same like the previous table :

-- checking data from nulls and duplication in the primary key (prd_id) 
-- Expectation : no result

select 
prd_id,
count (*)
from bronze.crm_prd_info
group by prd_id 
having count(*) > 1 or prd_id is null   -- result is null so the data is good. 

--===============
-- split the (prd_key) to columns to match the link in the (silver.erp_px_cat_g1v2) to link the 2 tabels with the prd_key (cat_id) 
-- so we have to split it to match the same caracrters in the (silver.erp_px_cat_g1v2): 

select 
prd_key,
replace (substring (prd_key, 1, 5), '-', '_' )  as cat_id
from bronze.crm_prd_info

-- to check the silver.erp_px_cat_g1v2:
select distinct id from bronze.erp_px_cat_g1v2  -- so the new colomn is the same like the ( id ) one here . 


-- to find the not matching data in the (bronze.crm_prd_info) with the (bronze.erp_px_cat_g1v2):
select 
prd_key,
replace (substring (prd_key, 1, 5), '-', '_' )  as cat_id
from bronze.crm_prd_info
where replace (substring (prd_key, 1, 5), '-', '_' )  not in 
(select distinct id from bronze.erp_px_cat_g1v2 )   -- as a result we can find one result ( co_pe ) 

-- to splet the 2nd part (prd_key) to be abole to join it with another table (silver.crm_sales_details) :

select 
prd_key,
replace (substring (prd_key, 1, 5), '-', '_' )  as cat_id,
substring(prd_key, 7, len(prd_key)) as prd_key  -- this len(prd_key) to make the lenth dynamic and mathe any number. 
from bronze.crm_prd_info

-- to chack the (bronze.crm_sales_details) the targt column is (sls_prd_key):
select * from bronze.crm_sales_details 


-- to check for the products that not have orders or sales : 
select 
prd_key,
replace (substring (prd_key, 1, 5), '-', '_' )  as cat_id,
substring(prd_key, 7, len(prd_key)) as prd_key  
from bronze.crm_prd_info
where substring(prd_key, 7, len(prd_key)) not in 
( select sls_prd_key from bronze.crm_sales_details )  

-- there are a lot of results we we have doute that this is not correct so we have to check this :
select sls_prd_key from bronze.crm_sales_details where sls_prd_key like 'FG-1%'  -- no results 


-- so we can use this jut to be sure that there are some matching data :
select 
prd_key,
replace (substring (prd_key, 1, 5), '-', '_' )  as cat_id,
substring(prd_key, 7, len(prd_key)) as prd_key  
from bronze.crm_prd_info
where substring(prd_key, 7, len(prd_key)) in    -- we delete not to see the matching data 
( select sls_prd_key from bronze.crm_sales_details ) 
-- now we have a lot of matching data so it is very good to join . 

--===================

-- checking the spaces for ( prd_nm) :
-- expected result null :

select 
	prd_nm
	from bronze.crm_prd_info
	where prd_nm != trim (prd_nm)   --no result so it's good column 

--=======================

--( prd_cost) column checking quality of the numbers :
-- checking for nulls or nigative numbers :
-- expected not result 

select 
	prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null  
-- so we can find some nulls so we have to replace them with (0) as following :

select 
	isnull ( prd_cost, '0') as prd_cost
from bronze.crm_prd_info

--=========================================
--( prd_line) 
--25:48:55
-- data standardlization and consistance :
select 
distinct  prd_line
from bronze.crm_prd_info  -- like this we can see the figers 

-- to change these appreviation with frindly full nice names :
select 
case when upper (trim (prd_line)) = 'M' then 'Mountain'
	 when upper (trim (prd_line)) = 'R' then 'Road'
	 when upper (trim (prd_line)) = 'S' then 'Other Sales'
	 when upper (trim (prd_line)) = 'T' then 'Touring'
else 'N/A' 
End as prd_line
from bronze.crm_prd_info
  --  another way to write the same previouse code :
select 
case upper (trim (prd_line))
	 when 'M' then 'Mountain'
	 when 'R' then 'Road'
	 when 'S' then 'Other Sales'
	 when 'T' then 'Touring'
else 'N/A' 
End as prd_line
from bronze.crm_prd_info

--======================================================
-- Last 2 columns : prd_start_dt and prd_end_dt 
-- Check for invalid data orders :
select *
from bronze.crm_prd_info
where prd_end_dt < prd_start_dt  -- there are a lot of results so the data is not correct as the end date is smaller than the start data 

--25:56:26
-- so we have to arrange them depend on the start date in the each record ( end date will be the start date of the following period minus 1 )
-- according to each product Key.
-- code for this :

select 
prd_id,
prd_key,
--prd_start_dt ,
--prd_end_dt,
cast (prd_start_dt as date ) as prd_start_dt,
cast (lead ( prd_start_dt) over ( partition by prd_key order by prd_start_dt)-1 AS date)  as prd_end_dt 
from bronze.crm_prd_info
where prd_key in ( 'AC-HE-HL-U509-R', 'AC-HE-HL-U509' ) 
-- after that we can delete the original 2 collomn (prd_start_dt) and (prd_end_dt ) to get a clean data 

--=====================================
-- now we have to go the DDL of the selver data to add the CAT_ID column with varchart (50) to make us abole to join them with anothr table
-- we already add it 
--=====================================

-- now we prepare the final query to insert it in the selver layer . 
-- Here is the final tabel before insert it :

select 
prd_id,
replace (substring (prd_key, 1, 5), '-', '_' ) as cat_id,
substring(prd_key, 7, len(prd_key)) as prd_key,
prd_nm,
isnull ( prd_cost, '0') as prd_cost,
case upper (trim (prd_line))
	 when 'M' then 'Mountain'
	 when 'R' then 'Road'
	 when 'S' then 'Other Sales'
	 when 'T' then 'Touring'
	else 'N/A' 
End as prd_line,
cast (prd_start_dt as date ) as prd_start_dt,
cast (lead ( prd_start_dt) over ( partition by prd_key order by prd_start_dt)-1 AS date)  as prd_end_dt 
from bronze.crm_prd_info


-- To insert it :

insert into silver.crm_prd_info (
	prd_id,
    cat_id,
    prd_key,
    prd_nm ,
    prd_cost,
    prd_line ,
    prd_start_dt,
    prd_end_dt
	)

select 
prd_id,
replace (substring (prd_key, 1, 5), '-', '_' ) as cat_id, -- we do extract category ID
substring(prd_key, 7, len(prd_key)) as prd_key,           -- we do extract product KEY 
prd_nm,
isnull ( prd_cost, '0') as prd_cost,                      -- handling missing information
case upper (trim (prd_line))
	 when 'M' then 'Mountain'
	 when 'R' then 'Road'
	 when 'S' then 'Other Sales'
	 when 'T' then 'Touring'
	else 'N/A' 
End as prd_line,                                          -- map product line codes to discriptiv value 
cast (prd_start_dt as date ) as prd_start_dt,             -- we made tata type casting from date to another
cast (lead ( prd_start_dt) over ( partition by prd_key order by prd_start_dt)-1 AS date)  as prd_end_dt  -- calculate the end date one day before the start of the next period and data Enrichment 
from bronze.crm_prd_info

--================================================================
-- to check the data after inserting it :

-- 1st check for nulls and duplicated in primery key : 

select 
prd_id,
count (*)
from silver.crm_prd_info
group by prd_id 
having count (*) > 1 or prd_id is null 

-- 2nd check for unwanted spaces : 

select 
prd_nm
from silver.crm_prd_info
where prd_nm  != trim (prd_nm) 

-- 3rd check for nulls and nigative numbers : 

select 
prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null 

-- 4th check standeredlization and consistency : 

select 
distinct prd_line
from silver.crm_prd_info

-- 5th check invalid data order : 

select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt 

-- Final look : 

select *
from silver.crm_prd_info



--======================================================================================================================================================


-- =============  3rd table : bronze.crm_sales_details ========

--======================================================================================================================================================
	
	CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

--=====================================================

select 
	sls_ord_num ,
    sls_prd_key,
    sls_cust_id ,
    sls_order_dt,
    sls_ship_dt ,
    sls_due_dt  ,
    sls_sales ,
    sls_quantity ,
    sls_price 
from bronze.crm_sales_details

--===== data testing =================

-- checking for unwanted spaces in [sls_ord_num]
select 
	sls_ord_num 
from bronze.crm_sales_details
where sls_ord_num  != trim (sls_ord_num )  -- no result this is good 

-- cheking the connection if it is good with the prd_key or not with the selver layer : 

select 
    sls_prd_key
from bronze.crm_sales_details
where sls_prd_key not in ( select prd_key from silver.crm_prd_info )  -- no results this is good all product key exist in the silver product key good for join 

-- The same for sls_cust_id :

select 
    sls_cust_id
from bronze.crm_sales_details
where sls_cust_id not in ( select cst_id  from silver.crm_cust_info )  -- no results this is good all customer IDs exist in the silver product customer Ids good for join 
 
--========== Date checking and data transform: ============= 

-- the next 3 columns : [sls_order_dt][sls_ship_dt][sls_due_dt] these are integers we need to change them to dates :

select 
sls_order_dt
from bronze.crm_sales_details 
where sls_order_dt < 0    -- no nigative value good 

--checking for zeros :
select
sls_order_dt
from bronze.crm_sales_details 
where sls_order_dt < = 0    -- we have a lot we have to change them to nulls :

-- changing nulls to zero : 
select
nullif (sls_order_dt, 0 ) as sls_order_dt
from bronze.crm_sales_details 
where sls_order_dt < = 0 

-- checking if the date less than 8 numbers because this will be an issue :
select
sls_order_dt
from bronze.crm_sales_details 
where sls_order_dt < = 0  or len (sls_order_dt) != 8  -- we'll get 2 results so this is bad data 


-- to check the date out of boundert ( fack date that in not comming yet ) 
select
sls_order_dt
from bronze.crm_sales_details 
where sls_order_dt >  20500101 or sls_order_dt < 19000101 -- no result this is good . 

-- add all the date checking codes :
select
nullif (sls_order_dt, 0 ) as sls_order_dt
from bronze.crm_sales_details 
where  sls_order_dt < = 0 
or len (sls_order_dt) != 8
or sls_order_dt >  20500101 
or sls_order_dt < 19000101

-- checking for invalid date order between the 3 columns to see if they are sequances or not : 

select *
from bronze.crm_sales_details 
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt   -- no result this is good 


-- Cheking the nest 3 colomns [sls_sales][sls_quantity][sls_price]

-- there is a rule to follow ( sales = quantity * price ) no negative , zeros or nulls are allowed values /  so the 3 collomns are connected 


select distinct 
    sls_sales ,
    sls_quantity ,
    sls_price 
from bronze.crm_sales_details
where sls_sales !=  sls_quantity *  sls_price 
or  sls_sales is null or sls_quantity is null or sls_price  is null
or  sls_sales < = 0 or sls_quantity < = 0 or sls_price  < = 0
order by  sls_sales, sls_quantity , sls_price         -- there are many bad results 

-- so as per the company rule we have to deal with these bad data so we'll follow the next ruls to fix them:
-- 1- If the sales zero nulls or negative derive it using quantity and price 
-- 2- If price is zero or null calulate it using sales and qualntity 
-- 3- If the price is niative convert it to positive value 
-- These are the ruls to follow :

select distinct 
	case when sls_sales is null or sls_sales  < = 0 or sls_sales  != sls_quantity * ABS(sls_price)
	then sls_quantity * ABS(sls_price)  
	else sls_sales 
	end as sls_sales,
    sls_quantity ,
	case when  sls_price  is null or sls_price < = 0 
	then sls_price / nullif ( sls_quantity , 0 ) 
	else sls_price 
	end as sls_price
from bronze.crm_sales_details
where sls_sales !=  sls_quantity *  sls_price 
or  sls_sales is null or sls_quantity is null or sls_price  is null
or  sls_sales < = 0 or sls_quantity < = 0 or sls_price  < = 0
order by  sls_sales, sls_quantity , sls_price 


------------------------------------------------------
---- the main query to insert in the silver layer --------------

insert into silver.crm_sales_details (

sls_ord_num ,
    sls_prd_key,
    sls_cust_id ,
    sls_order_dt,
    sls_ship_dt ,
    sls_due_dt  ,
    sls_sales ,
    sls_quantity ,
    sls_price 
)
select 
	sls_ord_num ,
    sls_prd_key,
    sls_cust_id ,
	case when  sls_order_dt = 0 or len( sls_order_dt) !=8 then null 
	 else cast (cast (sls_order_dt as varchar ) as date )
	 end as sls_order_dt,
	case when  sls_ship_dt  = 0 or len( sls_ship_dt ) !=8 then null 
	 else cast (cast (sls_ship_dt  as varchar ) as date )
	 end as sls_ship_dt ,
	case when  sls_due_dt = 0 or len( sls_due_dt ) !=8 then null 
	 else cast (cast (sls_due_dt  as varchar ) as date )
	 end as sls_due_dt ,
   	case when sls_sales is null or sls_sales  < = 0 or sls_sales  != sls_quantity * ABS(sls_price)
	then sls_quantity * ABS(sls_price)  
	else sls_sales 
	end as sls_sales,        -- recalculate sales if the data is missing or invalid 
    sls_quantity ,
	case when  sls_price  is null or sls_price < = 0 
	then sls_sales / nullif ( sls_quantity , 0 ) 
	else sls_price          -- Derive price if original value is invalid 
	end as sls_price
from bronze.crm_sales_details
 


---============================================================

-- checking the data transferd :
select *
from silver.crm_sales_details  -- all ok 

----------------------------------

-- checking sales :

select *
from silver.crm_sales_details 
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt  -- all ok 

-- chiking the calculation of the products and the sales :

select 
    sls_sales ,
    sls_quantity ,
    sls_price 
from silver.crm_sales_details
where sls_sales !=  sls_quantity *  sls_price 
or  sls_sales is null or sls_quantity is null or sls_price  is null
or  sls_sales < = 0 or sls_quantity < = 0 or sls_price  < = 0
order by  sls_sales, sls_quantity , sls_price     -- all ok 

-------------------------------------------------------------------------------------------

--========================================================================================================================================

--========================   4th table silver.erp_cust_az12  ==================================================

--========================================================================================================================================

select 
  cid,  
  bdate,        
  gen  
from bronze.erp_cust_az12


-- to check with the connecting table column : cst_ key :
select 
  cid,  
  bdate,        
  gen  
from bronze.erp_cust_az12 
where cid like '%AW00011000%'   -- so we have NAS letters more that we have to delete to join with [silver].[crm_cust_info] ( cst_ key ) 

select*  from [silver].[crm_cust_info] -- to check the cst_ key to join 

---------------------
-- start transformation the data :

select 
cid,
case when cid like 'NAS%' then substring ( cid, 4, len (cid) ) 
else cid 
end as cid , 
bdate,        
gen 
from bronze.erp_cust_az12 

-- to check for unmatching data between the 2 tabels  [silver].[crm_cust_info] and bronze.erp_cust_az12  by cst_key :

select 
cid,
case when cid like 'NAS%' then substring ( cid, 4, len (cid) ) 
else cid 
end as cid , 
bdate,        
gen 
from bronze.erp_cust_az12 
where case when cid like 'NAS%' then substring ( cid, 4, len (cid) ) 
else cid 
end not in ( select distinct cst_key from  silver.crm_cust_info )   -- no result this is good so the data is clean and match each others 

--------------------------------

-- checking the 2nd column bdate :

select 
  bdate        
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE ()  -- we have a lot of result so there are a bad data 

-- cleaning the data :

select 
  bdate, 
  case when bdate > getdate () then null 
  else bdate
  end as bdate
from bronze.erp_cust_az12

-----------------------------------

-- cheking the last column gen 

select distinct gen 
from bronze.erp_cust_az12  -- bad data 

-- clean data :

select DISTINCT 
gen, 
case when upper (trim (gen)) in ( 'F', 'FEMALE' ) then 'FEMALE' 
     when upper (trim (gen)) in ( 'M', 'MALE' ) then 'MALE'  
	 ELSE 'N/A' 
	 END AS gen

	 FROM bronze.erp_cust_az12 

--------------------------------------------------------

-- general query for clean data :

select 
case when cid like 'NAS%' then substring ( cid, 4, len (cid) ) 
else cid 
end as cid , 
 case when bdate > getdate () then null 
  else bdate
  end as bdate,       
case when upper (trim (gen)) in ( 'F', 'FEMALE' ) then 'FEMALE' 
     when upper (trim (gen)) in ( 'M', 'MALE' ) then 'MALE'  
	 ELSE 'N/A' 
	 END AS gen 
from bronze.erp_cust_az12 

------------------------------------------------------

-- insert the table into the selver layer : 

insert into silver.erp_cust_az12 (
  cid,  
  bdate,        
  gen  
  )
  select 
case when cid like 'NAS%' then substring ( cid, 4, len (cid) ) 
else cid 
end as cid ,                               -- removing 'NAS' perfix from the column data 
 case when bdate > getdate () then null 
  else bdate
  end as bdate,                           -- set future birtdate to null       
case when upper (trim (gen)) in ( 'F', 'FEMALE' ) then 'FEMALE' 
     when upper (trim (gen)) in ( 'M', 'MALE' ) then 'MALE'  
	 ELSE 'N/A' 
	 END AS gen                           -- normalize genders value and handel unkown cases 
from bronze.erp_cust_az12 

--------------------------------------------

-- cehcking the data in the selver layer :

select 
  bdate        
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > GETDATE ()  --  ok

select distinct gen  
from silver.erp_cust_az12   -- ok 

-- general view :

select* from silver.erp_cust_az12

--=========================================================================================================================================================================

--============================== 5th table Clean and load silver.erp_loc_a101    ============================================================================================

--==========================================================================================================================================================================

select 
  cid ,
  cntry
from bronze.erp_loc_a101

----------------------------------

-- we have to connect this tabel 'cid' with the 'cast_key'  from the other tabel  'silver.crm_cust_info' :

select *
from silver.crm_cust_info


--------------------------------

-- Transform the date to delet the '-' 

select 
replace (cid, '-', '' ) as cid 
from bronze.erp_loc_a101

---------------------
-- to check : 

select 
replace (cid, '-', '' ) as cid 
from bronze.erp_loc_a101
where replace (cid, '-', '' ) not in ( select cst_key from silver.crm_cust_info )   -- nothing so all ok 

----------------------------

-- checking the 2nd column country :

select  distinct 
 cntry
from bronze.erp_loc_a101  -- there are a lot of bad data 

-- cleanning the data and make it standerdlize and consistance :

select
case when trim (cntry) = 'DE' then 'Germany'
when trim( cntry ) in ( 'US', 'USA' ) then ' United States '
when trim( cntry ) = '' or cntry is null then 'N/A' 
else trim (cntry) 
end as cntry 
from bronze.erp_loc_a101 

---------------------------------
-- main query :

select 
replace (cid, '-', '' ) as cid, 
case when trim (cntry) = 'DE' then 'Germany'
when trim( cntry ) in ( 'US', 'USA' ) then ' United States '
when trim( cntry ) = '' or cntry is null then 'N/A' 
else trim (cntry) 
end as cntry
from bronze.erp_loc_a101 



------------------------------------

-- Insert the data in the silver layer:

insert into silver.erp_loc_a101 (
 cid ,
 cntry
  )
select 
replace (cid, '-', '' ) as cid,    -- manupulate the data to delete a part 
case when trim (cntry) = 'DE' then 'Germany'
when trim( cntry ) in ( 'US', 'USA' ) then ' United States '
when trim( cntry ) = '' or cntry is null then 'N/A' 
else trim (cntry) 
end as cntry                       -- we do normalize the country name and handel the missing and blanck data 
from bronze.erp_loc_a101     

-------------------------------------------

--Checking the data 

select distinct 
cntry
from silver.erp_loc_a101
order by cntry   -- all ok 

-- over all look :

select* 
from silver.erp_loc_a101  -- all ok 

--==========================================================================================================================================================

--=================================== 6th table silver.erp_px_cat_g1v2 =====================================================================================

--===========================================================================================================================================================
select 
id,  
cat,     
subcat, 
maintenance
from bronze.erp_px_cat_g1v2

--------------------------------------------
-- we have to connect this table wite the product Id ( cat_id ) from [silver].[crm_prd_info] .

select * from silver.crm_prd_info   -- all ok 

--------------------------------------------
-- checking the 2nd column 'cat' :
select distinct 
cat   
from bronze.erp_px_cat_g1v2   -- all uniq category all ok 

select distinct 
cat   
from bronze.erp_px_cat_g1v2
where cat != trim(cat)   -- no result so all ok 

-------------------------------------------
-- checking the 3rd column 'subcat' :
select distinct 
subcat 
from bronze.erp_px_cat_g1v2   -- all uniq category all ok 

select distinct 
subcat  
from bronze.erp_px_cat_g1v2
where subcat != trim(subcat)   -- no result so all ok 

------------------------------------------
-- checking the 4th column 'maintenance' :

select distinct 
maintenance 
from bronze.erp_px_cat_g1v2   -- all uniq category all ok 

select distinct 
maintenance 
from bronze.erp_px_cat_g1v2
where maintenance != trim(maintenance)   -- no result so all ok 

------------------------------------------------

-- this table has a good data quality we do not have to make any changes so we'll load it directly into the selver layer :

insert into silver.erp_px_cat_g1v2 (
id,  
cat,     
subcat, 
maintenance
)

select 
id,  
cat,     
subcat, 
maintenance
from bronze.erp_px_cat_g1v2

-------------------------------

-- cheching the date :
select * from silver.erp_px_cat_g1v2   -- all ok 

--=======================================================================================================================================================

-- we have to check the file 'proc_load_silver' for the rest of the final code to make the code reload every time we run it so there are no dublication 

--========================================================================================================================================================

