use DataWarehouse

-- this file to help with the file 'proc_load_gold.sql '  to explane step by step the explenation and the checkes for each code 
--=====================================================================================================================================================

-- =============  1st table : gold.dim_customers ========

--=====================================================================================================================================================


--26:49:00  

-- 1st to join the data the we need :

SELECT
    ci.cst_id ,                         
    ci.cst_key ,                     
    ci.cst_firstname ,                
    ci.cst_lastname , 
    ci.cst_marital_status ,
    ci.cst_gndr,
    ci.cst_create_date, 
    ca.bdate, 
    ca.gen, 
    la.cntry                           
                                    
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;


-- 2nd to check for any dublication after the join :

select cst_id, count(*) from 
(
SELECT
    ci.cst_id ,                         
    ci.cst_key ,                     
    ci.cst_firstname ,                
    ci.cst_lastname , 
    ci.cst_marital_status ,
    ci.cst_gndr,
    ci.cst_create_date, 
    ca.bdate, 
    ca.gen, 
    la.cntry                           
                                    
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
    ) t
    group by cst_id 
    having count (*) > 1    -- no result that mean that all data are good after joinning the tabels

    -- This is a very important check to be sure that all data are good and the joinning works fine ..........

    -- If we check again we can find that we have 2 sourses for genders in the 2 tabels so we have to do data integration : 

    -- we will make first a new query and move all the other column :

    SELECT distinct 
    ci.cst_gndr,
    ca.gen                       
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid

    order by 1,2    -- we can see now all the senarios and can see that the 2 tabels gives diffrent informations !!
                    -- and also we'll get nulls this is not from the silver layer but from the join which mean that there is no match data between some data
                    -- in these 2 tabels, some customers in 'crm_cust_info' are not availabole in 'erp_loc_a101' . this is an issue !!
                    -- the big one that we'll have the not matching gender in one tabel Male and the other one Femal. 
                    -- here we have to ask which data is the master the CRM or the ERP, so we can take them as a truth refrance. 
                    -- In this project the CRM is more accurate than the ERP. 


-- now we have to do data integration like this :
   SELECT distinct 
    ci.cst_gndr,
    ca.gen,   
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END  AS gender
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid
    order by 1,2     -- like that we prepare a good column for the gender . 

-- now we integrate this logic to the main table :

SELECT
    ci.cst_id ,                         
    ci.cst_key ,                     
    ci.cst_firstname ,                
    ci.cst_lastname , 
    ci.cst_marital_status ,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END  AS gender,
    ci.cst_create_date, 
    ca.bdate, 
    la.cntry                           
                                    
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;       -- now we have a clean good tabel . 

--========================================

-- now we have to prepare the tabel with frindly names and make a surrgate key for the tabel this help when joinning the tabels and this key is only 
-- in the data we use it for identifiy the data ( System geneated unique identifier assigned to each record ia a table )
-- we can make this key in the DDL or window function like this:

SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;       -- now every this is ready 

--=======================================================================

-- Creating the object and as we decide it will be a virtuale one ( A view ) so here is the view of this table :

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

--=========================================================================

-- Cheking the quality of this objet that we create :

--27:14:48

select *
from gold.dim_customers

select distinct 
gender 
from gold.dim_customers    -- all ok


--=====================================================================================================================================================

-- =============  2nd table : gold.dim_products ========

--=====================================================================================================================================================

select
    pn.prd_id ,
    pn.prd_key,      
    pn.prd_nm ,      
    pn.cat_id ,      
    pn.prd_cost  ,  
    pn.prd_line  ,  
    pn.prd_start_dt,
    pn.prd_end_dt
 from silver.crm_prd_info pn  

 -- we'll target the current data so we'll target the end_dt column with nulls as this will be the current data (open and not closed yet) that we need :

 select
    pn.prd_id ,
    pn.prd_key,      
    pn.prd_nm ,      
    pn.cat_id ,      
    pn.prd_cost  ,  
    pn.prd_line  ,  
    pn.prd_start_dt
   --pn.prd_end_dt
 from silver.crm_prd_info pn 
 where prd_end_dt is null   -- filter out all historical data now we do not need the end date in the sellection 

 ---------------------------------------------------

 -- next step to join it with the product category :

  select
    pn.prd_id ,
    pn.prd_key,      
    pn.prd_nm ,      
    pn.cat_id ,      
    pn.prd_cost  ,  
    pn.prd_line  ,  
    pn.prd_start_dt,
    pc.cat,           -- new column from the 2nd table 
    pc.subcat,        -- new column from the 2nd table
    pc.maintenance    -- new column from the 2nd table
   --pn.prd_end_dt
 from silver.crm_prd_info pn 
 LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
 where prd_end_dt is null 
 
 -------------------------------------

 -- next step checking the quality of these results and the uniqe results specially for the product key  :

 select prd_key, count(*) from (
  select
    pn.prd_id ,
    pn.prd_key,      
    pn.prd_nm ,      
    pn.cat_id ,      
    pn.prd_cost  ,  
    pn.prd_line  ,  
    pn.prd_start_dt,
    pc.cat,           -- new column from the 2nd table 
    pc.subcat,        -- new column from the 2nd table
    pc.maintenance    -- new column from the 2nd table
   --pn.prd_end_dt
 from silver.crm_prd_info pn 
 LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
 where prd_end_dt is null 

)t 
group by prd_key
having count(*) > 1     -- no result so the data is uniqe 

-----------------------------------

-- next step to group up the relevant information together :

SELECT
    pn.prd_id  ,   
    pn.prd_key   ,
    pn.prd_nm    ,
    pn.cat_id    ,
    pc.cat       ,
    pc.subcat     ,
    pc.maintenance ,
    pn.prd_cost   ,
    pn.prd_line   ,
    pn.prd_start_dt 
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; 

---------------------------------------------------

-- next step give frindly names to the columns :

SELECT
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; 
 

 ------------------------------------------------------------

 -- as this is a Dimention table we'll create a Surrogate key for it :

 SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

----------------------------------------------------------------

-- next step create the view for this table :

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data


-- cheking the view if every thing is fine :

select * from gold.dim_products  -- all ok
------------------------------------------------------------

--=====================================================================================================================================================

-- =============  3rd table : gold.fact_sales ========

--=====================================================================================================================================================

SELECT
    sd.sls_ord_num  ,
    sd.sls_prd_key ,
    sd.sls_cust_id ,
    sd.sls_order_dt ,
    sd.sls_ship_dt  ,
    sd.sls_due_dt  ,
    sd.sls_sales  ,
    sd.sls_quantity ,
    sd.sls_price  
FROM silver.crm_sales_details sd

-- Here we have to answer if this dimention or facts ?? it's sure fact table ....

-- now we'll make datalookup to join the table with the other ones with the Surrogate key and replace sd.sls_prd_key and sd.sls_cust_id with the Surrogate key that we 
-- generate in the other tabels :

SELECT
    sd.sls_ord_num  ,
    pr.product_key ,
    cu.customer_key ,
    sd.sls_order_dt ,
    sd.sls_ship_dt  ,
    sd.sls_due_dt  ,
    sd.sls_sales  ,
    sd.sls_quantity ,
    sd.sls_price  
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

---------------------------------------------------

-- Next step gives the columns frindly names:
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

-----------------------------------------------------

-- now we can crate the review for this table :

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;


-- quality check :

select * from gold.fact_sales    --- all ok 

---------
-- anothr check :

select * from gold.fact_sales f 
left join gold.dim_customers c 
on c.customer_key = f.customer_key
where c.customer_key is null     -- no results so all data are ok and perfect 

-- another check :
select * from gold.fact_sales f 
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null      -- no results so all data are ok and perfect  


--====================================================================================================================


--27:28:28

