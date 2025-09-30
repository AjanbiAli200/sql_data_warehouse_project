/*
================================================================================
Quality Checks
================================================================================
    CHECK For NULLS OR Duplicates in Primary Key
    EXCEPTION: No Result
    CHECK For Unwanted SPACES in STRING VALUES
    EXPECTATION: No Result
    Check for NULL OR Negative Numbers
    EXPECTATION: No Result
    Check the consistency of values in low cardinality columns
    EXCEPTION: No Result
    Check for Invalid DATE Orders
    Identify Out-of-Range Dates

Usage Notes:
    - Run these checks after data loading into silver layer.
    - Investigate and resolve any discrepancies found during the checks.
=================================================================================
*/


SELECT * FROM bronze.crm_cust_info;

SELECT * FROM silver.crm_cust_info;

SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;



SELECT
*
FROM(
SELECT 
*,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;



SELECT cst_lastname FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);



SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;



--------------------------------------------------------------

SELECT * FROM silver.crm_prd_info

SELECT prd_id, COUNT(*) FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT prd_nm FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

SELECT prd_cost FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

SELECT DISTINCT prd_line FROM silver.crm_prd_info

SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')

-----------------------------------------------------------------

SELECT * FROM sliver.crm_sales_details

SELECT * FROM silver.crm_sales_details

SELECT 
NULLIF(sls_order_dt, 0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8 
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101;

SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL
OR sls_quantity IS NULL
OR sls_price IS NULL
OR sls_sales <= 0
OR sls_quantity <= 0
OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price

------------------------------------------------------------------

SELECT
cid,
bdate,
gen
FROM silver.erp_cust_az12
WHERE bdate > GETDATE()

SELECT DISTINCT gen FROM silver.erp_cust_az12

SELECT * FROM silver.erp_cust_az12



SELECT * FROM silver.erp_loc_a101

SELECT * FROM silver.erp_px_cat_g1v2
