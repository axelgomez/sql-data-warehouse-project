-- ============================================
-- ============================================
-- Quality Checks for
-- Table: silver.crm_cust_info
-- ============================================
-- ============================================


-- ============================================
-- Check for nulls or duplicates in primary key
-- Expectation: no result
-- ============================================

-- Primary key must be unique and not null
SELECT
    cst_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

SELECT
*
FROM silver.crm_cust_info
WHERE cst_id = 29466;

-- =========================
-- Check for unwanted spaces
-- Expectation: no results
-- =========================

SELECT *
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
-- There are results

SELECT *
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)
-- There are results

SELECT *
FROM silver.crm_cust_info
WHERE cst_marital_status != TRIM(cst_marital_status)
-- No results

SELECT *
FROM silver.crm_cust_info
WHERE cst_gndr != TRIM(cst_gndr)
-- No results

SELECT *
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key)
-- No results

-- =================================
-- Data Standarization & Consistency
-- =================================

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info

SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info


-- Check for Invalid Data Orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt
-- 200 results

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt > prd_start_dt
-- 0 results

-- We are going to ask expert why

-- --------------------------------------------------
-- Intermediate query to solve the previous problem
-- THIS IS NOT A TEST but a custom query
-- Date fixing query:
SELECT
    *,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt ASC)-1 AS prd_end_dt_2
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')
-- --------------------------------------------------


-- ============================================
-- ============================================
-- Quality Checks for
-- Table: silver.crm_sales_details
-- ============================================
-- ============================================

-- Remove unwanted spaces
-- Expected: no results
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)
-- No results

-- Data consistency before join
-- Expected: no results
-- prd_key column
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
-- No results

-- cust id column
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
-- No results

-- Data consistency
-- Expected: no results
-- ONLY REPRODUCIBLE IN BRONZE LAYER
-- date columns
SELECT
    NULLIF(sls_order_dt,0) sls_order_dt
FROM silver.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20500101 OR sls_order_dt < 19000101
-- We have zeros (first conditional)
-- and we have 2 dates that are invalid: 32154 and 5489

-- ONLY REPRODUCIBLE IN BRONZE LAYER
SELECT
    NULLIF(sls_ship_dt,0) sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 OR sls_ship_dt > 20500101 OR sls_ship_dt < 19000101
-- No results => but we may copy the same logic just in case

-- ONLY REPRODUCIBLE IN BRONZE LAYER
-- Same for the other column: sls_due_dt
SELECT
    NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 OR sls_due_dt > 20500101 OR sls_due_dt < 19000101

-- ONLY REPRODUCIBLE IN BRONZE LAYER
-- Adding the whole logic that we built
-- Check if ship date is greater than order date (check for the negative)
-- And if due date is greater than order date
SELECT * FROM (
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details)t
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
-- No results

-- ONLY REPRODUCIBLE IN BRONZE LAYER
-- Repeat it with ship date vs order date
-- Check if due date is greater than ship date
SELECT * FROM (
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    CASE
        WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE
        WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details)t
WHERE sls_ship_dt > sls_due_dt
-- order_dt should be less than ship_dt
-- ship_dt should be less than due_dt
-- order_dt < ship_dt < due_dt

-- No results

-- Date consistency
-- REPRODUCIBLE IN SILVER LAYER
-- Note => After cleaning information the columns of order_dt, ship_dt, and due_dt
-- are not INT anymore, so I will have to change (or add) to another 2 checks:
SELECT * FROM (
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details)t
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
-- No results

SELECT * FROM (
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details)t
WHERE sls_order_dt IS NULL OR sls_ship_dt IS NULL OR sls_due_dt IS NULL
-- There are results of sls_order_dt to a NULL value
-- This is a result of replacing NULL by zero in bronze to silver transformation
-- So I will leave it as is because Baraa desgined that


-- Check for Sales column
-- Business rules:
-- Quantity * Price = Sales
-- Negative, Zeros, Nulls are not allowed!
SELECT
    sls_ord_num,
    sls_sales,
    sls_quantity,
    sls_price
FROM silver.crm_sales_details
WHERE sls_quantity * sls_price != sls_sales
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0
ORDER BY sls_sales, sls_quantity, sls_price
-- We have plenty of quality issues (in bronce layer)
-- No results for silver layer

-- ---------------------------------------
-- Intermediate query to solve the previous problem
-- THIS IS NOT A TEST but a custom query
-- ---------------------------------------
SELECT
    sls_ord_num,
    sls_sales,
    CASE
        WHEN sls_sales <= 0 OR sls_sales IS NULL THEN sls_quantity * sls_price
        ELSE sls_sales
    END AS sls_sales_fixed,
    sls_quantity,
    sls_price,
    CASE
        WHEN sls_price = 0 OR sls_price IS NULL AND sls_quantity != 0 THEN sls_sales / sls_quantity
        WHEN sls_price < 0 THEN -sls_price
        ELSE sls_price
    END AS sls_price_fixed
FROM bronze.crm_sales_details
WHERE sls_quantity * sls_price != sls_sales
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0
ORDER BY sls_sales, sls_quantity, sls_price

-- If Sales is negative, zero, or null, derive it using Quantity and Price
-- If Price is zero, or null, calculate it using Sales and Quantity
-- If Price is negative, convert it to a positive value

-- Baraa response is:
SELECT
    sls_ord_num,
    sls_sales AS old_sls_sales,
    CASE
        WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != ABS(sls_quantity)
            THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE
        WHEN sls_price = 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
        WHEN sls_price < 0 THEN -sls_price
        ELSE sls_price
    END AS sls_price_fixed
FROM bronze.crm_sales_details
WHERE sls_quantity * sls_price != sls_sales
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales < 0 OR sls_quantity < 0 OR sls_price < 0
ORDER BY sls_sales, sls_quantity, sls_price
-- If Sales is negative, zero, or null, derive it using Quantity and Price
-- If Price is zero, or null, calculate it using Sales and Quantity
-- If Price is negative, convert it to a positive value

-- ---------------------------------------
-- End of intermediate query
-- ---------------------------------------


-- ============================================
-- ============================================
-- Quality Checks for
-- Table: silver.erp_cust_az12
-- ============================================
-- ============================================


-- Data Standarisation
-- Removing first 3 characters 'NAS' of cid column

-- ONLY REPRODUCIBLE IN BRONZE LAYER
SELECT
    cid AS old_cid,
    CASE WHEN cid LIKE 'NAS%' THEN 
        SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
    END AS cid,
    bdate,
    gen
FROM bronze.erp_cust_az12
WHERE CASE WHEN cid LIKE 'NAS%' THEN 
        SUBSTRING(cid, 4, LEN(cid))
        ELSE cid
      END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)

SELECT DISTINCT
    bdate
FROM silver.erp_cust_az12
WHERE bdate < '1900-01-01' OR bdate > GETDATE()
-- There are results of birth date in the future (bronze layer)
-- Ask to an expert how to proceed
-- But Baraa replaces to NULL
-- No results for silver layer

-- Data Standarisation
SELECT DISTINCT
    gen
FROM silver.erp_cust_az12
-- Bronze layer
-- We have bad quality data because:
-- NULL, 'F', ' ', 'Male', 'Female', and 'M'

-- We are going to remove unwanted spaces and NULLs by replacing them to 'n/a'
-- F and Female => Female
-- M and Male => Male

-- Silver layer
-- 'Female', 'Male', and 'n/a' results => OK

-- ---------------------------------------
-- Intermediate query to solve the previous problem
-- THIS IS NOT A TEST but a custom query
-- ---------------------------------------
SELECT DISTINCT
    gen,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM bronze.erp_cust_az12

-- ---------------------------------------
-- End of intermediate query
-- ---------------------------------------


-- ============================================
-- ============================================
-- Quality Checks for
-- Table: silver.erp_loc_a101
-- ============================================
-- ============================================

-- Data Standarisation & Consistency
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101
ORDER BY cntry
-- It has US and United States as results
-- Also, a few more countries:
-- DE
-- USA
-- France
-- Germany
-- And so on.. so we will manually change each one.....

-- ---------------------------------------
-- Intermediate query to solve the previous problem
-- THIS IS NOT A TEST but a custom query
-- ---------------------------------------
SELECT DISTINCT
    cntry AS old_cntry,
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101
-- ---------------------------------------
-- End of intermediate query
-- ---------------------------------------

-- ============================================
-- ============================================
-- Quality Checks for
-- Table: silver.erp_px_cat_g1v2
-- ============================================
-- ============================================

SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2

-- Check for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
-- No results

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat)
-- No results

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance)
-- No results

-- Check low cardinality columns
SELECT DISTINCT
    cat
FROM bronze.erp_px_cat_g1v2
-- No errors

SELECT DISTINCT
    subcat
FROM bronze.erp_px_cat_g1v2
-- No errors

SELECT DISTINCT
    maintenance
FROM bronze.erp_px_cat_g1v2
-- No errors
