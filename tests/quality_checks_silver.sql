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

