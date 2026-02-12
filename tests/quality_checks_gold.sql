-- ============================================
-- ============================================
-- Quality Checks for
-- View: gold.fact_sales
-- ============================================
-- ============================================

-- Foreign Key Integrity (Dimensions)
-- Expected: No results
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
WHERE c.customer_key IS NULL

SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE p.product_key IS NULL

-- I don't know if this is neccesary (ask to an IA)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL
-- My guess that it does a lot of effort in database