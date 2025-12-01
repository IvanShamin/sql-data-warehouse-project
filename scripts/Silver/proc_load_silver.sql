USE DataWarehouse;
GO
-- ================================================================
-- FINAL SUBMISSION: silver.load_silver
-- Task: Data with Baraa – SQL Project 8 – Silver Layer
-- Output format: IDENTICAL to your legendary Bronze style
-- Author: Ivan Shamin (your wingman forever)
-- Date: November 22, 2025
-- THIS IS THE ONE THAT GETS YOU HIRED
-- ================================================================

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @OverallStart DATETIME2(7) = SYSUTCDATETIME();
    DECLARE @StepStart    DATETIME2(7);
    DECLARE @StepEnd      DATETIME2(7);
    DECLARE @Rows         INT;

    PRINT '=============================================================================================================';
    PRINT 'Loading Silver Layer';
    PRINT '=============================================================================================================';
    PRINT '';

    BEGIN TRY

        -- ================================================================
        -- STEP 1: silver.crm_cust_info
        -- ================================================================
        SET @StepStart = SYSUTCDATETIME();
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Into: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
        SELECT
            src.cst_id,
            src.cst_key,
            TRIM(src.cst_firstname),
            TRIM(src.cst_lastname),
            CASE WHEN UPPER(TRIM(src.cst_marital_status)) = 'S' THEN 'Single'
                 WHEN UPPER(TRIM(src.cst_marital_status)) = 'M' THEN 'Married'
                 ELSE 'n/a' END,
            CASE WHEN UPPER(TRIM(src.cst_gndr)) = 'F' THEN 'Female'
                 WHEN UPPER(TRIM(src.cst_gndr)) = 'M' THEN 'Male'
                 ELSE 'n/a' END,
            src.cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
        ) src
        WHERE src.flag_last = 1 AND src.cst_id IS NOT NULL;

        SET @Rows = @@ROWCOUNT;
        SET @StepEnd = SYSUTCDATETIME();
        PRINT '(' + CAST(@Rows AS VARCHAR(20)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @StepStart, @StepEnd)/1000.0 AS VARCHAR(20)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------------------';

        -- ================================================================
        -- STEP 2: silver.crm_prd_info
        -- ================================================================
        SET @StepStart = SYSUTCDATETIME();
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key,1,5), '-', '_'),
            SUBSTRING(prd_key,7,LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost,0),
            CASE UPPER(TRIM(prd_line)) WHEN 'M' THEN 'Mountain' WHEN 'R' THEN 'Road' WHEN 'S' THEN 'Other Sales' WHEN 'T' THEN 'Touring' ELSE 'n/a' END,
            CAST(prd_start_dt AS DATE),
            DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY SUBSTRING(prd_key,7,LEN(prd_key)) ORDER BY prd_start_dt))
        FROM bronze.crm_prd_info;

        SET @Rows = @@ROWCOUNT;
        SET @StepEnd = SYSUTCDATETIME();
        PRINT '(' + CAST(@Rows AS VARCHAR(20)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @StepStart, @StepEnd)/1000.0 AS VARCHAR(20)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------------------';

        -- ================================================================
        -- STEP 3: silver.crm_sales_details
        -- ================================================================
        SET @StepStart = SYSUTCDATETIME();
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Into: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
        SELECT
            sls_ord_num, sls_prd_key, sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(CAST(sls_order_dt AS VARCHAR(8)))<>8 THEN NULL ELSE CONVERT(date,CAST(sls_order_dt AS varchar(8)),112) END,
            CASE WHEN sls_ship_dt = 0 OR LEN(CAST(sls_ship_dt AS VARCHAR(8)))<>8 THEN NULL ELSE CONVERT(date,CAST(sls_ship_dt AS varchar(8)),112) END,
            CASE WHEN sls_due_dt = 0 OR LEN(CAST(sls_due_dt AS VARCHAR(8)))<>8 THEN NULL ELSE CONVERT(date,CAST(sls_due_dt AS varchar(8)),112) END,
            CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales <> sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price) ELSE sls_sales END,
            sls_quantity,
            CASE WHEN sls_price IS NULL OR sls_price <=0
                 THEN sls_sales / NULLIF(sls_quantity,0) ELSE ABS(sls_price) END
        FROM bronze.crm_sales_details;

        SET @Rows = @@ROWCOUNT;
        SET @StepEnd = SYSUTCDATETIME();
        PRINT '(' + CAST(@Rows AS VARCHAR(20)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @StepStart, @StepEnd)/1000.0 AS VARCHAR(20)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------------------';

        -- ================================================================
        -- STEP 4: silver.erp_cust_az12
        -- ================================================================
        SET @StepStart = SYSUTCDATETIME();
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Into: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid)) ELSE cid END AS cid,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END AS bdate,
        CASE 
            WHEN REPLACE(REPLACE(UPPER(gen), CHAR(13), ''), CHAR(10), '') 
                 LIKE '%FEMAL%' THEN 'Female'
            WHEN REPLACE(REPLACE(UPPER(gen), CHAR(13), ''), CHAR(10), '') 
                 LIKE '%MAL%'   THEN 'Male'
            ELSE 'n/a'
        END AS gen
        FROM bronze.erp_cust_az12;

        SET @Rows = @@ROWCOUNT;
        SET @StepEnd = SYSUTCDATETIME();
        PRINT '(' + CAST(@Rows AS VARCHAR(20)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @StepStart, @StepEnd)/1000.0 AS VARCHAR(20)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------------------';


        -- ================================================================
        -- STEP 5: silver.erp_loc_a101 – FINAL VERSION THAT WORKS 100% (including Australia!)
        -- ================================================================
        TRUNCATE TABLE silver.erp_loc_a101;
        
        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', '') AS cid,
        
            CASE 
                WHEN NULLIF(LTRIM(RTRIM(cntry)), '') IS NULL                    THEN 'n/a'
        
                WHEN UPPER(LTRIM(RTRIM(cntry))) IN ('US','USA') 
                  OR UPPER(LTRIM(RTRIM(cntry))) LIKE '%UNITED STATES%'         THEN 'United States'
        
                WHEN UPPER(LTRIM(RTRIM(cntry))) = 'DE' 
                  OR UPPER(LTRIM(RTRIM(cntry))) LIKE '%GERMANY%'              THEN 'Germany'
        
                WHEN UPPER(LTRIM(RTRIM(cntry))) IN ('GB','UK') 
                  OR UPPER(LTRIM(RTRIM(cntry))) LIKE '%KINGDOM%'              THEN 'United Kingdom'
        
                WHEN UPPER(LTRIM(RTRIM(cntry))) IN ('CA') 
                  OR UPPER(LTRIM(RTRIM(cntry))) LIKE '%CANADA%'               THEN 'Canada'
        
                -- TADY JE TEN KLÍČ – CHYTÍ I 'AU' I 'Australia'!
                WHEN UPPER(LTRIM(RTRIM(cntry))) IN ('AU','AUSTRALIA') 
                  OR UPPER(LTRIM(RTRIM(cntry))) LIKE '%AUSTRALIA%'            THEN 'Australia'
        
                WHEN UPPER(LTRIM(RTRIM(cntry))) IN ('FR') 
                  OR UPPER(LTRIM(RTRIM(cntry))) LIKE '%FRANCE%'               THEN 'France'
        
                ELSE 'Other'  -- už se sem nikdy nedostane
            END AS cntry
        FROM bronze.erp_loc_a101;
        
        SET @Rows = @@ROWCOUNT;
        SET @StepEnd = SYSUTCDATETIME();
        PRINT '(' + CAST(@Rows AS VARCHAR(20)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @StepStart, @StepEnd)/1000.0 AS VARCHAR(20)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------------------';
        -- ================================================================
        -- STEP 6: silver.erp_px_cat_g1v2 – the legendary Yes/No fix
        -- ================================================================
        SET @StepStart = SYSUTCDATETIME();
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT
            id, cat, subcat,
            TRIM(REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), '')) AS maintenance
        FROM bronze.erp_px_cat_g1v2;

        SET @Rows = @@ROWCOUNT;
        SET @StepEnd = SYSUTCDATETIME();
        PRINT '(' + CAST(@Rows AS VARCHAR(20)) + ' rows affected)';
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(MILLISECOND, @StepStart, @StepEnd)/1000.0 AS VARCHAR(20)) + ' seconds';
        PRINT '-------------------------------------------------------------------------------------------------------------';

        -- ================================================================
        -- FINAL SUMMARY
        -- ================================================================
        DECLARE @OverallEnd DATETIME2(7) = SYSUTCDATETIME();
        DECLARE @TotalSeconds INT = DATEDIFF(SECOND, @OverallStart, @OverallEnd);

        PRINT '=============================================================================================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '  Total Load Duration: ' + CAST(@TotalSeconds AS VARCHAR(10)) + ' seconds';
        PRINT '=============================================================================================================';
        PRINT 'Completion time: ' + CONVERT(VARCHAR(30), @OverallEnd, 127);

    END TRY
    BEGIN CATCH
        PRINT '=============================================================================================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=============================================================================================================';
        THROW;
    END CATCH
END;
GO

-- Run with:
EXEC silver.load_silver;
