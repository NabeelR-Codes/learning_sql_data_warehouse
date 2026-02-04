/*
================================================================================================
Stored Procedure: Bronze Layer -----> Silver Layer
================================================================================================
Script Purpose: 
  This procedure performs an ETL (Extract, Transform, Load) process, and loads the final clean
  data to the 'Silver' layer. 
Actions:
  - Truncates Table
  - Loads clean and transformed data.
================================================================================================
Parameters
  This procedure does not require any parameters to run. 
USAGE:
  CALL silver.load_silver()
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
	DECLARE
		start_time TIMESTAMP;
		end_time TIMESTAMP;
		load_seconds NUMERIC;
	BEGIN
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Truncating Table: silver.crm_cust_info';
			RAISE NOTICE '===========================================';
			TRUNCATE TABLE silver.crm_cust_info;
	
			start_time := clock_timestamp();
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Loading Data Into: silver.crm_cust_info';
			RAISE NOTICE '===========================================';
			INSERT INTO silver.crm_cust_info (cst_id, 
				cst_key, 
				cst_firstname, 
				cst_lastname, 
				cst_marital_status, 
				cst_gndr, 
				cst_create_date)
			
			-- We first cleaned the data. 
			SELECT 
				cst_id,
				cst_key,
				TRIM(cst_firstname), -- Removing spaces
				TRIM(cst_lastname), -- Removing spaces
				
				CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' 
					WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
					ELSE 'N/A'
				END cst_marital_status, -- Data normalization
				
				CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
					WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
					ELSE 'N/A'
				END cst_gndr, -- Data normalization
				
				cst_create_date
			FROM(
				SELECT 
					*,
					ROW_NUMBER() OVER (Partition by cst_id ORDER BY cst_create_date DESC) AS flag_last
				FROM bronze.crm_cust_info
			) t WHERE t.flag_last = 1; -- Cleaning cst_id data, to remove duplicates and nulls
			end_time := clock_timestamp();
			load_seconds := EXTRACT(EPOCH FROM (end_time-start_time)); 
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Data Loaded Into: silver.crm_prd_info';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Load time for crm_cust_info: %seconds', load_seconds;
			RAISE NOTICE '===========================================';
			
	-----------------------------------------------------------------------------------
			-- SILVER.crm_prd_info
			RAISE NOTICE '';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Truncating Table: silver.crm_prd_info';
			RAISE NOTICE '===========================================';
			TRUNCATE TABLE silver.crm_prd_info;
	
			start_time := clock_timestamp();
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Loading Data Into: silver.crm_prd_info';
			RAISE NOTICE '===========================================';
			INSERT INTO silver.crm_prd_info(
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
				REPLACE(SUBSTRING(prd_key,1,5), '-', '_') cat_id,
				SUBSTRING(prd_key,7,LENGTH(prd_key)) prd_key,
				prd_nm,
				COALESCE(prd_cost, 0) prd_cost, 
				
				CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
					WHEN UPPER(TRIM(prd_line))= 'R' THEN 'Road'
					WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
					ELSE 'N/A'
				END prd_line,
				
				prd_start_dt,
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 prd_end_dt
			FROM bronze.crm_prd_info;
			end_time := clock_timestamp();
			load_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Data Loaded Into: silver.crm_prd_info';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Load time for crm_prd_info: %seconds', load_seconds;
			RAISE NOTICE '===========================================';
			
	------------------------------------------------------------------------------------
			-- SILVER.crm_sales_details
			RAISE NOTICE '';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Truncating Table: silver.crm_sales_details';
			RAISE NOTICE '===========================================';
			TRUNCATE TABLE silver.crm_sales_details;
	
			start_time := clock_timestamp();
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Loading Data Into: silver.crm_sales_details';
			RAISE NOTICE '===========================================';
			INSERT INTO silver.crm_sales_details(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
				END sls_order_dt, -- Changing integers into dates
				
				CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
				END sls_ship_dt, -- changing integers into dates
				
				CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
				END sls_due_dt, --changing integers into dates
				
				CASE WHEN sls_sales IS NULL or sls_sales<=0 or sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
					ELSE sls_sales
				END sls_sales, -- Dealing with nulls, and inconsistent data
				
				sls_quantity,
				
				CASE WHEN sls_price IS NULL or sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
					ELSE sls_price
				END sls_price -- Dealing with nulls, and inconsistent data
				
			FROM bronze.crm_sales_details;
			end_time := clock_timestamp();
			load_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Data Loaded Into: silver.crm_sales_details';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Load time for crm_sales_details: %seconds', load_seconds;
			RAISE NOTICE '===========================================';
			
	-----------------------------------------------------------------------------------
			-- SILVER.erp_cust_az12
			RAISE NOTICE '';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Truncating Table: silver.erp_cust_az12';
			RAISE NOTICE '===========================================';
			TRUNCATE TABLE silver.erp_cust_az12;
	
			start_time := clock_timestamp();
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Loading Data Into: silver.erp_cust_az12';
			RAISE NOTICE '===========================================';
			-- Inserting data into silver layer
			INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
			
			
			SELECT 
			CASE WHEN cid ILIKE 'NAS%' THEN SUBSTRING(cid,4, LENGTH(cid))
				ELSE cid
			END cid, --Extracting cust_id
			
			CASE WHEN bdate > NOW() THEN NULL 
				else bdate
			END bdate, -- Fixing unusual dates
			
			CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE 'n/a'
			END gen -- Normalising data, and dealing with nulls 
			
			FROM bronze.erp_cust_az12;
			end_time := clock_timestamp();
			load_seconds := EXTRACT(EPOCH FROM (end_time -  start_time));
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Data Loaded Into: silver.erp_cust_az12';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Load time for erp_cust_az12: %seconds', load_seconds;
			RAISE NOTICE '===========================================';
			
	----------------------------------------------------------------------------------------
			-- SILVER.erp_loc_a101
			RAISE NOTICE '';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Truncating Table: silver.erp_loc_a101';
			RAISE NOTICE '===========================================';
			TRUNCATE TABLE silver.erp_loc_a101;
	
			start_time := clock_timestamp();
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Loading Data Into: silver.erp_loc_a101';
			RAISE NOTICE '===========================================';
			-- Inserting data into silver layer
			INSERT INTO silver.erp_loc_a101 (cid, cntry)
			
			--Cleaning Data
			SELECT 
			REPLACE(cid, '-', '') cid, --Removing '-' from cid
			
			CASE WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
				WHEN TRIM(cntry) IS NULL or TRIM(cntry) = ' ' or TRIM(cntry) = '' THEN 'N/A'
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				ELSE cntry
			END cntry -- normalising data, and dealing with NULLS
			
			FROM bronze.erp_loc_a101;
			end_time := clock_timestamp();
			load_seconds := EXTRACT(EPOCH FROM (end_time - start_time ));
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Data Loaded Into: silver.erp_loc_a101';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Load time for erp_loc_a101: %seconds', load_seconds;
			RAISE NOTICE '===========================================';
			
	-------------------------------------------------------------------------------------------
			-- SILVER.erp_px_cat_g1v2
			RAISE NOTICE '';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Truncating Table: silver.erp_px_cat_g1v2';
			RAISE NOTICE '===========================================';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
	
			start_time := clock_timestamp();
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Loading Data Into: silver.erp_px_cat_g1v2';
			RAISE NOTICE '===========================================';
			INSERT INTO silver.erp_px_cat_g1v2 
			
			SELECT 
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2;
			end_time := clock_timestamp();
			load_seconds := EXTRACT(EPOCH FROM (end_time - start_time ));
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Data Loaded Into: silver.erp_px_cat_g1v2';
			RAISE NOTICE '===========================================';
			RAISE NOTICE 'Load time for erp_px_cat_g1v2: %seconds', load_seconds;
			RAISE NOTICE '===========================================';
		EXCEPTION
			WHEN OTHERS THEN 
				RAISE NOTICE '=======================================';
        		RAISE NOTICE 'ERROR OCCURRED DURING BRONZE LOAD';
        		RAISE NOTICE 'Error Message: %', SQLERRM;
        		RAISE NOTICE 'Error Code (SQLSTATE): %', SQLSTATE;
        		RAISE NOTICE '=======================================';
	END;
$$;

