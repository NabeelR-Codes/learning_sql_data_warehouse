/*
================================================================
STORED PROCEDURE: Loading Bronze Layer (Source -> Bronze)
================================================================
Script Purpose:
  This stored procedure loads data into the 'Bronze' schema from external CSV files. 
  It performs the following instructions:
    -  Truncates the bronze tables before loading data.
    -  Using bulk insert to load data.

Parameters: NONE
   The stored procedure does not need any parameters to run. 

Using Example:
  CALL bronze.load_bronze()
================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
	DECLARE
		start_time TIMESTAMP;
		end_time TIMESTAMP;
		load_seconds NUMERIC; 
	BEGIN
		-- TRUNCATING TABLE
				TRUNCATE TABLE 
					bronze.crm_cust_info,
					bronze.crm_prd_info,
					bronze.crm_sales_details,
					bronze.erp_cust_az12,
					bronze.erp_loc_a101,
					bronze.erp_px_cat_g1v2;
		
				RAISE NOTICE '=======================================';
				RAISE NOTICE 'TABLES TRUNCATED';
				RAISE NOTICE '=======================================';

			
		-- LOADING CRM DATA
			start_time := clock_timestamp();
			copy bronze.crm_cust_info
			FROM 'D:\Data D\Data_Engineer\SQL Project\3_sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
			DELIMITER ','
			CSV HEADER; 
			
			
		
			copy bronze.crm_prd_info 
			FROM 'D:\Data D\Data_Engineer\SQL Project\3_sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
			DELIMITER ','
			CSV HEADER;
			
			copy bronze.crm_sales_details
			FROM 'D:\Data D\Data_Engineer\SQL Project\3_sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
			DELIMITER ','
			CSV HEADER;
			end_time := clock_timestamp();
			load_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
	
			RAISE NOTICE '=======================================';
			RAISE NOTICE 'CRM DATA LOADED';
			RAISE NOTICE '=======================================';
			RAISE NOTICE '>>> CRM TABLE LOAD TIME: % seconds', load_seconds;
			RAISE NOTICE '=======================================';
			--- CRM DATA LOADED

			--- LOADING ERP DATA
			start_time := clock_timestamp();
			copy bronze.erp_cust_az12
			FROM 'D:\Data D\Data_Engineer\SQL Project\3_sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
			DELIMITER ','
			CSV HEADER;
			
			copy bronze.erp_loc_a101
			FROM 'D:\Data D\Data_Engineer\SQL Project\3_sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
			DELIMITER ','
			CSV HEADER; 
			
			copy bronze.erp_px_cat_g1v2
			FROM 'D:\Data D\Data_Engineer\SQL Project\3_sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
			DELIMITER ','
			CSV HEADER; 
			end_time := clock_timestamp();
			load_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
			
			RAISE NOTICE '=======================================';
			RAISE NOTICE 'ERP DATA LOADED';
			RAISE NOTICE '=======================================';
			RAISE NOTICE '>>> ERP TABLE LOAD TIME: % seconds ', load_seconds;
			RAISE NOTICE '=======================================';
			
			
		EXCEPTION
			WHEN OTHERS THEN
        		RAISE NOTICE '=======================================';
        		RAISE NOTICE 'ERROR OCCURRED DURING BRONZE LOAD';
        		RAISE NOTICE 'Error Message: %', SQLERRM;
        		RAISE NOTICE 'Error Code (SQLSTATE): %', SQLSTATE;
        		RAISE NOTICE '=======================================';
	END;
	$$;

