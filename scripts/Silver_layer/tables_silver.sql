/*
Script Purpose:
  The purpose of this script is to create the tables for the Silver layer, where clean data will be inserted.
  A metadata clause has also been added to the document to indicate when the data was inserted.
*/


CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),	
	cst_lastname VARCHAR(50),	
	cst_marital_status VARCHAR(50),	
	cst_gndr VARCHAR(50),	
	cst_create_date DATE,
	dwh_create_date TIMESTAMP DEFAULT clock_timestamp()
);
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	prd_key VARCHAR(50),	
	prd_nm VARCHAR(50),
	prd_cost INT,	
	prd_line VARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date TIMESTAMP DEFAULT clock_timestamp()
);
CREATE TABLE silver.crm_sales_details (
	sls_ord_num VARCHAR(50),
	sls_prd_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date TIMESTAMP DEFAULT clock_timestamp()
);


CREATE TABLE silver.erp_cust_az12 (
	CID VARCHAR(50),
	BDATE DATE,
	GEN VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT clock_timestamp()
);
CREATE TABLE silver.erp_loc_a101 (
	CID VARCHAR(50),	
	CNTRY VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT clock_timestamp()
);
CREATE TABLE silver.erp_px_cat_g1v2 (
	ID VARCHAR(50),	
	CAT	VARCHAR(50),
	SUBCAT	VARCHAR(50),
	MAINTENANCE VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT clock_timestamp()
);
