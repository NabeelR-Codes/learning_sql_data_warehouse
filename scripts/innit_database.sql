/*
==============================================
CREATE DATABASE AND SCHEMA
==============================================
Script Purpose:
The purpose of this script is to create a new database called "DataWarehouse" after checking if it already exists.
Additionally, the script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
  Running this script will drop the entire 'DataWarehouse' database.
*/

-- Drop database if it already exists
DROP DATABASE IF EXISTS datawarehouse;

-- Create the database
CREATE DATABASE datawarehouse;

-- Connect to the database before running the rest

-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
