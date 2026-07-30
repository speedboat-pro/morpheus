------------------------------------------------------
-- Customers Pipeline with Change Data Capture
------------------------------------------------------


------------------------------------------------------------------------------------------------------------
-- STEP 1: JSON -> Bronze Ingestion
------------------------------------------------------------------------------------------------------------
-- Ingestion the JSON files from cloud storage into a streaming table using Auto Loader
------------------------------------------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE sdp_1_bronze.customers_bronze_raw_demo12
  COMMENT "Raw data from customers CDC feed"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false -- prevent full table refreshes on the bronze table
  )
AS 
SELECT 
  *,
  current_timestamp() processing_time,  -- Obtain the ingestion processing time for the rows
  _metadata.file_name as source_file    -- Obtain the file name of the record
FROM STREAM read_files(
  "${source}/customers", 
  format => "json");



---------------------------------------------------------------------------------------
-- STEP 2: Create the Bronze Clean Streaming Table with Data Quality Enforcement
---------------------------------------------------------------------------------------
CREATE STREAMING TABLE sdp_1_bronze.customers_bronze_clean_demo12
  (
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = "DELETE"),
    CONSTRAINT valid_address EXPECT (
      (address IS NOT NULL and 
        city IS NOT NULL and 
        state IS NOT NULL and 
        zip_code IS NOT NULL) OR
       operation = "DELETE"),
    CONSTRAINT valid_email EXPECT (
      rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') OR 
            operation = "DELETE") ON VIOLATION DROP ROW
  )
  COMMENT "Clean raw bronze timestamp column and add data quality constraints"
AS 
SELECT 
  *,
  CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
FROM STREAM sdp_1_bronze.customers_bronze_raw_demo12;



---------------------------------------------------------------------------------------
-- STEP 3: Processing CDC Data with AUTO CDC INTO
---------------------------------------------------------------------------------------

-- Create the streaming target table if it's not already created
CREATE OR REFRESH STREAMING TABLE sdp_2_silver.scd_type_1_customers_silver_demo12
  COMMENT 'SCD Type 1 Historical Customer Data';

-- Perform SCD TYPE 1
CREATE FLOW scd_type_1_flow AS 
AUTO CDC INTO sdp_2_silver.scd_type_1_customers_silver_demo12  -- Target table to update with SCD Type 1 (or 2)
FROM STREAM sdp_1_bronze.customers_bronze_clean_demo12  -- Source records to determine updates, deletes and inserts
  KEYS (customer_id)                              -- Primary key for identifying records
  APPLY AS DELETE WHEN operation = "DELETE"       -- Handle deletes from source to the target
  SEQUENCE BY timestamp_datetime                  -- Defines order of operations for applying changes
  COLUMNS * EXCEPT (timestamp, _rescued_data, operation)     -- Select columns and exclude metadata fields
  STORED AS SCD TYPE 1;      -- Use Slowly Changing Dimension Type 1 to update the target table (no historical information)

CREATE OR REFRESH STREAMING TABLE sdp_2_silver.scd_type_2_customers_silver_demo12
  COMMENT 'SCD Type 1 Historical Customer Data';
  -- Perform SCD TYPE 2
CREATE FLOW scd_type_2_flow AS 
AUTO CDC INTO sdp_2_silver.scd_type_2_customers_silver_demo12  -- Target table to update with SCD Type 1 (or 2)
FROM STREAM sdp_1_bronze.customers_bronze_clean_demo12  -- Source records to determine updates, deletes and inserts
  KEYS (customer_id)                              -- Primary key for identifying records
  APPLY AS DELETE WHEN operation = "DELETE"       -- Handle deletes from source to the target
  SEQUENCE BY timestamp_datetime                  -- Defines order of operations for applying changes
  COLUMNS * EXCEPT (timestamp, _rescued_data, operation)     -- Select columns and exclude metadata fields
  STORED AS SCD TYPE 2;      -- Use Slowly Changing Dimension Type 1 to update the target table (no historical information)