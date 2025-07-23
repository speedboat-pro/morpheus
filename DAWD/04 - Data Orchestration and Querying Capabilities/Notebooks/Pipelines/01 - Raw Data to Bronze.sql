-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Raw Data to Bronze
-- MAGIC This notebook demonstrates the how to ingests raw data into the Bronze layer using Databricks' Auto Loader functionality. The Bronze layer is designed to hold raw, unvalidated data with minimal transformations, ensuring that all incoming records are preserved for further processing in subsequent layers.
-- MAGIC
-- MAGIC **Learning Objectives**
-- MAGIC
-- MAGIC By the end of this notebook, you will:
-- MAGIC - Understand the process of streaming data ingestion with Auto Loader.
-- MAGIC - Learn how to apply data quality constraints to ensure data reliability.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 1: Raw Data Ingestion with Auto Loader
-- MAGIC
-- MAGIC - Ingest raw data from JSON files into the Bronze layer for both customers and orders datasets.
-- MAGIC
-- MAGIC   - Ingest the raw customer Change Data Capture (CDC) feed into the customers_bronze table.
-- MAGIC   - **Auto Loader:** The   method detects new files in the source directory and incrementally ingests them.
-- MAGIC   - **Additional Fields:**
-- MAGIC     - **processing_time:** Timestamp for when the record was processed.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_bronze
  COMMENT "Raw data from customers CDC feed"
AS 
SELECT 
  current_timestamp() processing_time, 
  *
FROM cloud_files("${source}/customers", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Orders Data Ingestion:
-- MAGIC - Load raw orders data with inferred column types.
-- MAGIC - **Additional Fields:**
-- MAGIC   - **processing_time:** Captures the ingestion timestamp.
-- MAGIC   - **source_file:** Records the file name for traceability.
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE order_table_bronze
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM cloud_files("${source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 2: Data Quality Enforcement
-- MAGIC
-- MAGIC - Apply constraints to the Bronze layer to ensure data reliability.
-- MAGIC
-- MAGIC - **Constraints:**
-- MAGIC   - Ensure only clean, validated data progresses to the next layer.
-- MAGIC     - **valid_id:** Ensures customer_id is not null; fails the transaction otherwise.
-- MAGIC     - **valid_operation:** Drops rows with null operations.
-- MAGIC     - **valid_email:** Validates email format using regex.
-- MAGIC

-- COMMAND ----------

CREATE STREAMING TABLE customers_bronze_clean
  (
    CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_name EXPECT (name IS NOT NULL or operation = "DELETE"),
    CONSTRAINT valid_address EXPECT (
      (address IS NOT NULL and 
       city IS NOT NULL and 
       state IS NOT NULL and 
       zip_code IS NOT NULL) or
       operation = "DELETE"),
    CONSTRAINT valid_email EXPECT (
      rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
      operation = "DELETE") ON VIOLATION DROP ROW
  )
AS 
SELECT *
FROM STREAM(LIVE.customers_bronze)
