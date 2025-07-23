-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DLT Pipeline: Customer and Order Analytics
-- MAGIC
-- MAGIC This notebook defines a Delta Live Tables (DLT) pipeline for ingesting, validating, and enriching customer and order data using a medallion architecture. The pipeline demonstrates best practices in DLT syntax and data quality enforcement, culminating in a gold-layer materialized view for analytics and reporting.
-- MAGIC
-- MAGIC **Learning Objectives**
-- MAGIC
-- MAGIC By the end of this notebook, you will learn to:
-- MAGIC - Ingest raw data into bronze tables using Auto Loader.
-- MAGIC - Validate and clean data in the silver layer with constraints.
-- MAGIC - Apply changes to maintain a Type 1 Slowly Changing Dimension (SCD) table.
-- MAGIC - Join customer and order data into a materialized view for reporting.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer: Raw Data Ingestion
-- MAGIC
-- MAGIC The bronze layer captures raw data from the source systems, preserving its fidelity and metadata for downstream processing.
-- MAGIC

-- COMMAND ----------

-- BRONZE LAYER: Raw Data Ingestion
CREATE OR REFRESH STREAMING TABLE customer_bronze
COMMENT "Raw data from customers CDC feed"
AS 
SELECT 
  current_timestamp() AS processing_time, 
  *
FROM cloud_files("${Source}/customers", "json");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Order Bronze Table
-- MAGIC We ingest raw order data, capturing both the ingestion timestamp and metadata about the source files.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE order_bronze
COMMENT "Raw data from orders feed"
AS 
SELECT 
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM cloud_files("${Source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Bronze Layer: Data Cleaning and Validation
-- MAGIC **Customer Bronze Clean Table**
-- MAGIC
-- MAGIC This table enforces multiple constraints to ensure data quality:
-- MAGIC - **`valid_id`**: Fails if `customer_id` is null.
-- MAGIC - **`valid_operation`**: Drops rows with null `operation` values.
-- MAGIC - **`valid_name`**: Ensures non-null names unless the operation is `DELETE`.
-- MAGIC - **`valid_address`**: Validates address fields unless the operation is `DELETE`.
-- MAGIC - **`valid_email`**: Checks for valid email formats using regex, excluding `DELETE` operations.

-- COMMAND ----------

-- Bronze LAYER: Data Cleaning and Validation for Customers
CREATE STREAMING TABLE customer_bronze_clean
(
  CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_name EXPECT (name IS NOT NULL OR operation = "DELETE"),
  CONSTRAINT valid_address EXPECT (
    (address IS NOT NULL AND 
     city IS NOT NULL AND 
     state IS NOT NULL AND 
     zip_code IS NOT NULL) OR
     operation = "DELETE"),
  CONSTRAINT valid_email EXPECT (
    rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') OR 
    operation = "DELETE") ON VIOLATION DROP ROW
)
AS 
SELECT *
FROM STREAM(LIVE.customer_bronze);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Layer
-- MAGIC The silver layer validates and enriches the data for consistency and reliability.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Order Silver Table
-- MAGIC The `order_silver` table enforces a constraint to validate the `order_timestamp` and enriches the data with cleaned fields.

-- COMMAND ----------

-- SILVER LAYER: Data Cleaning and Validation for Orders
CREATE OR REFRESH STREAMING TABLE order_silver
(CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
COMMENT "Validated and enriched orders data"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  timestamp(order_timestamp) AS order_timestamp, 
  * EXCEPT (order_timestamp, _rescued_data)
FROM STREAM(LIVE.order_bronze);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer Silver Table
-- MAGIC The `customer_silver` table processes Change Data Capture (CDC) events from `customer_bronze_clean` using `APPLY CHANGES INTO`.

-- COMMAND ----------

-- SILVER LAYER: Enriched Customers Data
CREATE OR REFRESH STREAMING TABLE customer_silver;

APPLY CHANGES INTO LIVE.customer_silver
FROM STREAM(LIVE.customer_bronze_clean)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = "DELETE"
SEQUENCE BY timestamp
COLUMNS * EXCEPT (operation, _rescued_data);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer: Materialized View for Analytics
-- MAGIC
-- MAGIC The gold layer aggregates and combines the cleaned customer and order data for analytics and reporting.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Customer Order Materialized View
-- MAGIC This view joins the `order_silver` and `customer_silver` tables, providing a consolidated dataset for BI and reporting.

-- COMMAND ----------

-- GOLD LAYER: Materialized View for Customers and Orders
CREATE OR REFRESH MATERIALIZED VIEW customer_order
COMMENT "Join of customers and orders for analytics and reporting"
AS 
SELECT 
  o.order_id, 
  o.order_timestamp, 
  o.processing_time AS order_processing_time,
  c.customer_id, 
  c.name AS customer_name, 
  c.email AS customer_email, 
  c.state AS customer_state,
  o.notifications AS order_notifications 
FROM LIVE.order_silver o
INNER JOIN LIVE.customer_silver c
ON o.customer_id = c.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC This notebook is use for demonstrating:
-- MAGIC - Ingesting raw customer and order data into bronze tables using Auto Loader.
-- MAGIC - Validating and cleaning data in the silver layer using constraints and `APPLY CHANGES INTO`.
-- MAGIC - Creating a gold-layer materialized view for consolidated analytics.
-- MAGIC
-- MAGIC These techniques showcase the power of Delta Live Tables in building scalable, reliable data pipelines in the Databricks Lakehouse.
-- MAGIC
