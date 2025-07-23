-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Bronze to Silver
-- MAGIC This notebook refines raw data from the Bronze layer by applying additional validations, transformations, and Change Data Capture (CDC) logic. The Silver layer contains enriched and validated data ready for analytics.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 3: Validating and Transforming Orders Data
-- MAGIC
-- MAGIC - Filter and enrich orders data with quality constraints and transformations.
-- MAGIC
-- MAGIC - **Validations:**
-- MAGIC   - **valid_date:** Ensures order_timestamp is later than January 1, 2021.
-- MAGIC - **Transformations:**
-- MAGIC     - Converts order_timestamp to a timestamp format.
-- MAGIC     - Excludes unnecessary fields like _rescued_data.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE order_table_silver
  (CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
COMMENT "Append only orders with valid timestamps"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  timestamp(order_timestamp) AS order_timestamp, 
  * EXCEPT (order_timestamp, _rescued_data)
FROM STREAM(LIVE.order_table_bronze)                    -- References the orders_bronze streaming table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 4: Processing Customers CDC Data with APPLY CHANGES INTO
-- MAGIC
-- MAGIC - Process customer CDC data into a Type 1 Slowly Changing Dimension (SCD) table.
-- MAGIC - **CDC Logic:**
-- MAGIC   - Applies inserts, updates, and deletes to maintain the latest state.
-- MAGIC   - Uses `customer_id` as the primary key.
-- MAGIC   - Orders operations by the `timestamp` field.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze_clean)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY timestamp
  COLUMNS * EXCEPT (operation, _rescued_data)
