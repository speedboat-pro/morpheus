-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver to Gold
-- MAGIC This notebook aggregates and joins data from the Silver layer to produce analytics-ready datasets in the Gold layer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 5: Aggregating Orders Data
-- MAGIC Generate daily order summaries for reporting.
-- MAGIC - Provides a daily summary of orders for dashboards and business insights.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW order_table_gold      -- PREVIOUS SYNTAX: CREATE OR REFRESH LIVE TABLE...
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM LIVE.order_table_silver                                 -- References the full orders_silver streaming table
GROUP BY date(order_timestamp)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Step 6: Aggregating Customer Counts by State
-- MAGIC Count the number of active customers per state.
-- MAGIC - Enables geographic analysis of customer distribution.

-- COMMAND ----------

CREATE MATERIALIZED VIEW customer_counts_state           -- PREVIOUS SYNTAX: CREATE OR REFRESH LIVE TABLE...
COMMENT "Total active customers per state"
AS 
SELECT 
  state, 
  count(*) as customer_count, 
  current_timestamp() updated_at
FROM LIVE.customers_silver
GROUP BY state

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Step 7: Joining Orders and Customers
-- MAGIC
-- MAGIC Create a live view of subscribed customer order emails.
-- MAGIC - Provides a real-time view of subscribed customers and their orders for email marketing and notifications.

-- COMMAND ----------

CREATE LIVE VIEW subscribed_order_emails_v         
AS 
SELECT 
  a.customer_id, 
  a.order_id, 
  b.email 
FROM LIVE.order_table_silver a
INNER JOIN LIVE.customers_silver b
ON a.customer_id = b.customer_id
WHERE notifications = 'Y'
