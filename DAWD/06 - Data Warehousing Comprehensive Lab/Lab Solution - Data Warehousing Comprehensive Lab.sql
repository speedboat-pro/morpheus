-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Warehousing Comprehensive Lab
-- MAGIC
-- MAGIC This lab will guide you through creating a complete pipeline in Databricks, leveraging Delta Lake, data ingestion techniques, transformations, dashboards, and Databricks Genie. The goal is to give you hands-on experience with the Databricks platform.
-- MAGIC
-- MAGIC **Learning Objectives**
-- MAGIC
-- MAGIC By the end of this lab, you will:
-- MAGIC - Create Delta tables and explore Delta Lake features like Time Travel and Version History.
-- MAGIC - Perform data ingestion using techniques - Upload UI.
-- MAGIC - Clean and transform datasets into Bronze, Silver, and Gold layers.
-- MAGIC - Visualize insights using Databricks Dashboards.
-- MAGIC - Leverage Databricks Genie for data exploration and analysis.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC     - In the drop-down, select **More**.
-- MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
-- MAGIC     
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC 1. Wait a few minutes for the cluster to start.
-- MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Requirements
-- MAGIC
-- MAGIC Please review the following requirements before starting the lesson:
-- MAGIC
-- MAGIC - To run this notebook, you need to use one of the following Databricks runtime(s): `16.3.x-scala2.12`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Classroom Setup
-- MAGIC
-- MAGIC Before starting the lab, run the provided classroom setup script. This script will define configuration variables necessary for the lab.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-LAB

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Other Conventions:**
-- MAGIC
-- MAGIC Throughout this lab, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"Username:          {DA.username}")
-- MAGIC print(f"Catalog Name:      {DA.catalog_name}")
-- MAGIC print(f"Schema Name:       {DA.schema_name}")
-- MAGIC print(f"Working Directory: {DA.paths.working_dir}")
-- MAGIC print(f"Dataset Location:  {DA.paths.datasets}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Ensure you are using the correct catalog and schema for this lab:**

-- COMMAND ----------

USE CATALOG ${DA.catalog_name};
USE SCHEMA ${DA.schema_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 1 - Creating Delta Tables and Exploring Delta Lake Features
-- MAGIC In this task, you will learn how to create Delta tables and explore the advanced features of Delta Lake.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.1 Create the `sales_table` Delta Table
-- MAGIC Follow these steps to create a Delta table from a CSV file and explore its features:
-- MAGIC 1. Create the Delta table by reading data from the CSV file.
-- MAGIC 2. Verify the table creation by selecting a sample of the data.

-- COMMAND ----------

---- Drop the table if it already exists for demonstration purposes
DROP TABLE IF EXISTS sales_table;

---- Create a Delta table using the CSV file
CREATE TABLE sales_table USING DELTA
AS
SELECT *
FROM read_files(
  '${DA.paths.datasets.retail}/source_files/sales.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);

---- Select from the newly created table
SELECT * FROM sales_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.2 Enable Column Mapping and Modify the Table
-- MAGIC In this step, you will enhance the functionality and structure of the sales_table Delta table by enabling column mapping and modifying the schema. Column mapping is essential for managing schema evolution and ensuring data consistency in Delta Lake. Follow these steps:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - **Step 1: Enable Column Mapping:**
-- MAGIC
-- MAGIC   Set the table properties to enable column mapping. This feature allows you to rename columns, manage schema changes, and maintain backward compatibility for readers.
-- MAGIC
-- MAGIC - **Step 2: Drop Unnecessary Columns:**
-- MAGIC
-- MAGIC   Remove the `_rescued_data` column, which is often added to capture extra data during schema inference but may not be required for further analysis.

-- COMMAND ----------

---- Enable column mapping on the Delta table
ALTER TABLE sales_table SET TBLPROPERTIES (
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5',
   'delta.columnMapping.mode' = 'name'
);

---- Drop the column after enabling column mapping
ALTER TABLE sales_table DROP COLUMNS (_rescued_data);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - **Step 3: Add and Update a New Column:**
-- MAGIC
-- MAGIC   Add a new column named `discount_code` to the table schema and populate it with values based on conditions. In this step:
-- MAGIC
-- MAGIC     - Assign `Discount_20%` to rows where the `product_category` is `'Ramsung'`.
-- MAGIC     - Assign `N/A` to all other rows.

-- COMMAND ----------

---- Alter the table by adding a new column
ALTER TABLE sales_table ADD COLUMNS (discount_code STRING);

---- Update the newly added column with data
UPDATE sales_table
SET discount_code = CASE
  WHEN product_category = 'Ramsung' THEN 'Discount_20%'
  ELSE 'N/A'
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - **Step 4: View Table History:**
-- MAGIC   
-- MAGIC   Use the `DESCRIBE HISTORY` command to view the version history of the table.

-- COMMAND ----------

---- Display the history of changes made to the sales_table
DESCRIBE HISTORY sales_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.3 Restore the Table Using Time Travel
-- MAGIC
-- MAGIC Delta Lake's time travel feature allows you to access and restore previous versions of a Delta table. This is useful for scenarios such as data recovery, debugging, or auditing changes.
-- MAGIC
-- MAGIC In this sub task, you will restore the `sales_table` Delta table to a specific version using the `RESTORE TABLE` command.

-- COMMAND ----------

---- Restore the sales_table to previous version
RESTORE TABLE sales_table TO VERSION AS OF 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 2 -  Data Ingestion Techniques
-- MAGIC In this task, you will learn how to ingest data into Databricks using the UI. This includes downloading a dataset, uploading it to your schema, and creating a Delta table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 - Uploading Data and Creating a Delta Table using UI
-- MAGIC
-- MAGIC 1. Download the `customers.csv` data file by following [this link](/ajax-api/2.0/fs/files/Volumes/dbacademy_retail/v01/source_files/customers.csv). This will download the CSV file to your browser's download folder.
-- MAGIC 1. Using the the [Catalog Explorer](/explore/data/dbacademy) user interface, create a table named *customers_ui* in your schema, using the file you just downloaded.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - **Step 1: Verify the Table Creation**
-- MAGIC
-- MAGIC   After successfully creating the Delta table, you can verify its creation and view a sample of the data by following these steps:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use the `SHOW TABLES` command to display all tables in the current schema and confirm that `customers_ui` exists.

-- COMMAND ----------

---- Show all tables in the current Schema
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use the `SELECT` statement to retrieve and display the first 10 records from the `customers_ui` table to ensure the data has been ingested correctly.
-- MAGIC

-- COMMAND ----------

---- Display the first 10 records from the customers_ui table
SELECT * FROM customers_ui LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2 Create Table as Select (CTAS)
-- MAGIC
-- MAGIC In this step, we create the `customers_ui_bronze` Delta table by selecting data from `customers_ui` and applying transformations.

-- COMMAND ----------

---- Drop the customers_ui_bronze table if it already exists
DROP TABLE IF EXISTS customers_ui_bronze;
---- Create a new Delta table
CREATE TABLE customers_ui_bronze USING DELTA AS
SELECT *, 
  CAST(CAST(valid_from / 1e6 AS TIMESTAMP) AS DATE) AS first_touch_date, 
  CURRENT_TIMESTAMP() AS updated,
  _metadata.file_name AS source_file
FROM customers_ui;

---- Verify the data in the newly created table
SELECT * FROM customers_ui_bronze LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 3 - Data Transformation
-- MAGIC In this task, you will transform the data in your Delta tables to create the Silver and Gold tables. These transformations will clean, enrich, and join the data to provide valuable insights for analytics and reporting.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3.1 Create the Silver Table
-- MAGIC
-- MAGIC The Silver table represents a refined layer with cleaned and enriched data derived from the Bronze table. 
-- MAGIC
-- MAGIC Follow these steps:
-- MAGIC - Transform the `customers_ui_bronze` table to clean and enrich the data.
-- MAGIC - Create a new column, `loyalty_level`, that categorizes customers based on their loyalty segment.
-- MAGIC - Save the results as the `customers_ui_silver` table.

-- COMMAND ----------

---- Create or replace the Silver table
CREATE OR REPLACE TABLE customers_ui_silver AS
SELECT 
  customer_id,
  customer_name,
  state,
  city,
  units_purchased,
  loyalty_segment, ---- Selecting relevant columns from the Bronze table.
  CASE 
    WHEN loyalty_segment = 1 THEN 'High'
    WHEN loyalty_segment = 2 THEN 'Medium'
    ELSE 'Low'
  END AS loyalty_level  ---- Adding a new column, loyalty_level, based on the loyalty_segment values.
FROM customers_ui_bronze;

---- Verify the Silver table
SELECT * FROM customers_ui_silver LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3.2 Create the Gold Table
-- MAGIC
-- MAGIC The Gold table represents a business insights layer, created by joining the Silver table with the `sales_table`.
-- MAGIC
-- MAGIC Follow these steps:
-- MAGIC
-- MAGIC - Join the `customers_ui_silver` table with the `sales_table` on the `customer_id` column.
-- MAGIC - Select key metrics and dimensions required for analytics and save the result as the `customers_ui_gold` table.

-- COMMAND ----------

---- Create or replace the Gold table
CREATE OR REPLACE TABLE customers_ui_gold AS
SELECT 
  c.customer_id,
  c.customer_name,
  c.loyalty_level,
  s.product_category,
  s.product_name,
  s.total_price,
  s.order_date
FROM customers_ui_silver c
JOIN sales_table s ---- Joining the customers_ui_silver table with the sales_table on the customer_id column.
ON c.customer_id = s.customer_id; ---- Selecting key attributes from both tables to create a comprehensive insights layer.

-- Verify the Gold table
SELECT * FROM customers_ui_gold LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Task 4 - Visualization with Dashboards
-- MAGIC In this task, you will create a dashboard in Databricks to visualize insights derived from the Gold table. The task involves adding datasets, creating visualizations, and exploring the dashboard using Databricks Genie.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###4.1: Create a New Dashboard
-- MAGIC Follow these steps to create a new dashboard:
-- MAGIC * Navigate to **Dashboards** in the side navigation panel.
-- MAGIC * Select **Create dashboard**. 
-- MAGIC * At the top of the resulting screen, click on the Dashboard name and change it to **Customer_Sales Dashboard**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.2: Adding Data to the Dashboard
-- MAGIC
-- MAGIC To create visualizations, you need to associate datasets with the dashboard. Complete the following steps:
-- MAGIC
-- MAGIC 1. Navigate to the **Data** tab in the dashboard.
-- MAGIC 2. Use the **+ Select a table** button to add datasets. 
-- MAGIC 3. Search for and select the **`customers_ui_gold`** table from {DA.schema_name}.{DA.schema_name}and click **Confirm**. The table will appear in your dataset list.
-- MAGIC
-- MAGIC You can modify the SQL query associated with each dataset in the query editing panel to customize the data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.3: Visualization - Combo Chart
-- MAGIC Visualize the insights by creating a Combo Chart that displays total sales value and sales order counts over a three-month span.
-- MAGIC
-- MAGIC **Steps to Create the Combo Chart:**
-- MAGIC
-- MAGIC 1. In the **Data** tab, select the **+ Create from SQL** option.
-- MAGIC 2. Enter and execute the following SQL query (replace `{DA.catalog_name}.{DA.schema_name}` with your actual **catalog name** and **schema name**):
-- MAGIC
-- MAGIC     ```sql
-- MAGIC     SELECT customer_name, 
-- MAGIC            total_price AS Total_Sales, 
-- MAGIC            date_format(order_date, "MM") AS Month, 
-- MAGIC            product_category 
-- MAGIC     FROM {DA.catalog_name}.{DA.schema_name}.customers_ui_gold 
-- MAGIC     WHERE order_date >= to_date('2019-08-01')
-- MAGIC     AND order_date <= to_date('2019-10-31');
-- MAGIC     ```
-- MAGIC
-- MAGIC 3. Rename the query to **Three Month Sales** and save it.
-- MAGIC 4. Switch to the **Canvas** tab and click **Add a visualization** at the bottom.
-- MAGIC 5. Select the **Three Month Sales** dataset and choose the **Combo** chart as the visualization type.
-- MAGIC 6. Configure the chart settings:
-- MAGIC     - **X axis:** Month
-- MAGIC     - **Bar:** Total_Sales (Rename to **Total Sales Value**)
-- MAGIC     - **Line:** COUNT(`*`) (Rename to **Count of Sales Orders**)
-- MAGIC
-- MAGIC 7. Enable **dual axis** from the Y-axis configuration menu.
-- MAGIC 8. Change the left Y-axis format to **Currency ($)**.
-- MAGIC
-- MAGIC This visualization will show the correlation between sales volume and total sales value for each month.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4.4: Creating a Genie Space from a Dashboard
-- MAGIC
-- MAGIC Databricks Genie allows you to explore data directly from the dashboard in a conversational interface.
-- MAGIC
-- MAGIC **Steps to Create a Genie Space:**
-- MAGIC
-- MAGIC 1. Open the **Retail Dashboard** you created.
-- MAGIC 2. Switch to the **Draft** view.
-- MAGIC 3. Click the kebab menu (three vertical dots) in the upper-right corner and select **Open Draft Genie space**.
-- MAGIC 4. In the chatbox, ask:
-- MAGIC
-- MAGIC     `
-- MAGIC     What tables are there and how are they connected? Give me a short summary.
-- MAGIC     `
-- MAGIC
-- MAGIC 5. Review the response provided by Genie to understand the data relationships and structure.
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC By completing this task, you have successfully created a visual dashboard to analyze business insights and leveraged Genie for exploratory analysis.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC Congratulations on completing the **Data Warehousing Comprehensive Lab**! Throughout this lab, you gained hands-on experience with Databricks to build and analyze a complete data pipeline, leveraging the robust features of Delta Lake, Databricks Dashboards, and Databricks Genie.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
