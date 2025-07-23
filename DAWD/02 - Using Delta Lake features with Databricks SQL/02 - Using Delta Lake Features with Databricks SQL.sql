-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demo - Using Delta Lake Features with Databricks SQL
-- MAGIC In this demo, we’ll explore the powerful features of Delta Lake and demonstrate how they enhance data management in a data warehousing context. We’ll start by creating and exploring Delta tables, then dive into key features like Time Travel and Version History.
-- MAGIC
-- MAGIC **Learning Objectives**
-- MAGIC
-- MAGIC By the end of this demo, you will be able to:
-- MAGIC - Explore key features of Delta Lake such as **Time Travel**, **Version History**, and metadata management.
-- MAGIC - Use SQL commands like `DESCRIBE EXTENDED`, `DESCRIBE HISTORY`, `VERSION AS OF`, and `RESTORE TABLE`.

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
-- MAGIC Before starting the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup-2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Other Conventions:**
-- MAGIC
-- MAGIC Throughout this demo, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"Username:          {DA.username}")
-- MAGIC print(f"Catalog Name:      {DA.catalog_name}")
-- MAGIC print(f"Schema Name:       {DA.schema_name}")
-- MAGIC print(f"Working Directory: {DA.paths.working_dir}")
-- MAGIC print(f"Dataset Location:  {DA.paths.datasets.retail}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Introduction
-- MAGIC Delta Lake is an open-source storage layer that brings reliability, performance, and ACID transactions to data lakes. In this demo, we will:
-- MAGIC - Create and explore a Delta table.
-- MAGIC - Simulate data changes and view metadata and history.
-- MAGIC - Use Time Travel to query and restore data to previous states.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ## Setup
-- MAGIC
-- MAGIC **Step 1: Select a default catalog and schema**
-- MAGIC - Run the following commands to select a default catalog and schema, which makes referencing table names easier.

-- COMMAND ----------

USE CATALOG ${DA.catalog_name};
USE SCHEMA ${DA.schema_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Step 2: Create the `retail_sales` table** and view the data in the table
-- MAGIC - We will use a pre-existing dataset file located at `DA.paths.datasets.retail/source_files/sales.csv`.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"{DA.paths.datasets.retail}/source_files/sales.csv")

-- COMMAND ----------

SELECT * FROM 
csv.`/Volumes/dbacademy_retail/v01/source_files/sales.csv`

-- COMMAND ----------

-- Create a Delta table from the CSV file
DROP TABLE IF EXISTS retail_sales;
CREATE TABLE IF NOT EXISTS retail_sales
--USING DELTA
AS
SELECT *
FROM read_files(
  '${DA.paths.datasets.retail}/source_files/sales.csv',
  format => 'csv',
  header => true,
  inferSchema => true
);
-- Display Data of the retail_sales table
SELECT * FROM retail_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part 1: Explore the Table
-- MAGIC
-- MAGIC Delta Lake tables store metadata that can be queried for insights about the table structure, state, and history.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1: View Extended Metadata
-- MAGIC Use the following command to view detailed metadata about the `retail_sales` table:

-- COMMAND ----------

DESCRIBE retail_sales

-- COMMAND ----------

DESCRIBE DETAIL retail_sales

-- COMMAND ----------

DESCRIBE EXTENDED retail_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 2: Describe the History of the Table
-- MAGIC Run the following command to view the history of `retail_sales`:

-- COMMAND ----------

DESCRIBE HISTORY retail_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC As the `retail_sales` table has not been modified yet, its version history will only show **Version 0**, which represents the initial creation of the table. Further modifications to the table will create additional versions in the history log.
-- MAGIC
-- MAGIC The history includes detailed metadata about the table's state and modifications:
-- MAGIC - **Version**: Indicates the specific version of the table.
-- MAGIC - **Timestamp**: Specifies when the operation occurred.
-- MAGIC - **Operation**: Describes the type of modification (e.g., `INSERT`, `UPDATE`, `DELETE`).
-- MAGIC - **Operation Metrics**: Includes key metrics like the number of rows added, removed, or affected, and the total data size.
-- MAGIC - **User Information**: Tracks which user performed the operation.
-- MAGIC - **Cluster Information**: Logs the cluster ID where the operation was executed.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part 2: Simulating Data Changes and Time Travel Queries
-- MAGIC
-- MAGIC Delta Lake allows for simulating data modifications and querying historical versions of the data using Time Travel.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Simulate Data Changes
-- MAGIC We will simulate updates and deletions to demonstrate how Delta Lake tracks changes in the table's version history.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Task 1: Update the Table
-- MAGIC Use the following command to update the `retail_sales` table, updating the `product_name` column for all rows where the `product_category` is `Rony`.

-- COMMAND ----------

UPDATE retail_sales
   SET product_name = 'Updated Items'
   WHERE product_category = 'Rony';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Before proceeding, let's obtain a timestamp representing this particular moment in time, which will be useful in a subsequent task. After executing, copy the resulting value to the clipboard for later use.

-- COMMAND ----------

SELECT now()

-- COMMAND ----------

SELECT current_timestamp();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Task 2: Delete Records
-- MAGIC Use the following command to delete specific records from the `retail_sales` table, removing all records for a specific customer:

-- COMMAND ----------

DELETE FROM retail_sales
WHERE customer_name = 'VASQUEZ,  YVONNE M';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###View the Table's History
-- MAGIC
-- MAGIC To understand the number of versions currently in the table, run the following command:

-- COMMAND ----------

DESCRIBE HISTORY retail_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Delta Lake supports various operations that allow robust data management:
-- MAGIC - **OPTIMIZE**: Optimizes the storage layout of the Delta table for better query performance. This operation compacts smaller files into larger ones, reducing the number of files scanned during queries.
-- MAGIC - **UPDATE**: Modifies existing records in the table based on a condition.
-- MAGIC - **DELETE**: Removes specific rows from the table based on a condition.
-- MAGIC
-- MAGIC These operations enable efficient data management and ensure the table remains up-to-date with minimal manual intervention.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Time Travel Queries
-- MAGIC
-- MAGIC Delta Lake's Time Travel feature allows you to query data as it existed in previous versions or at specific timestamps.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Task 1: Query a Specific Version
-- MAGIC Retrieve data from an early version of the table using the following command:

-- COMMAND ----------

--2025-07-23T19:02:53.426+00:00

-- COMMAND ----------

SELECT * 
  FROM retail_sales --v@3
  TIMESTAMP  AS OF  '2025-07-23T19:02:53.426+00:00'
--  VERSION AS OF 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Task 2: Query by Timestamp
-- MAGIC Retrieve data as it existed at a specific timestamp. Replace the text below with the timestamp copied earlier, uncomment the following lines, and run the cell.

-- COMMAND ----------

-- SELECT * 
--   FROM retail_sales
--   TIMESTAMP AS OF 'PASTE TIMESTAMP HERE';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Part 3: Restore Table to a Previous Version
-- MAGIC
-- MAGIC Delta Lake provides a powerful feature to restore a table to a previous state. This is particularly useful in scenarios where data is accidentally modified or deleted.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###View Table History
-- MAGIC Before restoring, check the table's history to identify the version you want to restore:

-- COMMAND ----------

DESCRIBE HISTORY retail_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This command displays the table's operation history, including timestamps and version numbers.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Restore the Table
-- MAGIC Use the following command to restore the `retail_sales` table to a specific version. For this demo, we’ll restore it to **Version 2**:

-- COMMAND ----------

RESTORE TABLE retail_sales TO VERSION AS OF 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This command reverts the table to the specified version, undoing any changes made after that version.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Verify the Restoration
-- MAGIC After restoring, query the table to confirm that it has been reverted to the expected state:

-- COMMAND ----------

SELECT * FROM retail_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The table will reflect the data as it existed in **Version 2**. This can also be validated by reviewing the history.
-- MAGIC Use this feature to safeguard data integrity and recover from unintended modifications.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY retail_sales;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use this feature to safeguard data integrity and recover from unintended modifications.

-- COMMAND ----------

ALTER TABLE retail_sales SET TBLPROPERTIES ('delta.enableChangeDataFeed' = True)

-- COMMAND ----------

DELETE FROM retail_sales
WHERE customer_name like 'V%';

-- COMMAND ----------

SELECT _change_type, count(*)  FROM table_changes('retail_sales',7)
group by _change_type

-- COMMAND ----------

select distinct product_category from retail_sales

-- COMMAND ----------

UPDATE retail_sales
   SET product_name = 'Apple'
   WHERE product_category = 'Opple';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC Delta Lake provides powerful features such as **Time Travel**, **Version History**, and **metadata management**, Delta Lake provides robust solutions for querying historical data, tracking changes, and recovering from unintended modifications. These capabilities not only ensure data reliability and consistency but also enhance scalability and performance.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
