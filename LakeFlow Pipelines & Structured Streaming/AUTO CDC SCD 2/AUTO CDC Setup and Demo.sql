-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 2.2 Demo - Programmatic Exploration and Data Ingestion to Unity Catalog
-- MAGIC
-- MAGIC In this demonstration, we will programmatically explore our data objects, display a raw CSV file from a volume, then read the CSV file from the volume and create a table.
-- MAGIC
-- MAGIC ### Objectives
-- MAGIC - Apply programmatic techniques to view data objects in our environment.
-- MAGIC - Demonstrate how to upload a CSV file into a volume in Databricks.
-- MAGIC - Demonstrate how to use a `CREATE TABLE` statement to create a table from a CSV file in a Databricks volume.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT A SHARED SQL WAREHOUSE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select the **SHARED SQL WAREHOUSE** in the lab. Follow these steps:
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down to select compute (it might say **Connect**). Complete one of the following below:
-- MAGIC
-- MAGIC    a. Under **Recent resources**, check to see if you have a **shared_warehouse SQL**. If you do, select it.
-- MAGIC
-- MAGIC    b. If you do not have a **shared_warehouse** under **Recent resources**, complete the following:
-- MAGIC
-- MAGIC     - In the same drop-down, select **More**.
-- MAGIC
-- MAGIC     - Then select the **SQL Warehouse** button.
-- MAGIC
-- MAGIC     - In the drop-down, make sure **shared_warehouse** is selected.
-- MAGIC
-- MAGIC     - Then, at the bottom of the pop-up, select **Start and attach**.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC    <img src="../Includes/images/sql_warehouse.png" alt="SQL Warehouse" width="600">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course in the lab environment.
-- MAGIC
-- MAGIC ### IMPORTANT LAB INFORMATION
-- MAGIC
-- MAGIC Recall that your lab setup is created with the [0 - REQUIRED - Course Setup and Data Discovery]($../0 - REQUIRED - Course Setup and Data Discovery) notebook. If you end your lab session or if your session times out, your environment will be reset, and you will need to rerun the Course Setup notebook.

-- COMMAND ----------

-- MAGIC %run ../Includes/2.2-Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Programmatically Exploring Your Environment
-- MAGIC
-- MAGIC In this section, we will demonstrate how to programmatically explore your environment, an alternative method to using the Catalog Explorer.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View your available catalogs using the `SHOW CATALOGS` statement. Notice that your environment has a series of catalogs available.

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the following cell to view your default catalog and schema. You should notice that your default catalog is set to **samples** and your default schema is set to **nyctaxi**.
-- MAGIC
-- MAGIC    **NOTE:** Setting the default catalog and schema in Databricks allows you to avoid repeatedly typing the full path (catalog.schema.table) when referring to your data objects. Once set, Databricks will automatically use your chosen catalog and schema, making it easier and faster to work with your data without needing to specify the full namespace each time.
-- MAGIC

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Use the `SHOW SCHEMAS` statement to view available schemas within the default catalog. Notice that it displays schemas (databases) within the **samples** catalog since that is your current default catalog.

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. You can modify the `SHOW SCHEMAS` statement to specify a specific catalog, like the **dbacademy** catalog. Notice that this displays available schemas (databases) within the **dbacademy** catalog.

-- COMMAND ----------

SHOW SCHEMAS IN dbacademy;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. To view available tables in a schema, use the `SHOW TABLES` statement. Notice that, by default, it displays the one table within the default **samples** catalog in the **nyctaxi** schema (database).
-- MAGIC

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT * FROM _sqldf

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. To query the **samples.nyctaxi.trips** table, you only need to specify the table name **trips** and not the entire three-level namespace (catalog.schema.table) because the default catalog and schema are **samples** and **nyctaxi**, respectively.

-- COMMAND ----------

SELECT *
FROM trips
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Let's try querying the **dbacademy.labuser.ca_orders** table without using the three-level namespace. Notice that an error is returned because it is looking for the **ca_orders** table in **samples.nyctaxi**.

-- COMMAND ----------

-- This query will return an error 
-- This is because the table does not exist in the default catalog (samples) and default schema (nyctaxi) schema
SELECT *
FROM ca_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. We want to modify our default catalog and default schema to use **dbacademy** and our **labuser** schema to avoid writing the three-level namespace everytime we query and create tables in this course.
-- MAGIC
-- MAGIC     However, before we proceed, note that each of us has a different schema name. Your specific schema name has been stored dynamically in the SQL variable `DA.schema_name` during the classroom setup script.
-- MAGIC
-- MAGIC     Run the code below and confirm that the value of the `DA.schema_name` variable matches your specific schema name (e.g., **labuser1234_678**).

-- COMMAND ----------

values(DA.schema_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Let's modify our default catalog and schema using the `USE CATALOG` and `USE SCHEMA` statements.
-- MAGIC
-- MAGIC     - `USE CATALOG` – Sets the current catalog.
-- MAGIC
-- MAGIC     - `USE SCHEMA` – Sets the current schema. 
-- MAGIC
-- MAGIC     **NOTE:** Since our dynamic schema name is stored in the variable `DA.schema_name` as a string, we will need to use the `IDENTIFIER` clause to interpret the constant string in our variable as a schema name. The `IDENTIFIER` clause can interpret a constant string as any of the following:
-- MAGIC     - Relation (table or view) name
-- MAGIC     - Function name
-- MAGIC     - Column name
-- MAGIC     - Field name
-- MAGIC     - Schema name
-- MAGIC     - Catalog name
-- MAGIC
-- MAGIC     [IDENTIFIER clause documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-names-identifier-clause?language=SQL)
-- MAGIC
-- MAGIC   Alternatively, you can simply add your schema name without using the `IDENTIFIER` clause.

-- COMMAND ----------

USE CATALOG dbacademy;
USE SCHEMA IDENTIFIER(DA.schema_name);

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 10. Let's view the available tables in the **dbacademy** catalog within our **labuser** schema. Notice that your schema contains a variety of tables.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 11. Let's query the **ca_orders** table in the **dbacademy** catalog within our **labuser** schema without using the three-level namespace.

-- COMMAND ----------

SELECT *
FROM ca_orders
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 12. While you can set your default catalog and schema to avoid using the three-level namespace, there are times when you might want to reference a specific table. You can do this by specifying the catalog and schema name in the query.
-- MAGIC
-- MAGIC     In this example, let's query the **samples.nyctaxi.trips** table using the three-level namespace.

-- COMMAND ----------

SELECT *
FROM samples.nyctaxi.trips
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 13. Lastly, let's view the available volumes within our **labuser** schema in the **dbacademy** catalog using the `SHOW VOLUMES` statement. Notice that our **labuser** schema contains a variety of Databricks volumes, including the **backup** volume.

-- COMMAND ----------

SHOW VOLUMES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 14. Let's view the available files in our **dbacademy.labuser.backup** volume using the UI. Complete the following:
-- MAGIC
-- MAGIC     a. In the left navigation pane, expand the **dbacademy** catalog.
-- MAGIC
-- MAGIC     b. Expand your **labuser** schema.
-- MAGIC
-- MAGIC     c. Expand **Volumes**.
-- MAGIC
-- MAGIC     d. Expand the **backup** volume.
-- MAGIC
-- MAGIC     e. Notice that your volume contains the **au_products.csv** file.
-- MAGIC
-- MAGIC   **NOTES:** 
-- MAGIC   - You can also use the [LIST statement](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-list) to programmatically view available files in a volume shown below.
-- MAGIC   - The `LIST` statement does not list files in Unity Catalog managed tables.
-- MAGIC   - Add your unique schema name to the string in the `LIST` statement to programmatically view the file in the volume.

-- COMMAND ----------

-- Add your schema name in the LIST statement below, example - `/Volumes/dbacademy/labuser1234_5678/backup`
LIST '/Volumes/dbacademy/labuser12370010_1761748415/backup'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Create a Table From a CSV File in a Volume
-- MAGIC
-- MAGIC In this section, we will use SQL to create a table (Delta Table) from a CSV file stored in a Databricks volume using two methods:
-- MAGIC - `read_files`
-- MAGIC - `COPY INTO`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Our goal is to read the **au_products.csv** file and create a table. To start, it's good practice to examine the raw file(s) you want to use to create a table. We can do that with the following code to [query the data by path](https://docs.databricks.com/aws/en/query#query-data-by-path). This enables us to see:
-- MAGIC
-- MAGIC     - The delimiter of the CSV file
-- MAGIC
-- MAGIC     - The general structure of the CSV file
-- MAGIC
-- MAGIC     - If the CSV contains headers in the first row
-- MAGIC     
-- MAGIC     - You can use this technique to query a variety of file types.
-- MAGIC
-- MAGIC     Complete the following:
-- MAGIC
-- MAGIC       a. In the left navigation pane, navigate to your **backup** volume and find the **au_products.csv** file.
-- MAGIC
-- MAGIC       b. In the cell below, place your cursor between the two backticks.
-- MAGIC
-- MAGIC       c. In the navigation pane, hover over the **au_products.csv** file and select the `>>` to insert the path of the CSV file between the backticks where it says 'REPLACE WITH YOUR VOLUME PATH'. Yours will look something like this with your unique schema name:
-- MAGIC
-- MAGIC
-- MAGIC       ```SQL
-- MAGIC       SELECT *
-- MAGIC       FROM text.`/Volumes/dbacademy/labuser1234_5678/backup/au_products.csv`
-- MAGIC       ```
-- MAGIC
-- MAGIC       d. Run the cell and view the results. Notice that:
-- MAGIC       - The first row of the CSV file contains a header
-- MAGIC       - The values are separated by a comma

-- COMMAND ----------

-- Add the volume path to your file: Example - `/Volumes/dbacademy/labuser_1234_5678/backup/au_products.csv`
SELECT *
FROM csv.`/Volumes/dbacademy/labuser12370010_1761748415/backup/au_products.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. read_files Function

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. In the cell below, let's create a table named **au_products** in the **dbacademy** catalog within your **labuser** schema using the **au_products.csv** file with the [read_files table-valued function](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) (TVF).
-- MAGIC
-- MAGIC     The `read_files` function reads files from a provided location and returns the data in tabular form. It supports reading JSON, CSV, XML, TEXT, BINARYFILE, PARQUET, AVRO, and ORC file formats.
-- MAGIC
-- MAGIC     The `read_files` function below uses the following options:
-- MAGIC
-- MAGIC       - The path of the CSV file is created by concatenating the volume path with your schema name, which is stored in the `DA.schema_name` variable. This allows dynamic referencing of the file for your unique lab schema name.
-- MAGIC       
-- MAGIC       - The `format => 'csv'` option specifies the data file format in the source path. The format is auto-inferred if not provided.
-- MAGIC       
-- MAGIC       - The `header => true` option specifies that the CSV file contains a header.
-- MAGIC       
-- MAGIC       - When the schema is not provided, `read_files` attempts to infer a unified schema across the discovered files, which requires reading all the files.
-- MAGIC
-- MAGIC **NOTES:**
-- MAGIC
-- MAGIC - There are a variety of different options for each file type. You can view the available options for your specific file type in the [Options](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files#options) documentation.
-- MAGIC
-- MAGIC - If a volume contains related files, you can read all of the files into a table by specifying the path of the volume without the file name. In this example, we are specifying the volume path and the file name.

-- COMMAND ----------

CREATE OR REPLACE TABLE au_products AS
SELECT * EXCEPT (_rescued_data)
FROM read_files(
  '/Volumes/dbacademy/' || DA.schema_name || '/backup/au_products.csv',
  format => 'csv',
  header => true
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View available tables in your **labuser** schema. Notice that a new table named **au_products** was created.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Query the **au_products** table within your **labuser** schema and view the results.
-- MAGIC
-- MAGIC     Notice the following:
-- MAGIC     - The table was created from the CSV file successfully.
-- MAGIC     - A new column named **_rescued_data** was added. This column is provided by default to rescue any data that doesn’t match the schema.

-- COMMAND ----------

SELECT *
FROM au_products;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. When the schema is not provided, `read_files` attempts to infer a unified schema across the discovered files, which requires reading all the files which can be inefficient for large files.
-- MAGIC
-- MAGIC     For larger files it's more efficient to specify the schema within the `read_files` function. For our small CSV file performance is not an issue.

-- COMMAND ----------

CREATE OR REPLACE TABLE au_products_with_schema AS
SELECT *
FROM read_files(
  '/Volumes/dbacademy/' || DA.schema_name || '/backup/au_products.csv',
  format => 'csv',
  header => true,
  schema => '
    productid STRING,
    productname STRING,
    listprice DOUBLE
  '
);

SELECT *
FROM au_products_with_schema;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. COPY INTO
-- MAGIC Another method to create a table from files is using the `COPY INTO` statement. The `COPY INTO` statement loads data from a file location into a Delta table. This is a retryable and idempotent operation — Files in the source location that have already been loaded are skipped. This is true even if the files have been modified since they were loaded.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. The cell below shows how to create a table and copy CSV data into it using `COPY INTO`:
-- MAGIC
-- MAGIC    a. The `CREATE TABLE` statement creates an empty table with a defined schema (columns `productid`, `productname`, and `listprice`). `COPY INTO` will copy the data into this table.
-- MAGIC
-- MAGIC    b. The `COPY INTO` command loads CSV files from the specified path into the created table. 
-- MAGIC
-- MAGIC    c. `FROM` specifies the volume path to read from. This could also reference an external location.
-- MAGIC
-- MAGIC     - The `FILEFORMAT = CSV` specifies the format of the input data. Databricks supports various file formats such as CSV, PARQUET, JSON, and more.
-- MAGIC     
-- MAGIC     - The `FORMAT_OPTIONS` specifies file format options like reading the header and schema inference.
-- MAGIC
-- MAGIC <br></br>
-- MAGIC
-- MAGIC **REQUIRED** - In the cell below add the path to YOUR **backup** volume in the `FROM` clause of `COPY INTO`. 
-- MAGIC
-- MAGIC Example: `FROM '/Volumes/dbacademy/labuser1234_5678/backup'`

-- COMMAND ----------

DROP TABLE IF EXISTS au_products_copy_into;


-- Create an empty table
-- You can define the schema of the table if you desire
CREATE TABLE au_products_copy_into(
  productid STRING,
  productname STRING,
  listprice DOUBLE
);


-- Copy the files into the table and merge the schema
COPY INTO au_products_copy_into
FROM '/Volumes/dbacademy/labuser12370010_1761748415/backup/au_products.csv'     -- TO DO: Add your path to the backup volume here
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Rerun the `COPY INTO` statement again after adding the path to your **backup** volume in the `FROM` clause again. 
-- MAGIC
-- MAGIC     Notice that **num_affected_rows** and **num_inserted_rows** are both 0. Since all of the data was already read, `COPY INTO` does not ingest the file(s) again.

-- COMMAND ----------

COPY INTO au_products_copy_into
FROM '/Volumes/dbacademy/labuser12370010_1761748415/backup/au_products.csv'   -- TO DO: Add your path to the backup volume here
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true', 'inferSchema'='true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Display the **au_products_copy_into** table.

-- COMMAND ----------

SELECT *
FROM au_products_copy_into;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Summary: COPY INTO (legacy)
-- MAGIC The CREATE STREAMING TABLE SQL command is the recommended alternative to the legacy COPY INTO SQL command for incremental ingestion from cloud object storage. See COPY INTO. For a more scalable and robust file ingestion experience, Databricks recommends that SQL users leverage streaming tables instead of COPY INTO.
-- MAGIC
-- MAGIC [COPY INTO (legacy)](https://docs.databricks.com/aws/en/ingestion/#copy-into-legacy)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C3. Introduction to Streaming Tables in DBSQL (Bonus)
-- MAGIC
-- MAGIC This is a high-level introduction to streaming tables in DBSQL. Streaming tables enable streaming or incremental data processing. Depending on your final objective, streaming tables can be extremely useful. 
-- MAGIC
-- MAGIC
-- MAGIC   We will briefly cover the topic to familiarize you with its capabilities. For more details, check out the [CREATE STREAMING TABLE documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table). Databricks offers several features for managing streaming and real-time data ingestion, so remember to consult with your Data Engineering team for additional support with streaming data.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. In this example, we will create a silver streaming table from a simple bronze table. In many scenarios, you will be reading from cloud storage to start the process. If you want to stream raw data into a table from cloud storage, view examples in the [CREATE STREAMING TABLE documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table).
-- MAGIC
-- MAGIC     To begin, let's create the table **emp_bronze_raw** with a list of employees.

-- COMMAND ----------

-- Use our catalog and schema
USE CATALOG dbacademy;
USE SCHEMA IDENTIFIER(DA.schema_name);

-- Drop the tables if they exist to start from the beginning
DROP TABLE IF EXISTS emp_bronze_raw;
DROP TABLE IF EXISTS emp_silver_streaming;

-- Create the employees table
CREATE TABLE emp_bronze_raw 
(
    EmployeeID INT,
    FirstName VARCHAR(20),
    Department VARCHAR(20)
);

ALTER TABLE emp_bronze_raw SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Insert 5 rows of sample data
INSERT INTO emp_bronze_raw (EmployeeID, FirstName, Department)
VALUES
(1, 'John', 'Marketing'),
(2, 'Raul', 'HR'),
(3, 'Michael', 'IT'),
(4, 'Panagiotis', 'Finance'),
(5, 'Aniket', 'Operations');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Use the `CREATE OR REFRESH STREAMING TABLE` statement to create a streaming table named **emp_silver_streaming**. This table will incrementally load new rows as they are added to the **emp_bronze_raw** table. It will also create a new column named **IngestDateTime**, which records the date and time when the row was ingested.
-- MAGIC
-- MAGIC **NOTES:**
-- MAGIC - This process will take about a minute to run. Behind the scenes, streaming tables create a DLT pipeline. We will cover DLT in detail later in this course.
-- MAGIC - Streaming tables are supported only in DLT and on Databricks SQL with Unity Catalog.

-- COMMAND ----------

-- CREATE OR REFRESH STREAMING TABLE emp_silver_streaming 
-- TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
-- SCHEDULE EVERY 1 HOUR     -- Scheduling the refresh is optional
-- SELECT 
--   *, 
--   current_timestamp() AS IngestDateTime
-- FROM STREAM emp_bronze_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. The `DESCRIBE HISTORY` statement displays a detailed list of all changes, versions, and metadata associated with a Delta table, including information on updates, deletions, and schema changes.
-- MAGIC
-- MAGIC     Run the cell below and view the results. Notice the following:
-- MAGIC
-- MAGIC     - In the **operation** column, you can see that a streaming table performs two operations: **DLT SETUP** and **STREAMING UPDATE**.
-- MAGIC     
-- MAGIC     - Scroll to the right and find the **operationMetrics** column. In row 1 (Version 2 of the table), the value shows that the **numOutputRows** is 5, indicating that 5 rows were added to the **emp_silver_streaming** table.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Run the cell below to query the **emp_silver_streaming** table. Notice that the results display 5 rows of data.

-- COMMAND ----------

SELECT *
FROM emp_silver_streaming VERSION AS OF 2;

-- COMMAND ----------

SELECT * from table_changes('emp_silver_streaming',3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Run the cell below to insert 2 rows of data into the originally **emp_bronze_raw** table.

-- COMMAND ----------

INSERT INTO emp_bronze_raw (EmployeeID, FirstName, Department)
VALUES
(6, 'Athena', 'Marketing'),
(7, 'Pedro', 'Training');

-- COMMAND ----------

UPDATE emp_bronze_raw set Department = 'Analytics' where EmployeeID % 2 = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. In our scenario, the scheduled refresh occurs every hour. However, we don't want to wait that long. Use the `REFRESH STREAMING TABLE` statement to refresh the streaming table **emp_silver_streaming**. This statement refreshes the data for a streaming table (or a materialized view, which will be covered later).
-- MAGIC
-- MAGIC
-- MAGIC     For more information to refresh a streaming table, view the [REFRESH (MATERIALIZED VIEW or STREAMING TABLE) documentation](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-refresh-full).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 7. Run the `DESCRIBE HISTORY` statement again to view the changes in the table.
-- MAGIC
-- MAGIC     Notice the following:
-- MAGIC     - The **emp_silver_streaming** table now has a new version, version 3.
-- MAGIC
-- MAGIC     - Scroll over to the **operationParameters** column. Notice that **outputMode** specifies an **Append** operation occurred.
-- MAGIC     
-- MAGIC     - Scroll over to the **operationMetrics** column. Notice that the value of **numOutputRows** is 2, indicating an incremental update occurred and that two new rows were added to the **emp_silver_streaming** table.

-- COMMAND ----------

DESCRIBE HISTORY emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 8. Run the query below to view the data in **emp_silver_streaming**. Notice that the table now contains 7 rows.

-- COMMAND ----------

SELECT *
FROM emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 9. Lastly, let's drop the two tables we created.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # CDC Demo

-- COMMAND ----------

CREATE OR REPLACE TABLE cdf_bronze_tbl AS
SELECT 
_change_type AS changetype,
_commit_timestamp AS commit_timestamp
 , *
EXCEPT (_change_type, _commit_version, _commit_timestamp) FROM table_changes('emp_bronze_raw',1)
WHERE _change_type != 'update_preimage'

-- COMMAND ----------

UPDATE emp_bronze_raw set Department = 'Analytics' where EmployeeID % 2 = 1

-- COMMAND ----------

DELETE FROM emp_bronze_raw WHERE EmployeeID = 5

-- COMMAND ----------

INSERT INTO cdf_bronze_tbl
SELECT 
_change_type AS changetype,
_commit_timestamp AS commit_timestamp
 , *
EXCEPT (_change_type, _commit_version, _commit_timestamp) FROM table_changes('emp_bronze_raw',1)
WHERE _change_type != 'update_preimage'

EXCEPT SELECT * FROM cdf_bronze_tbl

-- COMMAND ----------

select distinct concat('~', changetype,'~') from cdf_bronze_tbl

-- COMMAND ----------

DROP TABLE IF EXISTS emp_bronze_raw;
DROP TABLE IF EXISTS cdf_bronze_tbl;
DROP TABLE IF EXISTS cdf_bronze2;
DROP TABLE IF EXISTS emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
