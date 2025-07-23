-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demo - Data Ingestion Techniques
-- MAGIC
-- MAGIC This notebook demonstrates the practical application of various data ingestion techniques in the Databricks Lakehouse, including:
-- MAGIC - **CREATE TABLE AS SELECT** (CTAS)
-- MAGIC - **COPY INTO** for incremental data loading
-- MAGIC - Using the **Databricks Upload UI**
-- MAGIC - Automating real-time ingestion with **Auto Loader**
-- MAGIC - An introduction to **Lakeflow Connect**
-- MAGIC
-- MAGIC **Learning Objectives**
-- MAGIC
-- MAGIC By the end of this notebook, you should be able to:
-- MAGIC 1. Create and populate Delta tables using **CREATE TABLE AS SELECT (CTAS)**.
-- MAGIC 2. Incrementally load data into Delta tables using **COPY INTO**.
-- MAGIC 3. Perform manual data ingestion through the **Databricks Upload UI**.
-- MAGIC 4. Set up and manage real-time data ingestion pipelines with **Auto Loader**.
-- MAGIC 5. Introduction to **Lakeflow Connect** for automated data ingestion pipeline creation and management.

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

-- MAGIC %run ../Includes/Classroom-Setup-3.1

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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying Files
-- MAGIC In the cell below, we are going to run a query on a directory of parquet files. These files are not currently registered as any kind of data object (i.e., a table), but we can run some kinds of queries exactly as if they were. We can run these queries on many data file types, too (CSV, JSON, etc.).
-- MAGIC
-- MAGIC Most workflows will require users to access data from external cloud storage locations. 
-- MAGIC
-- MAGIC In most companies, a workspace administrator will be responsible for configuring access to these storage locations. In this course, we are simply going to use data files that were already set up as part of the lab environment.
-- MAGIC

-- COMMAND ----------

SELECT * FROM parquet.`${DA.paths.datasets.ecommerce}/raw/sales-historical` LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can equivalently use the `read_files()` function to read from files. The syntax is more complicated, but it allows us to pass parameters into the reader which is often required.

-- COMMAND ----------

SELECT * FROM
  read_files(
    '${DA.paths.datasets.retail}/source_files/sales.csv',
    format => 'csv',
    header => true,
    inferSchema => true
  ) LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Table as Select (CTAS)
-- MAGIC
-- MAGIC We are going to create a table that contains historical sales data from a previous point-of-sale system. This data is in the form of parquet files.
-- MAGIC
-- MAGIC **`CREATE TABLE AS SELECT`** statements create and populate Delta tables using data retrieved from an input query. We can create the table and populate it with data at the same time.
-- MAGIC
-- MAGIC CTAS statements automatically infer schema information from query results and do **not** support manual schema declaration. 
-- MAGIC
-- MAGIC This means that CTAS statements are useful for external data ingestion from sources with well-defined schema, such as Parquet files and tables.

-- COMMAND ----------

-- Create or replace the table 'retail_sales_bronze' using Delta format
CREATE OR REPLACE TABLE retail_sales_bronze 
  USING DELTA AS
    SELECT * FROM parquet.`${DA.paths.datasets.ecommerce}/raw/sales-historical`;

-- Describe the structure of the 'retail_sales_bronze' table
DESCRIBE retail_sales_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By running `DESCRIBE <table-name>`, we can see column names and data types. We see that the schema of this table looks correct.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## COPY INTO for Incremental Loading
-- MAGIC **`COPY INTO`** provides an idempotent option to incrementally ingest data from external sources.
-- MAGIC
-- MAGIC Note that this operation does have some expectations:
-- MAGIC - Data schema should be consistent
-- MAGIC - Duplicate records should try to be excluded or handled downstream
-- MAGIC
-- MAGIC This operation is potentially much cheaper than full table scans for data that grows predictably.
-- MAGIC
-- MAGIC We want to capture new data but not re-ingest files that have already been read. We can use `COPY INTO` to perform this action. 
-- MAGIC
-- MAGIC The first step is to create an empty table. We can then use COPY INTO to infer the schema of our existing data and copy data from new files that were added since the last time we ran `COPY INTO`.

-- COMMAND ----------

DROP TABLE IF EXISTS users_bronze;
CREATE TABLE users_bronze USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **COPY INTO** loads data from data files into a Delta table. This is a retriable and idempotent operation, meaning that files in the source location that have already been loaded are skipped.
-- MAGIC
-- MAGIC The cell below demonstrates how to use COPY INTO with a parquet source, specifying:
-- MAGIC 1. The path to the data.
-- MAGIC 1. The FILEFORMAT of the data, in this case, parquet.
-- MAGIC 1. COPY_OPTIONS -- There are a number of key-value pairs that can be used. We are specifying that we want to merge the schema of the data.

-- COMMAND ----------

COPY INTO users_bronze
  FROM '${DA.paths.datasets.ecommerce}/raw/users-30m'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ## COPY INTO is Idempotent
-- MAGIC COPY INTO keeps track of the files it has ingested previously. We can run it again, and no additional data is ingested because the files in the source directory haven't changed. Let's run the `COPY INTO` command again to show this. 
-- MAGIC
-- MAGIC The count of total rows is the same as the `number_inserted_rows` above because no new data was copied into the table.

-- COMMAND ----------

COPY INTO users_bronze
  FROM '${DA.paths.datasets.ecommerce}/raw/users-30m'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');


SELECT count(*) FROM users_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Built-In Functions
-- MAGIC
-- MAGIC Databricks has a vast [number of built-in functions](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin.html) you can use in your code.
-- MAGIC
-- MAGIC We are going to create a table for user data generated by the previous point-of-sale system, but we need to make some changes. 
-- MAGIC
-- MAGIC The `first_touch_timestamp` is in the wrong format. We need to divide the timestamp that is currently in microseconds by 1e6 (1 million). We will then use `CAST` to cast the result to a [TIMESTAMP](https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-type.html). Then, we `CAST` to [DATE](https://docs.databricks.com/en/sql/language-manual/data-types/date-type.html).
-- MAGIC
-- MAGIC Since we want to make changes to the `first_touch_timestamp` data, we need to use the `CAST` keyword. The syntax for `CAST` is `CAST(column AS data_type)`. We first cast the data to a `TIMESTAMP` and then to a `DATE`.  To use `CAST` with `COPY INTO`, we need to use a `SELECT` clause (make sure you include the parentheses) after the word `FROM` (in the `COPY INTO`).
-- MAGIC
-- MAGIC Our **`SELECT`** clause leverages two additional built-in Spark SQL commands useful for file ingestion:
-- MAGIC * **`current_timestamp()`** records the timestamp when the logic is executed
-- MAGIC * **`_metadata.file_name`** records the source data file for each record in the table
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS users_bronze;
CREATE TABLE users_bronze;
COPY INTO users_bronze FROM
  (SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    _metadata.file_name source_file
  FROM '${DA.paths.datasets.ecommerce}/raw/users-historical/')
  FILEFORMAT = PARQUET
  COPY_OPTIONS ('mergeSchema' = 'true');

SELECT * FROM users_bronze LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### UPLOAD UI
-- MAGIC The add data UI allows you to manually load data into Databricks from a variety of sources.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Download a data file. For the purposes of this exercise, you may download the **sales.csv** file by following [this link](/ajax-api/2.0/fs/files/Volumes/dbacademy_retail/v01/source_files/sales.csv). This will download the CSV file to your browser's download folder.
-- MAGIC 1. Upload the data file to create a table. In the [Catalog Explorer](/explore/data/dbacademy) (also available from the left sidebar), do the following:
-- MAGIC    1. In the **dbacademy** catalog, navigate to your schema. 
-- MAGIC    1. Select **Create > Create table** from the top-right corner.
-- MAGIC    1. Drop the **sales.csv** you just downloaded into the drop zone (or use the file navigator to find the file in your downloads folder).
-- MAGIC 1. Complete the following steps to create the table:
-- MAGIC    1. Under **Table name**, name the table **`retail_sales_ui`**. Note that options are available to configure additional ingestion behavior, although we do not need to change any of these for this exercise.
-- MAGIC    1. Click **Create table** at the bottom of the page to create the table.
-- MAGIC    1. Confirm the table was created successfully. Then close the Catalog Explorer tab.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Use the SHOW TABLES statement to view the available tables in your schema. Confirm that the **`retail_sales_ui`** table has been created.

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Query the table to review its contents.
-- MAGIC
-- MAGIC **NOTE**: If you did not name the table as instructed, an error will be returned.

-- COMMAND ----------

SELECT * FROM retail_sales_ui LIMIT 10;

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC # What is Databricks Auto Loader?
-- MAGIC
-- MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/autoloader/autoloader-edited-anim.gif" style="float:right; margin-left: 10px" />
-- MAGIC
-- MAGIC [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html) lets you scan a cloud storage folder (S3, ADLS, GS) and only ingest the new data that arrived since the previous run.
-- MAGIC
-- MAGIC This is called **incremental ingestion**.
-- MAGIC
-- MAGIC Auto Loader can be used in a near real-time stream or in a batch fashion, e.g., running every night to ingest daily data.
-- MAGIC
-- MAGIC Auto Loader provides a strong gaurantee when used with a Delta sink (the data will only be ingested once).
-- MAGIC
-- MAGIC ## How Auto Loader simplifies data ingestion
-- MAGIC
-- MAGIC Ingesting data at scale from cloud storage can be really hard at scale. Auto Loader makes it easy, offering these benefits:
-- MAGIC
-- MAGIC
-- MAGIC * **Incremental** & **cost-efficient** ingestion (removes unnecessary listing or state handling)
-- MAGIC * **Simple** and **resilient** operation: no tuning or manual code required
-- MAGIC * Scalable to **billions of files**
-- MAGIC   * Using incremental listing (recommended, relies on filename order)
-- MAGIC   * Leveraging notification + message queue (when incremental listing can't be used)
-- MAGIC * **Schema inference** and **schema evolution** are handled out of the box for most formats (csv, json, avro, images...)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Auto Loader basics
-- MAGIC Let's create a new Auto Loader stream that will incrementally ingest new incoming files.
-- MAGIC
-- MAGIC In this example we will specify the full schema. We will also use `cloudFiles.maxFilesPerTrigger` to take 1 file a time to simulate a process adding files 1 by 1.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Visualization and Important Notes
-- MAGIC
-- MAGIC Once the Auto Loader stream is running, click on the **display_query** link above the visualization (as shown in the image) to monitor metrics like input rate, processing rate, and batch duration.
-- MAGIC
-- MAGIC - The **Input vs. Processing Rate** chart shows how records are being ingested and processed over time.
-- MAGIC - The **Batch Duration** chart indicates the time taken to process each batch of records.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Use Auto Loader to read the cloud file
-- MAGIC schema_location = f"{DA.paths.working_dir}/retail_sales_schema"
-- MAGIC
-- MAGIC cloud_dir = f'{DA.paths.datasets.retail}/retail-pipeline/orders/stream_json/'
-- MAGIC
-- MAGIC retail_sales_df = (spark.readStream
-- MAGIC                    .format("cloudFiles")
-- MAGIC                    .option("cloudFiles.format", "json")
-- MAGIC                    .option("cloudFiles.maxFilesPerTrigger", "1")
-- MAGIC                    .option("cloudFiles.inferColumnTypes", "true") 
-- MAGIC                    .option("cloudFiles.schemaLocation", schema_location)  # Schema location for Auto Loader
-- MAGIC                    .load(cloud_dir))  # Load the directory containing the CSV file
-- MAGIC
-- MAGIC # Display the streaming DataFrame
-- MAGIC display(retail_sales_df)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _**🚨Important:**_
-- MAGIC
-- MAGIC Make sure to **interrupt the cell** after completing the demo. The streaming query will continue running until explicitly interrupted, which could result in unnecessary resource usage.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LAKEFLOW CONNECT
-- MAGIC
-- MAGIC **NOTE: Lakeflow Connect is an advanced feature for automated data pipeline creation.**
-- MAGIC
-- MAGIC Lakeflow Connect simplifies the creation and management of data pipelines for efficient ingestion and transformation of data into Delta Lake.
-- MAGIC
-- MAGIC ![lakeflow_connect.png](https://www.databricks.com/sites/default/files/inline-images/lakeflow-connect-video.gif?v=1718218999)
-- MAGIC
-- MAGIC **The key benefits of using Lakeflow Connect are:**
-- MAGIC - **Automated Pipeline Creation**: Easily configure data ingestion pipelines from various sources into Delta Lake without extensive coding.
-- MAGIC - **Seamless Integration**: Lakeflow Connect supports multiple data sources and formats, enabling users to unify their data ingestion workflows.
-- MAGIC - **Built-In Transformation**: Perform data validation, schema enforcement, and enrichment directly within the pipeline configuration.
-- MAGIC - **Scalable and Reliable**: Designed for large-scale data processing, ensuring high availability and fault tolerance for enterprise workloads.
-- MAGIC
-- MAGIC **Lakeflow Connect enables:**
-- MAGIC - Real-time and batch data ingestion.
-- MAGIC - Simplified pipeline monitoring and management.
-- MAGIC - Integration with Delta Lake and Databricks ecosystem tools for optimized data operations.
-- MAGIC
-- MAGIC **Documentation Reference**:
-- MAGIC Learn more about Lakeflow Connect and its capabilities in the [official Databricks documentation](https://docs.databricks.com/en/ingestion/lakeflow-connect/index.html).
-- MAGIC
-- MAGIC **NOTE:** Lakeflow Connect is in preview and not yet generally available. Updates will be provided once it becomes widely accessible.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conclusion
-- MAGIC
-- MAGIC This notebook covered key data ingestion techniques in the Databricks Lakehouse, such as **CTAS**, **COPY INTO**, the **Upload UI**, and **Auto Loader** for incremental ingestion. Additionally, we introduced **Lakeflow Connect** for automated and scalable pipeline creation. These methods ensure efficient, reliable, and consistent data ingestion workflows, meeting the diverse needs of modern data engineering tasks.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
