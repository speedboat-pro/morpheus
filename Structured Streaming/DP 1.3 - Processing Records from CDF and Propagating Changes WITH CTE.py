# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Processing Records from CDF and Propagating Changes
# MAGIC
# MAGIC In this notebook, we'll demonstrate how you can easily propagate changes (inserts, updates and deletes) through a Lakehouse with Delta Lake [Change Data Feed (CDF)](https://docs.databricks.com/en/delta/delta-change-data-feed.html) via Stream or queried by its specific version.
# MAGIC
# MAGIC For this demo, we'll work with a slightly different dataset representing patient information for medical records. Descriptions of the data at various stages follow.
# MAGIC
# MAGIC #### Raw Files
# MAGIC We will be loading JSON files into a Delta table.
# MAGIC
# MAGIC #### Bronze Table (bronze_users)
# MAGIC Here we store all records as consumed. A row represents:
# MAGIC 1. A new patient providing data for the first time
# MAGIC 1. An existing patient confirming that their information is still correct
# MAGIC 1. An existing patient updating some of their information
# MAGIC
# MAGIC The type of action a row represents is not captured.
# MAGIC
# MAGIC #### Silver Table
# MAGIC This is the validated view of our data. Each patient will appear only once in this table. An upsert statement will be used to identify rows that have changed.
# MAGIC
# MAGIC
# MAGIC #### Gold Table
# MAGIC For this example, we'll create a simple gold table leveraging the silver table.
# MAGIC
# MAGIC
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Enable Change Data Feed on a cluster or for a particular table
# MAGIC - Describe how changes (insert, update and delete) are recorded
# MAGIC - Read CDF output with Spark SQL or PySpark
# MAGIC - Leverage the `change_table` function for tracking changes and `_change_type` column to get specific actions in your data. 
# MAGIC - Retrieve the latest history version from a table
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC   - In the drop-down, select **More**.
# MAGIC
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to your unique catalog name and the schema to your specific schema name shown below using the `USE` statements.
# MAGIC <br></br>
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG your-catalog;
# MAGIC USE SCHEMA your-catalog.pii_data;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-1.3

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Catalog and Schema
# MAGIC
# MAGIC Run the code below to view your current default catalog and schema. Ensure that their names match the ones from the cell above.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### A2. Source Data Volume
# MAGIC
# MAGIC Run the code below to view the volume path where the data will be stored for streaming. You can also check the contents in the Catalog Explorer, located in the left navigation pane. Currently no files are loaded to the volume.

# COMMAND ----------

print(f"Source Data Volume:{DA.paths.cdc_stream}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Structure Streaming Checkpoint folder
# MAGIC
# MAGIC The [checkpoint](https://docs.databricks.com/en/structured-streaming/checkpoints.html) location tracks information that identifies the query, including state data and processed records. When you delete files in a checkpoint directory or switch to a new checkpoint location, the next run of the query starts fresh.
# MAGIC
# MAGIC Run the code below to view the checkpoint path to use in our structured streaming process. You can also check this in the Catalog Explorer.Currently no files are loaded to the **_checkpoint** folder in the volume.

# COMMAND ----------

print(f"Checkpoint Volume:{DA.paths.checkpoints}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Create a Bronze Users Table and Ingest Data with Auto Loader
# MAGIC
# MAGIC In this section, we'll use Auto Loader to ingest data as it arrives.
# MAGIC
# MAGIC The steps below include:
# MAGIC * Declaring the target table
# MAGIC * Creating and starting the stream
# MAGIC * Loading data into the source directory

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### B1. Create the bronze_users table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop Table if exists
# MAGIC DROP TABLE IF EXISTS bronze_users;  
# MAGIC
# MAGIC -- Create bronze_users table
# MAGIC CREATE TABLE IF NOT EXISTS bronze_users ( 
# MAGIC   mrn BIGINT, 
# MAGIC   dob DATE, 
# MAGIC   sex STRING, 
# MAGIC   gender STRING, 
# MAGIC   first_name STRING, 
# MAGIC   last_name STRING, 
# MAGIC   street_address STRING, 
# MAGIC   zip BIGINT, city STRING, 
# MAGIC   state STRING, 
# MAGIC   updated timestamp
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Create, Start, and Load Stream Data into the Bronze Table
# MAGIC
# MAGIC The cell below completes the following:
# MAGIC - defines a schema for our the stream
# MAGIC - creates a stream into the **bronze_users** table that runs every **3 seconds**
# MAGIC - loads the first batch of JSON files to your *../pii_data/cdf_demo/stream_source/cdc* volume.
# MAGIC
# MAGIC **Note**: The stream will continue running every 3 seconds until it is terminated later in the notebook. Wait for the stream to finish reading the file before continuing to the next cell.
# MAGIC

# COMMAND ----------

DA.paths.stream_source.cdf_demo

# COMMAND ----------

# Define the schema for the incoming data
schema = """
  mrn BIGINT, 
  dob DATE, 
  sex STRING, 
  gender STRING, 
  first_name STRING, 
  last_name STRING, 
  street_address STRING, 
  zip BIGINT, 
  city STRING, 
  state STRING, 
  updated TIMESTAMP
"""

# Read the streaming data from the specified path with the defined schema into the Bronze Table
from pyspark.sql.functions import col, expr,date_add

bronze_users_stream = (
      spark
      .readStream
        .format("cloudFiles")                 # Specify the format as cloudFiles for auto loader
        .option("cloudFiles.format", "json")  # Specify the file format as JSON
        .schema(schema)                       # Apply the defined schema to the incoming data
        .load(DA.paths.cdc_stream)            # Load the data from the specified path
      .writeStream
        .format("delta")                      # Write the stream data in Delta format
        .outputMode("append")                 # Append new records to the table
        .trigger(processingTime='3 seconds')
        .option("checkpointLocation", f"{DA.paths.checkpoints}/bronze")  # Specify the checkpoint location
        .table("bronze_users"))               # Write the stream data to the Bronze table


# Load a file into the volume: DA.paths.cdc_stream
DA.load(copy_from=DA.paths.stream_source.cdf_demo, 
        copy_to=DA.paths.cdc_stream, 
        n=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Validate a File has been placed in your volume
# MAGIC
# MAGIC Wait for the stream to initialize and process the file that has been placed to be ingested into the **bronze_users** table. Run the cell below to get the exact names of your volumes. 
# MAGIC
# MAGIC In the catalog explorer check your source files and checkpoint volumes and confirm files have been added. You should see the following:
# MAGIC - In the  **cdc** folder a file named *batch01.json*
# MAGIC - In the **_checkpoints** a folder named **bronze** corresponding to the **bronze_users** table.
# MAGIC
# MAGIC **Example**
# MAGIC
# MAGIC ![Source and Checkpoint Folders](./Includes/images/stream_folders.png)
# MAGIC

# COMMAND ----------

print(f"Source Data Volume:{DA.paths.cdc_stream}")
print(f"Checkpoint Volume:{DA.paths.checkpoints}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### B4. Validate Ingested Data in the **bronze_users** Table
# MAGIC
# MAGIC Query the **bronze_users** table and confirm that *829* users have been loaded into the table from the first batch of JSON files (one file).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM bronze_users;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## C. Create a **silver_users** Target Table and Load It with Our Production Data
# MAGIC
# MAGIC Our **silver_users** table will be loaded with production data from our users to serve as a baseline. Here, we use `DEEP CLONE` to move read-only data from PROD to our environment, where we have full write/delete access.
# MAGIC
# MAGIC In this case, our production data is stored as a Delta table in the following location: `Volumes/dbacademy_gym_data/v01/pii/silver`

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_users;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE silver_users
# MAGIC DEEP CLONE delta.`/Volumes/dbacademy_gym_data/v01/pii/silver/`;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_users

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Enable Change Data Feed (CDF)
# MAGIC
# MAGIC You can enable CDF on both new and existing tables.
# MAGIC
# MAGIC To globally enable CDF on every new table, use the following syntax:
# MAGIC
# MAGIC ```spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", True)```
# MAGIC
# MAGIC Tables that were not created with CDF enabled will not have it turned on by default, but they can be altered to capture changes with the `ALTER TABLE` statement.
# MAGIC
# MAGIC For more information, see the [Enable Change Data Feed](https://docs.databricks.com/en/delta/delta-change-data-feed.html#enable-change-data-feed) documentation.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver_users 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Check if CDF is Enabled
# MAGIC
# MAGIC Use the `DESCRIBE TABLE EXTENDED` command on the silver table to check if CDF is enabled. In the output, look at the last row under **Table Properties** and confirm that CDF is set with the `[delta.enableChangeDataFeed=true]` property.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED silver_users;

# COMMAND ----------

# MAGIC %md
# MAGIC Query the **silver_users** table and confirm that it contains *3,132* rows.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM silver_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Upsert Data from Bronze to Silver Users
# MAGIC
# MAGIC In this section, we'll stream data from the **bronze_users** table to the **silver_users** table.
# MAGIC
# MAGIC We will create the `upsert_to_delta` function to handle the streaming `MERGE INTO` operation for the **silver_users** table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. `upsert_to_delta` Function
# MAGIC
# MAGIC Here, we create the upsert logic for the **silver_users** table using a streaming read from the **bronze_users** table.
# MAGIC
# MAGIC The function below performs the following steps:
# MAGIC - The `microBatchDF.createOrReplaceTempView("updates")` line takes the incoming micro-batch DataFrame (`microBatchDF`) and registers it as a temporary SQL view named **updates**.
# MAGIC - The `MERGE INTO` statement combines `UPDATE` and `INSERT` actions into one operation using our unique identifier **mrn** as the MERGE condition. This statement uses the incoming micro-batch data view **updates** as the source to update the target **silver_users**.

# COMMAND ----------

def upsert_to_delta(microBatchDF, batchId):
    # Create or replace a temporary view for the micro-batch DataFrame
    microBatchDF.createOrReplaceTempView("updates")
    
    # Perform a MERGE operation to upsert data into the silver table
    # The MERGE statement matches records in the 'silver' table with records in the 'updates' view based on the 'mrn' field
    # If a match is found and any of the specified fields are different, the existing record in 'silver' is updated with the new values from 'updates'
    # If no match is found, a new record is inserted into 'silver' with the values from 'updates'
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO silver_users s
        USING updates u
        ON s.mrn = u.mrn
        WHEN MATCHED AND s.dob <> u.dob OR
                         s.sex <> u.sex OR
                         s.gender <> u.gender OR
                         s.first_name <> u.first_name OR
                         s.last_name <> u.last_name OR
                         s.street_address <> u.street_address OR
                         s.zip <> u.zip OR
                         s.city <> u.city OR
                         s.state <> u.state OR
                         s.updated <> u.updated
            THEN UPDATE SET *
        WHEN NOT MATCHED
            THEN INSERT *
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Stream user's data from Bronze into Silver tables
# MAGIC
# MAGIC Now lets create and start the stream from **bronze_users** into **silver_users**. The `foreachBatch` method uses the `upsert_to_delta` function from above to upsert from a streaming query.
# MAGIC
# MAGIC Check the [Upsert from streaming queries using foreachBatch](https://docs.databricks.com/en/structured-streaming/delta-lake.html#merge-in-streaming) documentation for more information.
# MAGIC

# COMMAND ----------

silver_users_stream = (
              spark
              .readStream
                .table("bronze_users")
              .writeStream
                .foreachBatch(upsert_to_delta)  # Upsert micro-batch data into the silver table
                .trigger(processingTime='3 seconds')  # Trigger the stream processing every 3 seconds
                .start()
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. Check the History of the Silver Users Table
# MAGIC
# MAGIC Wait for the stream to initialize and process.
# MAGIC
# MAGIC Once complete, let's check the changes applied to our **silver_users** table.
# MAGIC
# MAGIC Running the cell below will show the history for this table, and there should be three log entries:
# MAGIC - Version 0: The initial clone
# MAGIC - Version 1: Setting table properties to enable CDF on the table
# MAGIC - Version 2: The MERGE stream from the **bronze_users** table.
# MAGIC
# MAGIC **NOTE:** If the results show only two versions, please wait for the stream to complete and then rerun the cell below.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_users

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Access and Read the Change Data Feed (CDF)
# MAGIC
# MAGIC The [Change Data Feed (CDF)](https://docs.databricks.com/en/delta/delta-change-data-feed.html) feature allows tracking of row-level changes between versions of a Delta table. When enabled, it records change events for all data written to the table, including metadata that indicates whether a row was inserted, deleted, or updated.
# MAGIC
# MAGIC To capture the recorded CDC data in a stream, we'll add two options:
# MAGIC - **`readChangeData` = True**
# MAGIC - **`startingVersion` = 2** (This refers to the version history; alternatively, you can use **`startingTimestamp`**)
# MAGIC
# MAGIC ## CDF Schema in Results
# MAGIC When reading from the change data feed, the schema includes the following metadata columns:
# MAGIC
# MAGIC | Column name        | Type      | Description                                    |
# MAGIC |--------------------|-----------|------------------------------------------------|
# MAGIC | _change_type       | String    | The type of change event: `insert`, `delete`, `update_preimage`, `update_postimage`. |
# MAGIC | _commit_version    | Long      | The Delta log or table version containing the change |
# MAGIC | _commit_timestamp  | Timestamp | The timestamp when the commit was created      |
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC In this section, we’ll display all changes to patients in the **silver_users** table starting at version *2*.
# MAGIC
# MAGIC Run the cell below and view the results. Notice that the results show 1,530 records have been modified.
# MAGIC
# MAGIC Then, scroll to the right of the table. You will notice the new columns: **_change_type**, **_commit_version**, and **_commit_timestamp**.
# MAGIC
# MAGIC **NOTE:** Users with changes will have two records: one corresponding to *update_preimage* and one corresponding to *update_postimage*.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("silver_users", 1,2)    -- Query the latest update
# MAGIC --WHERE _change_type = "insert"                         -- View all rows that were inserted
# MAGIC ORDER BY _commit_version

# COMMAND ----------

cdf_df = (spark.read
               .format("delta")
               .option("readChangeData", True)   # Read the change data
               .option("startingVersion", 2)     # Since we want to start reading changes from version 2 (when we merged the data)
               .table("silver_users"))

## Display the changed data
display(cdf_df)

# COMMAND ----------

cdf_df.createOrReplaceTempView('cdf')

# COMMAND ----------

# MAGIC %sql
# MAGIC select _change_type
# MAGIC , count(*) rows
# MAGIC from cdf
# MAGIC group by _change_type
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### E1. Land New Data to Stream
# MAGIC
# MAGIC As a recap, we currently have two active streams:
# MAGIC
# MAGIC 1. The first stream ingests data from our source folder (raw JSON files) into the **bronze_users** table.
# MAGIC 2. The second stream syncs data via `MERGE INTO` from the **bronze_users** table to the **silver_users** table.
# MAGIC
# MAGIC Run the cell below to land another JSON file in our source directory and stream additional data into the pipeline.

# COMMAND ----------

# Load files into the volume: DA.paths.cdc_stream
DA.load(copy_from=DA.paths.stream_source.cdf_demo, 
        copy_to=DA.paths.cdc_stream, 
        n=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### E2. Check again silver_users Table History
# MAGIC
# MAGIC #### NOTE: Wait a few seconds for the stream to process the newly added JSON file and upsert data into the **silver_users** table.
# MAGIC
# MAGIC Our objective is to validate the captured CDC changes for **`_commit_version` number 3** (you can change the sort order of the **`_commit_version`** column in the display above to see this).
# MAGIC
# MAGIC Run the cell below to view the history of the **silver_users** table after the new file was added to the volume. 
# MAGIC
# MAGIC After running the cell, view the results and confirm that version *3* is now available in the history with a *MERGE* operation. This should correspond to the last cell where new files were loaded.
# MAGIC
# MAGIC **NOTE:** If three rows are returned in the results, please wait a few more seconds for the stream to complete, then rerun the cell. The history should display 4 rows.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Table Changes Function
# MAGIC
# MAGIC To pick up the recorded CDC data for a specific range of table's history versions there is the [table_changes](https://docs.databricks.com/en/sql/language-manual/functions/table_changes.html) function is part of the Delta Lake Change Data Feed (CDF) feature, which tracks row-level changes between versions of a Delta table. It returns a log of changes to a Delta Lake table with Change Data Feed enabled, including inserts, updates, and deletes.
# MAGIC
# MAGIC Use the following syntax: `table_changes(table_str, start [, end])`
# MAGIC - _table_str_:  The table name
# MAGIC - _start_: starting history version or timestamp
# MAGIC - _end_: optional ending version or timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### F1. Get the Latest Version of the Table's History
# MAGIC
# MAGIC The cell below will capture the most recent version of the **silver_users** table and store it in an SQL variable.
# MAGIC
# MAGIC Confirm that the variable **latest_version** equals *3*.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the existing variable if it exists
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS latest_version;
# MAGIC
# MAGIC -- Declare the variable
# MAGIC DECLARE VARIABLE latest_version INT;
# MAGIC
# MAGIC -- Set the variable to the latest version of the table
# MAGIC SET VARIABLE latest_version = (
# MAGIC   SELECT max(version) AS latest_version
# MAGIC   FROM (DESCRIBE HISTORY silver_users)
# MAGIC );
# MAGIC
# MAGIC -- Select the variable
# MAGIC SELECT latest_version;

# COMMAND ----------

# MAGIC %md
# MAGIC ### F2. Get Operation Metrics for Inserted, Updated, and Deleted Rows
# MAGIC
# MAGIC The cell below will query the table’s latest history using the **latest_version** variable and display the following operation metrics for the most recent update/insert/delete:
# MAGIC - **numTargetRowsInserted**: The number of rows inserted in the latest version (147 inserted).
# MAGIC - **numTargetRowsUpdated**: The number of rows updated in the latest version (809 updated).
# MAGIC - **numTargetRowsDeleted**: The number of rows deleted in the latest version (0 deleted).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   operationMetrics['numTargetRowsInserted'],
# MAGIC   operationMetrics['numTargetRowsUpdated'],
# MAGIC   operationMetrics['numTargetRowsDeleted']
# MAGIC FROM (DESCRIBE HISTORY silver_users)
# MAGIC WHERE version = latest_version

# COMMAND ----------

# MAGIC %md
# MAGIC ### F3. View Modified Rows (Inserted, Updated or Deleted)

# COMMAND ----------

# MAGIC %md
# MAGIC The **_change_type** column allows us to easily see how rows were modified.
# MAGIC
# MAGIC - *insert*: Indicates a new row was added to the table.
# MAGIC - *delete*: Indicates a row was removed from the table.
# MAGIC - *update_preimage*: Represents the value of a row before it was updated.
# MAGIC - *update_postimage*: Represents the value of a row after it was updated.

# COMMAND ----------

# MAGIC %md
# MAGIC #### F3.1 View All Rows That Were Inserted (insert)
# MAGIC
# MAGIC In the cell below, use the `table_changes` function to query most recent update to the **silver_users** table and filter for all rows that were inserted. Use the **_change_type** column to filter for rows with the value *insert*. Run the cell.
# MAGIC
# MAGIC Notice that the resulting row count matches the one provided in the previous cell: ('numTargetRowsInserted') = *147*. 
# MAGIC
# MAGIC Scroll to the right of the table and view the columns **_change_type**, **_commit_version** and **_commit_timestamp**.
# MAGIC
# MAGIC
# MAGIC With CDF, you can easily inspect every row that was inserted.

# COMMAND ----------

# MAGIC %sql
# MAGIC EXPLAIN FORMATTED with cdf as
# MAGIC (SELECT * 
# MAGIC FROM table_changes("silver_users", 2,2)    -- Query the latest update
# MAGIC -- WHERE _change_type = "insert"                         -- View all rows that were inserted
# MAGIC -- ORDER BY _commit_version
# MAGIC )
# MAGIC ,base as
# MAGIC (select _change_type
# MAGIC , count(*) `rows`
# MAGIC from cdf
# MAGIC group by _change_type)
# MAGIC select `rows`, count(*) 
# MAGIC from base
# MAGIC group by `rows`

# COMMAND ----------

# MAGIC %md
# MAGIC #### F3.2 View Updated Row (update_postimage)
# MAGIC
# MAGIC In the cell below, use the `table_changes` function to query the most recent update to the **silver_users** table and filter for all updated rows. Use the **_change_type** column to filter for rows with the value *update_postimage*. Run the cell.
# MAGIC
# MAGIC Notice that the resulting row count matches the one provided in the previous cell: ('numTargetRowsUpdated'): *809*. 
# MAGIC
# MAGIC Scroll to the right of the table and view the columns **_change_type**, **_commit_version** and **_commit_timestamp**.
# MAGIC
# MAGIC With CDF, you can easily inspect every updated row.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("silver_users", latest_version)
# MAGIC WHERE _change_type = "update_postimage"
# MAGIC ORDER BY _commit_version

# COMMAND ----------

# MAGIC %md
# MAGIC #### F3.3 View Row Prior to Update (update_preimage)
# MAGIC
# MAGIC In the cell below, use the `table_changes` function to query the most recent update to the **silver_users** table and filter for all rows prior to the update. Use the **_change_type** column to filter for rows with the value *update_preimage*. Run the cell.
# MAGIC
# MAGIC Notice that the resulting row count matches the one provided in the previous cell: ('numTargetRowsUpdated'): *809*. 
# MAGIC
# MAGIC Scroll to the right of the table and view the columns **_change_type**, **_commit_version** and **_commit_timestamp**.
# MAGIC
# MAGIC With CDF, you can easily inspect every row prior to the update.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("silver_users", latest_version)
# MAGIC WHERE _change_type = "update_preimage"
# MAGIC ORDER BY _commit_version

# COMMAND ----------

# MAGIC %md
# MAGIC #### F3.4 View All Deleted Rows (delete)
# MAGIC
# MAGIC In the cell below, use the `table_changes` function to query the most recent update to the **silver_users** table and filter for all rows that were deleted. Use the **_change_type** column to filter for rows with the value *delete*. Run the cell.
# MAGIC
# MAGIC Notice that the resulting row count matches the one provided in the previous cell: ('numTargetRowsDeleted'): *0*. 
# MAGIC
# MAGIC With CDF, you can easily inspect every deleted row.
# MAGIC
# MAGIC **NOTE:** Don't worry! We'll review how to apply deletes and propagate them in the next section.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("silver_users", latest_version)
# MAGIC WHERE _change_type in ("delete")
# MAGIC ORDER BY _commit_version

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## G. Propagating Deletes
# MAGIC
# MAGIC While some use cases may require processing deletes alongside updates and inserts, the most important delete requests are those that allow companies to maintain compliance with privacy regulations such as GDPR and CCPA. Most companies have stated SLAs around how long these requests will take to process, but for various reasons, these are often handled in pipelines separate from their core ETL.
# MAGIC
# MAGIC This section is focused to Propagate Deletes applied in the **silver_users** to propagate into the **gold_users** table.
# MAGIC
# MAGIC **NOTE:** Please follow all regulatory standards for your data.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### G1. Gold Users Table Setup
# MAGIC
# MAGIC Let's first create the **gold_users** table, our final production table.
# MAGIC
# MAGIC Run the query below to create the **gold_users** table and populate it with the latest snapshot of the **silver_users** table. After running the query, view the results and confirm that the table contains *3,407* records.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS gold_users;
# MAGIC
# MAGIC -- Create the gold_users table from the silver_users table
# MAGIC CREATE OR REPLACE TABLE gold_users as
# MAGIC SELECT 
# MAGIC      mrn,
# MAGIC      street_address,
# MAGIC      zip,
# MAGIC      city,
# MAGIC      state,
# MAGIC      updated
# MAGIC FROM silver_users;
# MAGIC
# MAGIC -- View the gold table
# MAGIC SELECT * 
# MAGIC FROM gold_users;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### G2. Processing Right to Be Forgotten Requests
# MAGIC
# MAGIC While it is possible to process deletes at the same time as appends and updates, the fines around right to be forgotten requests may warrant a separate process.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Let's start by leveraging the **user_delete_requests** table. This table holds the **mrn**, and the **request_date** (as of today) for users to be deleted, as provided by the compliance team.
# MAGIC
# MAGIC Let's query the table and view the deletion requests. Confirm that the table currently contains 20 requests for deletion.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM user_delete_requests;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC We’ll create a new read stream from the **user_delete_requests** table to complete the following:
# MAGIC - Add a column named **deadline** to indicate a 30-day period for action, with a default status of *requested*.
# MAGIC - Add a column named **status** and update it from *requested* to *deleted* once the changes are propagated.
# MAGIC
# MAGIC
# MAGIC Run the query and view the results.

# COMMAND ----------

requests_df = (spark.readStream
                    .table("user_delete_requests")
                    .select(
                            "mrn",
                            "request_date",
                            F.date_add("request_date", 30).alias("deadline"),
                            F.lit("requested").alias("status")))


display(requests_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### G3. Adding Commit Messages in History
# MAGIC
# MAGIC Delta Lake supports arbitrary commit messages, which are recorded in the Delta transaction log and viewable in the table history. This feature can assist with auditing.
# MAGIC
# MAGIC Setting a global commit message with SQL will ensure it is used for all subsequent operations in the notebook.
# MAGIC
# MAGIC For more information, refer to the [Enrich Delta Lake tables with custom metadata](https://docs.databricks.com/en/delta/custom-metadata.html#enrich-delta-lake-tables-with-custom-metadata) documentation.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.commitInfo.userMetadata=Deletes committed

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC With DataFrames, commit messages can also be specified as part of the write options using `option("userMetadata","comment")`.
# MAGIC
# MAGIC In this section, we will create a new streaming table named **delete_requests** to indicate that we are manually processing these requests in the notebook, rather than using an automated job.
# MAGIC

# COMMAND ----------

query = (requests_df
         .writeStream
            .outputMode("append")        # Append mode to add new rows to the output table
            .option("checkpointLocation", f"{DA.paths.checkpoints}/delete_requests")  # Specify checkpoint location
            .option("userMetadata", "Requests processed interactively")  # Add user metadata
            .trigger(availableNow=True)  # Trigger the query to process all available data now
            .table("delete_requests"))   # Write the output to the delete_requests table


query.awaitTermination()  # Wait for the streaming query to finish

# COMMAND ----------

# MAGIC %md
# MAGIC View the history of the **delete_requests** table. Notice that the **operation** column messages clearly indicate *CREATE TABLE* and *STREAMING UPDATE* in the table history.
# MAGIC
# MAGIC Scroll to the right of the table. Notice the following:
# MAGIC - that the **userMetadata** column contains the note metadata note *Requests processed interactively* that we set above for the stream.
# MAGIC - the initial *CREATE TABLE* contains the user metadata *Deletes committed* for the creation of the table. 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delete_requests

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### G4. Processing Delete Requests
# MAGIC
# MAGIC The **delete_requests** table will be used to track users' requests to be forgotten. 
# MAGIC
# MAGIC It is possible to process delete requests alongside inserts and updates to existing data as part of a normal **`MERGE`** statement.
# MAGIC
# MAGIC Because PII exists in several places through the current lakehouse, tracking requests and processing them asynchronously may provide better performance for production jobs with low latency SLAs. The approach modeled here also indicates the time at which the delete was requested and the deadline, and provides a field to indicate the current processing status of the request.

# COMMAND ----------

# MAGIC %md
# MAGIC Query the new **delete_requests** table and view the results. Notice that the deletion requests are active, with a **status** is *requested*.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM delete_requests;

# COMMAND ----------

# MAGIC %md
# MAGIC ### G5. Check the Records to Delete
# MAGIC
# MAGIC When working with static data, committing deletes is straightforward. Run the cell below to preview the records to delete from the **silver_users** table. Notice that the **silver_users** table contains all users who have requested deletion.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_users
# MAGIC WHERE mrn IN (SELECT mrn FROM delete_requests)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### G6. Committing Deletes into Silver Users
# MAGIC
# MAGIC The following cell deletes records from the **silver_users** table by rewriting all data files containing records affected by the `DELETE` statement. 
# MAGIC
# MAGIC **NOTE:** Recall that with Delta Lake, deleting data will create new data files rather than deleting existing data files.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM silver_users
# MAGIC WHERE mrn IN (SELECT mrn FROM delete_requests)

# COMMAND ----------

# MAGIC %md
# MAGIC Describe the history of the **silver_users** table and confirm *4* versions of the table exists. The latest version contains the *DELETE* value in the **operation** column.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_users;

# COMMAND ----------

spark.sql("RESTORE silver_users VERSION AS OF 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_users

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED silver_users

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM table_changes("silver_users", 4)  

# COMMAND ----------

# MAGIC %md
# MAGIC ### G7. Collect Deleted Silver Users to Propagate with CDF
# MAGIC
# MAGIC The code below configures an incremental read of all changes committed to the **silver_users** table starting at version *4*, the delete operation.
# MAGIC
# MAGIC Run the cell and view the results. Scroll to the right of the table and notice that in this version *20* rows were deleted from the **silver_users** table. You can view the exact rows that were deleted with CDF.
# MAGIC

# COMMAND ----------

deleteDF = (spark.readStream
                 .format("delta")
                 .option("readChangeFeed", "true")
                 .option("startingVersion", 4)     # Start_version 4 where the delete operation occurred
                 .table("silver_users"))


display(deleteDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silver_users

# COMMAND ----------

# MAGIC %md
# MAGIC ### G8. Function to Propagate Deletes
# MAGIC
# MAGIC The relationships between our natural keys (**mrn**) are stored in the **silver_users** table. These keys allow us to link a user's data across various pipelines and sources. The Change Data Feed (CDF) from this table will retain all these fields, enabling successful identification of records to be deleted or modified in downstream tables. This approach can be expanded to use hashed values or other relevant keys.
# MAGIC
# MAGIC The function below demonstrates how to commit deletes to two tables using different keys and syntax. Note that, in this case, the `MERGE INTO` syntax is not necessarily the only method to process deletes to the **gold_users** table. However, this code block demonstrates the basic syntax that could be extended if inserts and updates were processed alongside deletes in the same operation.
# MAGIC
# MAGIC Assuming successful completion of these two table modifications, an update will be processed back to the **delete_requests** table as well. 
# MAGIC
# MAGIC The code below completes the following for each batch:
# MAGIC - Deletes rows in the **gold_users** table that have been requested.
# MAGIC - Updates the status of the requested deletes in the **delete_requests** table.
# MAGIC

# COMMAND ----------

def process_deletes(microBatchDF, batchId):
    
    (microBatchDF
        .createOrReplaceTempView("deletes"))
    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO gold_users u
        USING deletes d
        ON u.mrn = d.mrn
        WHEN MATCHED
            THEN DELETE
    """)

    
    microBatchDF._jdf.sparkSession().sql("""
        MERGE INTO delete_requests dr
        USING deletes d
        ON d.mrn = dr.mrn
        WHEN MATCHED
          THEN UPDATE SET status = "deleted"
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### G9. Propagate Changes into Gold Users and Delete Requests
# MAGIC
# MAGIC Recall that this workload is driven by incremental changes to the **silver_users** table (tracked through the Change Data Feed).
# MAGIC
# MAGIC Executing the following cell will propagate deletes from a single table to multiple tables throughout the lakehouse.
# MAGIC

# COMMAND ----------

query = (deleteDF.writeStream
                 .foreachBatch(process_deletes)
                 .outputMode("update")
                 .option("checkpointLocation", f"{DA.paths.checkpoints}/deletes")
                 .trigger(availableNow=True)
                 .start())

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### G10. Review Delete Commits
# MAGIC Notice the **status** for the records in the **delete_requests** table are now updated to *deleted*.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM delete_requests;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Describe the history of the **gold_users** table.
# MAGIC
# MAGIC Notice that in the latest version that our commit message will be in the far right column of our history, under the column **userMetadata**.
# MAGIC
# MAGIC For the **gold_users** table, the **operation** column in the history will indicate a merge because of the chosen syntax, even though only deletes were committed. The number of deleted rows can be reviewed in the **operationMetrics** in the key *numTargetRowsDeleted*.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold_users;

# COMMAND ----------

# MAGIC %md
# MAGIC Count the number of rows in the current **gold_users** table. Confirm that the table now has *3,387* rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS TotalRows
# MAGIC FROM gold_users;

# COMMAND ----------

# MAGIC %md
# MAGIC ### G11. Are Deletes Fully Committed?
# MAGIC
# MAGIC Not exactly.
# MAGIC
# MAGIC Due to how Delta Lake's history and CDF features are implemented, deleted values are still present in older versions of the data.
# MAGIC
# MAGIC Run the query below to count the number of records from version 0 of the **gold_users** table. The results show that the original table has 3,407 rows, which includes the deleted rows.
# MAGIC
# MAGIC With Delta tables, you can still view the original data in an earlier version of the table.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS TotalRows
# MAGIC FROM gold_users VERSION AS OF 0;

# COMMAND ----------

# MAGIC %md
# MAGIC For more information check out [GDPR and CCPA compliance with Delta Lake](https://docs.databricks.com/en/security/privacy/gdpr-delta.html#how-delta-lake-simplifies-point-deletes) and the [VACUUM](https://docs.databricks.com/en/sql/language-manual/delta-vacuum.html) statement.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## H. Stop Active Streams
# MAGIC Make sure to run the following cell to stop all active streams. Be careful when using streaming. If you do not stop an active stream, the cluster will continuously run. We are done with streaming data in this demonstration.

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()
    stream.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
