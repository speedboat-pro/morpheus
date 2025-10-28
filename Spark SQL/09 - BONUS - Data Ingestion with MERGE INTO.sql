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
-- MAGIC # 9 (BONUS) - Data Ingestion with MERGE INTO
-- MAGIC
-- MAGIC ### You may not have time to complete this during class, so please review it afterward.
-- MAGIC
-- MAGIC MERGE INTO in Databricks is a powerful tool for data ingestion, especially for data ingestion. It enables efficient, atomic, scalable upsert and delete operations. This command is useful when you have an existing Delta table and you wish to combine incoming data. 
-- MAGIC
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Utilize MERGE INTO to perform updates, inserts, and deletes on Delta tables.
-- MAGIC - Apply MERGE INTO with schema enforcement to manage data integrity.
-- MAGIC - Apply MERGE INTO with schema evolution to evolve the target tables.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default and you have a Shared SQL warehouse.
-- MAGIC
-- MAGIC <!-- ![Select Cluster](./Includes/images/selecting_cluster_info.png) -->
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC    - Click **More** in the drop-down.
-- MAGIC
-- MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 3. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this notebook.
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course in the lab environment.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-09

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the cell below to view your default catalog and schema. Notice that your default catalog is **dbacademy** and your default schema is your unique **labuser** schema.
-- MAGIC
-- MAGIC **NOTE:** The default catalog and schema are pre-configured for you to avoid the need to specify the three-level name when writing your tables (i.e., catalog.schema.table).

-- COMMAND ----------

SELECT current_catalog(), current_schema()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Preview the Current Delta table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Preview the **main_users_target** table (the target table to update). 
-- MAGIC
-- MAGIC     Notice that the table contains 4 rows of user information including their **id**, **first_name**, **email**, **sign_up_date**, and **status**. Each user's status is *current*.

-- COMMAND ----------

ALTER TABLE main_users_target SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')

-- COMMAND ----------

SELECT * 
FROM main_users_target
ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Preview the **update_users_source** table (the table to use to update the target). You can think of this as your incoming dataset that has arrived in cloud object storage. 
-- MAGIC
-- MAGIC     Notice that the table contains 4 rows and the same columns. In the **status** column, it displays the action to take for each user. We want to:
-- MAGIC     - delete user **id** *1*
-- MAGIC     - update the email of user **id** *2*
-- MAGIC     - add new users with **id** values of *5* and *6*.

-- COMMAND ----------

SELECT *
FROM update_users_source
ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. MERGE INTO
-- MAGIC
-- MAGIC As a part of ingestion, you can perform inserts, updates and deletes using data from a source table, view, or DataFrame into a target Delta table using the `MERGE` SQL operation. Delta Lake supports inserts, updates and deletes in `MERGE`, and supports extended syntax beyond the SQL standards to facilitate advanced use cases.
-- MAGIC <br></br>
-- MAGIC
-- MAGIC ```
-- MAGIC MERGE INTO target t
-- MAGIC USING source s
-- MAGIC ON {merge_condition}
-- MAGIC WHEN MATCHED THEN {matched_action}
-- MAGIC WHEN NOT MATCHED THEN {not_matched_action}
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C1. Merge Target with the Incoming Data
-- MAGIC In this scenario we will want to update the current **main_users_target** table with user updates from the **update_users_source** table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Use the `MERGE INTO` statement to merge the **update_users_source** table into the **main_users_target** table based on the **id** column.
-- MAGIC
-- MAGIC     The code below does the following:
-- MAGIC     - The `MERGE INTO` specifies the target table **main_users_target** to be modified. The table referenced must be a Delta table.
-- MAGIC     - The `USING` statement specifies the source table **update_users_source** to be merged into the target table.
-- MAGIC     - The `ON` statement specifies the condition for merging. Here it will merge based on the matching **id** values.
-- MAGIC     - The `WHEN MATCHED AND source.status = 'update' THEN UPDATE SET` clause will update the target table's **email** and **status** if the condition is true.
-- MAGIC     - The `WHEN MATCHED AND source.status = 'delete' THEN DELETE` clause will delete the target table's row if true.
-- MAGIC     - The `WHEN NOT MATCHED THEN INSERT {cols} VALUES {columns to insert}` clause will insert new rows from the target table if there is not a match of the **id** column.
-- MAGIC
-- MAGIC     Run the statement and view the results. Notice that:
-- MAGIC     - the **num_affected_rows** is *4*
-- MAGIC     - the **num_updated_rows** is *1*
-- MAGIC     - the **num_deleted_rows** is *1*
-- MAGIC     - the **num_inserted_rows** is *2*.
-- MAGIC

-- COMMAND ----------

MERGE INTO main_users_target target
USING update_users_source source
ON target.id = source.id
WHEN MATCHED AND source.status = 'update' THEN
  UPDATE SET 
    target.email = source.email,
    target.status = source.status
WHEN MATCHED AND source.status = 'delete' THEN
  DELETE
WHEN NOT MATCHED THEN
  INSERT (id, first_name, email, sign_up_date, status)
  VALUES (source.id, source.first_name, source.email, source.sign_up_date, source.status);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View the updated **main_users_target** table. Notice that:
-- MAGIC     - User **id** *1* was deleted.
-- MAGIC     - User **id** *2* has an updated email.
-- MAGIC     - User **id** *5* and *6* were added.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM main_users_target
ORDER BY id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Use the `DESCRIBE HISTORY` statement to view the history of the **main_users_target** table. Notice that there are now 4 versions of the table.
-- MAGIC     - Version *0* is the initial creation of the empty table.
-- MAGIC     - Version *1* is the insertion of values into the table.
-- MAGIC     - Version *2* is the merge (inserts, updates, deletes).
-- MAGIC     - Version *3* is the optimization that occurred on the Delta table.

-- COMMAND ----------

DESCRIBE HISTORY main_users_target;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. You can use `VERSION AS OF` to query a specific version of the table. Query version *1* of the table to view the original data.

-- COMMAND ----------

SELECT *
FROM main_users_target VERSION AS OF 1;

-- COMMAND ----------

select * from table_changes('main_users_target',3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### C2. Schema Enforcement and Schema Evolution with MERGE INTO
-- MAGIC What if your source data evolves and adds new columns? You can use `MERGE WITH SCHEMA EVOLUTION` to update the schema of the target table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the updated **main_users_target** table. Confirm that it contains *5* columns with the updated values from the previous `MERGE INTO`.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM main_users_target
ORDER BY ID;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. View the **new_users_source** table. This table contains an additional column named **country**, which captures information about our new users. This column was not captured with the original data.
-- MAGIC

-- COMMAND ----------

SELECT *
FROM new_users_source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Use the `MERGE INTO` statement to update the target table **main_users_target** with the **new_users_source** table.
-- MAGIC
-- MAGIC     The only change in this `MERGE INTO` statement is in the last `WHEN NOT MATCHED AND source.status='new' THEN` clause. Here, we added the **country** column to insert into the target table.
-- MAGIC
-- MAGIC     Run the query and view the error:
-- MAGIC
-- MAGIC     *Cannot resolve country in INSERT clause given columns target.id, target.first_name, target.email, target.sign_up_date, target.status.*
-- MAGIC
-- MAGIC     Notice that the statement cannot resolve the **country** column in the INSERT clause.
-- MAGIC

-- COMMAND ----------

--------------------------------------------
-- This query will return an ERROR
--------------------------------------------

MERGE WITH SCHEMA EVOLUTION INTO main_users_target target
USING new_users_source source
ON target.id = source.id
WHEN MATCHED AND source.status = 'update' THEN
  UPDATE SET 
    target.email = source.email,
    target.status = source.status
WHEN MATCHED AND source.status = 'delete' THEN
  DELETE
WHEN NOT MATCHED AND source.status = 'new' THEN
  INSERT (id, first_name, email, sign_up_date, status, country)
  VALUES (source.id, source.first_name, source.email, source.sign_up_date, source.status, source.country);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. You must explicitly enable schema evolution to evolve the schema of the target table. In Databricks Runtime 15.2 and above, you can specify schema evolution in a merge statement using SQL with `MERGE WITH SCHEMA EVOLUTION INTO` statement. 
-- MAGIC
-- MAGIC     [Schema evolution syntax for merge](https://docs.databricks.com/en/delta/update-schema.html#schema-evolution-syntax-for-merge)
-- MAGIC
-- MAGIC **NOTES**: You can also set the Spark conf `spark.databricks.delta.schema.autoMerge.enabled` to *true* for the current SparkSession. For more information check out the [Enable schema evolution](https://docs.databricks.com/en/delta/update-schema.html#enable-schema-evolution) documentation page.

-- COMMAND ----------

MERGE WITH SCHEMA EVOLUTION INTO main_users_target target  -- Use the MERGE WITH SCHEMA EVOLUTION INTO statement
USING new_users_source source
ON target.id = source.id
WHEN MATCHED AND source.status = 'update' THEN
  UPDATE SET 
    target.email = source.email,
    target.status = source.status
WHEN MATCHED AND source.status = 'delete' THEN
  DELETE
WHEN NOT MATCHED AND source.status = 'new' THEN
  INSERT (id, first_name, email, sign_up_date, status, country)
  VALUES (source.id, source.first_name, source.email, source.sign_up_date, source.status, source.country);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Preview the **main_users_target** table. Notice the following:
-- MAGIC     - The **country** column was added to the table, evolving the table schema.
-- MAGIC     - The three new users were inserted into the table.
-- MAGIC

-- COMMAND ----------

SELECT * 
FROM main_users_target;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 6. View the history of the **main_users_target** table. Notice there is now a version *_4_* reflecting the latest merge.
-- MAGIC

-- COMMAND ----------

DESCRIBE HISTORY main_users_target;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
