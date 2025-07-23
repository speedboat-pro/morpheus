# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Quality Enforcement
# MAGIC
# MAGIC This notebook allows you to programmatically generate and trigger an update of a DLT pipeline that consists of the following notebooks:
# MAGIC
# MAGIC |DLT Pipeline|
# MAGIC |---|
# MAGIC |Auto Load to Bronze|
# MAGIC |Stream from Multiplex Bronze|
# MAGIC |[Quality Enforcement]($./Pipeline/ADE 2.3.1 - Data Quality Enforcement)|
# MAGIC
# MAGIC As we continue through the course, you can return to this notebook and use the provided methods to:
# MAGIC - Land a new batch of data
# MAGIC - Trigger a pipeline update
# MAGIC - Process all remaining data
# MAGIC
# MAGIC **NOTE:** Re-running the entire notebook will delete the underlying data files for both the source data and your DLT Pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Setup
# MAGIC Run the following cell to reset and configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.3

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Generate DLT Pipeline
# MAGIC Run the cell below to auto-generate your DLT pipeline using the provided configuration values.
# MAGIC
# MAGIC Once the pipeline is ready, a link will be provided to navigate you to your auto-generated pipeline in the Pipeline UI.

# COMMAND ----------

DA.generate_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Pipeline Run
# MAGIC
# MAGIC With a pipeline created, you will now run the pipeline. The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.
# MAGIC
# MAGIC Explore the DAG - As the pipeline completes, the execution flow is graphed. With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.

# COMMAND ----------

DA.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land New Data
# MAGIC
# MAGIC Run the cell below to land more data in the source directory, then manually trigger another pipeline update using the UI or the cell above.

# COMMAND ----------

DA.daily_stream.load()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process All Remaining Data
# MAGIC To continuously load all remaining batches of data to the source directory, call the same load method above with the **`continuous`** parameter set to **`True`**.
# MAGIC
# MAGIC Trigger another update to process the remaining data.

# COMMAND ----------

DA.daily_stream.load(continuous=True)  # Load all remaining batches of data
DA.start_pipeline()  # Trigger another pipeline update

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
