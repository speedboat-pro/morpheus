# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream from Multiplex Bronze
# MAGIC ## Bronze to Silver
# MAGIC
# MAGIC This notebook allows you to programmatically generate and trigger an update of a Lakeflow Declarative pipeline that consists of the following notebooks:
# MAGIC
# MAGIC |Lakeflow Declarative pipeline|
# MAGIC |---|
# MAGIC |Auto Load to Bronze|
# MAGIC |[Stream from Multiplex Bronze]($./Pipeline/SDLT 2.2.1 - Stream from Multiplex Bronze)|
# MAGIC
# MAGIC As we continue through the course, you can return to this notebook and use the provided methods to:
# MAGIC - Land a new batch of data
# MAGIC - Trigger a pipeline update
# MAGIC - Process all remaining data
# MAGIC
# MAGIC **NOTE:** Re-running the entire notebook will delete the underlying data files for both the source data and your Lakeflow Declarative pipeline.

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
# MAGIC     - In the drop-down, select **More**.
# MAGIC
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
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
# MAGIC ## Run Setup
# MAGIC Run the following cell to reset and configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Generate the Lakeflow Declarative Pipeline
# MAGIC Run the cell below to auto-generate the pipeline using the provided configuration values. Please navigate to Pipelines under Data Engineering Section.
# MAGIC
# MAGIC **NOTE:** `DeclarativePipelineCreator` is a custom Python class provided in this course to simplify pipeline creation and execution. It leverages the Databricks REST API and SDK behind the scenes. While these APIs are beyond the scope of this course, you can explore the class implementation in the `./Includes/Classroom-Setup-Common` notebook if you're interested.

# COMMAND ----------

demo_pipeline = DeclarativePipelineCreator(
    pipeline_name=f'demo_02_pipeline_{DA.schema_name}', 
    catalog_name='dbacademy',
    schema_name=DA.schema_name,
    root_path_folder_name='Pipeline',
    source_folder_names=[
        'SDLT 2.1.1 - Auto Load to Bronze',
        'SDLT 2.2.1 - Stream from Multiplex Bronze'
        ],
    configuration={"source": DA.paths.stream_source, "lookup_db": DA.lookup_db},
    serverless=True,
    channel='CURRENT',
    delete_pipeine_if_exists = True
)

demo_pipeline.create_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Pipeline Run
# MAGIC
# MAGIC With a pipeline created, you will now run the pipeline. The initial run will take several minutes while a cluster is provisioned. Subsequent runs will be appreciably quicker.
# MAGIC
# MAGIC Explore the DAG - As the pipeline completes, the execution flow is graphed. With each triggered update, all newly arriving data will be processed through your pipeline. Metrics will always be reported for current run.

# COMMAND ----------

demo_pipeline.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 📌 NOTE: Please navigate to the Jobs & Pipelines tab and ensure the pipeline has completed successfully before running further cells.

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
demo_pipeline.start_pipeline()  # Trigger another pipeline update

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
