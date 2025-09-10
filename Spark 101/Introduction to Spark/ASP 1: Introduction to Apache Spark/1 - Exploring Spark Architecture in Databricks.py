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
# MAGIC # 1 - Exploring Spark Architecture in Databricks
# MAGIC
# MAGIC In this demonstration, we'll explore how Spark's architecture components manifest in a Databricks environment and how to monitor them using the various UIs available to us.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Identify key Spark architecture components in a Databricks cluster
# MAGIC - Navigate the Spark UI to monitor application execution
# MAGIC - Understand how Databricks implements Spark's cluster management

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
# MAGIC ## A: Introduction to Notebooks
# MAGIC
# MAGIC We will run all of the demos and exercises for this course in notebooks. Notebooks are interactive documents that combine live code, visualizations, narrative text, and outputs in a single document. In Databricks, notebooks provide a powerful environment for data exploration, analysis, and collaboration.
# MAGIC
# MAGIC Key features of Databricks notebooks:
# MAGIC - Code execution: Run code in multiple languages (Python, SQL, R, Scala)
# MAGIC - Rich text formatting: Support for Markdown to create well-documented analyses
# MAGIC - Cell-based structure: Code and content is organized into executable cells
# MAGIC - Interactive visualizations: Direct plotting and charting of results
# MAGIC - Collaboration: Share and work together on notebooks in real-time
# MAGIC
# MAGIC The default language for a notebook is set when you create it, as indicated by the __Python__ selection in the toolbar above☝️. You can change the default language through the notebook settings. Additionally, you can use magic commands to execute code in different languages within the same notebook:
# MAGIC
# MAGIC - Use `%python` to execute Python code
# MAGIC - Use `%sql` to execute SQL queries
# MAGIC - Use `%r` to execute R code
# MAGIC - Use `%scala` to execute Scala code
# MAGIC
# MAGIC The `%run` magic command is particularly useful - it allows you to execute another notebook within your current notebook, enabling modular code organization and reuse. For example `%run /path/to/another/notebook`.  
# MAGIC
# MAGIC This will execute all the cells in the referenced notebook as if they were part of your current notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. The SparkSession and SparkContext
# MAGIC
# MAGIC The SparkSession is automatically instantiated as `spark` in Databricks notebooks connected to a cluster.  The SparkContext is available via the SparkSession using `spark.sparkContext` or simply `sc`.

# COMMAND ----------

# DBTITLE 1,named cell
spark

# COMMAND ----------

type(spark)

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
application_id = spark.sparkContext.applicationId
print(f"Application ID: {application_id}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Create the first SparkSession
spark1 = SparkSession.builder.appName("FirstSession").getOrCreate()

# Create a new SparkSession using newSession()
spark2 = spark1.newSession()

# Both sessions will have the same SparkContext
print(spark1.sparkContext == spark2.sparkContext) # This will print: True

# COMMAND ----------

spark1

# COMMAND ----------

# MAGIC %scala
# MAGIC spark

# COMMAND ----------

sc

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Understanding Your Databricks Cluster
# MAGIC
# MAGIC 1. Right click on the __Compute__  menu item, select __Open link in new tab__
# MAGIC
# MAGIC 2. Locate your cluster in the __All-purpose compute__ tab, here you can see high level information about the cluster including memory and cores 
# MAGIC
# MAGIC 3. Click on the link to the cluster under the __Name__ column, here you can see more detailed information and access logs and metrics
# MAGIC <!-- 4. Click on the "Spark UI" button under Compute to observe:
# MAGIC     - The DAG visualization showing stages
# MAGIC     - Executor information showing Worker nodes
# MAGIC     - Memory usage across your cluster -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Creating and Monitoring a Spark Job
# MAGIC
# MAGIC Let's create a simple job that will help us visualize the execution flow. Run the below cell to create a spark Job
# MAGIC
# MAGIC **NOTE:** Don't focus too much on the code here. We want to focus on exploring the access logs and metrics.

# COMMAND ----------

# Create a large DataFrame to see parallelization in action
from pyspark.sql.functions import *
import time

# Generate some data
df = spark.range(0, 1000000)
df = df.withColumn("square", col("id") * col("id"))

# # Force multiple stages with a shuffle operation
result = df.groupBy(col("id") % 100).agg(sum("square").alias("sum_squares"))

# COMMAND ----------

# result = df.repartition(2)
# Cache the result to see storage in the UI
result.cache()

# COMMAND ----------

result.rdd.getNumPartitions()

# COMMAND ----------

result.explain()

# COMMAND ----------

# Force the computation with the 'count' action
# print(f"Number of groups: {result.count()}")
result.collect()

# COMMAND ----------

# Force the computation with the 'count' action
print(f"Number of groups: {result.count()}")

# COMMAND ----------

import pandas as pd
pdf = result.toPandas()
type(pdf)

# COMMAND ----------

import pyspark.pandas as ps
psdf = ps.from_pandas(pdf)
type(psdf)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploring the Spark UI
# MAGIC
# MAGIC > NOTE: Right click on the __View__ link (if there are more than one link, click the last one) under the __Spark Jobs__ heading ☝️ and open in a new tab
# MAGIC
# MAGIC Now that we have an active job, let's explore key areas of the Spark UI:
# MAGIC
# MAGIC 1. **Jobs Tab**
# MAGIC    - Shows the DAG for our groupBy operation
# MAGIC    - Multiple stages due to the shuffle operation
# MAGIC    - Click the link inside the description tab of a stage to see task-level details.
# MAGIC
# MAGIC
# MAGIC 2. **Executors Tab**
# MAGIC    - Lists all executors and their resource usage
# MAGIC    - Shows how many cores and memory each executor has
# MAGIC    - Demonstrates the Worker node distribution
# MAGIC
# MAGIC 3. **Storage Tab**
# MAGIC    - Shows cached DataFrames
# MAGIC    - Displays memory usage across executors
# MAGIC
# MAGIC Notice how the architecture we discussed (Driver → Master → Workers → Executors) is reflected in the UI. The Driver coordinates the job, while Executors on Worker nodes perform the actual computations (although in this class all processes reside on a single node).

# COMMAND ----------

# Free up executor memory by unpersisting cached objects
result.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
