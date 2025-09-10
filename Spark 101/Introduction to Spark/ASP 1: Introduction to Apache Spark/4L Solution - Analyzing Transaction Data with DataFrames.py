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
# MAGIC # 4L - Analyzing Transaction Data with DataFrames
# MAGIC
# MAGIC
# MAGIC In this lab, you'll analyze transactions from the Bakehouse dataset using Spark DataFrames. You'll apply the concepts from the lecture to solve real business problems and gain insights from the data.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Reading data into a DataFrame and exploring its contents and structure
# MAGIC - Filtering records and projecting columns from a DataFrame
# MAGIC - Saving a DataFrame to a table

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
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course.
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-Lab

# COMMAND ----------

# MAGIC %md
# MAGIC View your default catalog and schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Initial Setup and Data Loading
# MAGIC
# MAGIC First, let's load our data and examine its structure.

# COMMAND ----------

# Read the Bakehouse transaction data
transactions_df = spark.read.table("samples.bakehouse.sales_transactions")

# Examine the schema and display first few rows
print("DataFrame Schema:")
transactions_df.printSchema()

print("\nSample Data:")
display(transactions_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Data Exploration
# MAGIC
# MAGIC Let's explore the basic characteristics of the dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Total Transactions Count
# MAGIC Get a count of all transactions helps us understand the dataset size.

# COMMAND ----------

total_transactions = transactions_df.count()
print(f"Total number of transactions: {total_transactions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Transactions over $100
# MAGIC Find the transactions over $100, save these to a new DataFrame named `large_transactions_df`.  Display the contents of this new DataFrame.

# COMMAND ----------

from pyspark.sql.functions import col

large_transactions_df = transactions_df.filter(col("totalPrice") > 100)
display(large_transactions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Save the DataFrame to a table
# MAGIC Save the `large_transactions_df` DataFrame to a table called `large_transactions`

# COMMAND ----------

# Save the large transactions DataFrame to a table
large_transactions_df.write.saveAsTable("large_transactions", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Use Spark SQL to count the number of large transactions
# MAGIC Count the total number of large transactions in our `large_transactions` table

# COMMAND ----------

spark.sql("select count(*) as cnt_large_trnsc from large_transactions").display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
