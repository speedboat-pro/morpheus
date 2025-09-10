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
# MAGIC # 2 - Reading and Writing Data with DataFrames
# MAGIC
# MAGIC This demonstration will introduce you to Spark DataFrames by reading data from an external source into a DataFrame and then writing the DataFrame out to another location.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Read external data into a DataFrame
# MAGIC - Create and inspect DataFrame schemas
# MAGIC - Write the contents of a DataFrame to a specified location
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

# DBTITLE 1,Named Cell
# MAGIC %run ./Includes/Classroom-Setup-Common

# COMMAND ----------

# MAGIC %md
# MAGIC Viewing the current catalog and schema

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_catalog(),current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Reading Data 
# MAGIC
# MAGIC ### 1. Schema Inference
# MAGIC
# MAGIC Let's start by creating a DataFrame by reading a directory of CSV files.

# COMMAND ----------

# MAGIC %fs ls /Volumes/dbacademy_retail/v01/source_files

# COMMAND ----------

# MAGIC %fs head dbfs:/Volumes/dbacademy_retail/v01/source_files/sales.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files('dbfs:/Volumes/dbacademy_retail/v01/source_files/sales.csv')

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`dbfs:/Volumes/dbacademy_retail/v01/source_files/sales.csv`

# COMMAND ----------

# Read the customers.csv file with schema inference
customers_df = spark.read.format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("/Volumes/dbacademy_retail/v01/source_files")

# COMMAND ----------

# Show the inferred schema for customers_df
customers_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC > __NOTE:__ If the dataset has a header row, column names are inferred. Without a header, columns are named **_c0**, **_c1**, etc., and must be renamed manually.

# COMMAND ----------

# Alternatively we can use below command to print schema in linear format
print(customers_df.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## `display()` function
# MAGIC
# MAGIC The `display()` function is available in Databricks runtimes to be used within notebooks.  
# MAGIC
# MAGIC `display()` represents a Spark action, which returns results for a DataFrame in a formatted tabular result window.  As this is intended for visual inspection only, results are limited to 10,000 rows or 2 MB, whichever is less. Additional features of the `display()` function include:
# MAGIC
# MAGIC - Ability to sort or filter data in the result set
# MAGIC - Ability to download result data to a CSV file
# MAGIC - Ability to profile data using the __+__ -> __Data Profile__ option
# MAGIC - Ability to visualize data using the __+__ -> __Visualization__ option
# MAGIC

# COMMAND ----------

# Using Display function to preview the data in the DataFrame
display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. Explicitly Defining a Schema
# MAGIC
# MAGIC Let's load the same dataset into a DataFrame, this time we will explicitly define the schema.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# or ...
from pyspark.sql.types import *

# Define explicit schema using StructType
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("tax_id", StringType(), True),
    StructField("tax_code", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("city", StringType(), True),
    StructField("postcode", StringType(), True),
    StructField("street", StringType(), True),
    StructField("number", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("region", StringType(), True),
    StructField("district", StringType(), True),
    StructField("lon", DoubleType(), True),
    StructField("lat", DoubleType(), True),
    StructField("ship_to_address", StringType(), True),
    StructField("valid_from", IntegerType(), True),
    StructField("valid_to", IntegerType(), True),
    StructField("units_purchased", IntegerType(), True),
    StructField("loyalty_segment", IntegerType(), True)])

# Read the customers.csv file with explicit StructType schema
customers_structtype_df = spark.read.format("csv") \
  .option("header", "true") \
  .schema(customer_schema) \
  .load("/Volumes/dbacademy_retail/v01/source_files/customers.csv")

# COMMAND ----------

# Examine the explicit schema
customers_structtype_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Using a DDL Schema
# MAGIC Let's define the schema now using a DDL schema for readability.

# COMMAND ----------

ddl_schema = """
  customer_id INT NOT NULL,
  tax_id STRING,
  tax_code STRING,
  customer_name STRING,
  state STRING,
  city STRING,
  postcode STRING,
  street STRING,
  number STRING,
  unit STRING,
  region STRING,
  district STRING,
  lon DOUBLE,
  lat DOUBLE,
  ship_to_address STRING,
  valid_from INT,
  valid_to INT,
  units_purchased INT,
  loyalty_segment INT
"""

# COMMAND ----------

customers_ddl_df = spark.read.format("csv") \
  .option("header", "true") \
  .schema(ddl_schema) \
  .load("/Volumes/dbacademy_retail/v01/source_files/customers.csv")

# COMMAND ----------

customers_ddl_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Writing Data From DataFrames
# MAGIC
# MAGIC Now let's write out the contents of a DataFrame to different output locations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the contents of a DataFrame to a File System
# MAGIC We can write the contents of our DataFrame to a filesystem in various formats using the `write` and `save` methods as shown here.

# COMMAND ----------

# Define an output path
parquet_output_volume_path = f"/Volumes/{DA.catalog_name}/{DA.schema_name}/v01/customers_parquet"

# COMMAND ----------

parquet_output_volume_path

# COMMAND ----------

customers_ddl_df.rdd.getNumPartitions()

# COMMAND ----------

# Write the DataFrame out as Parquet files to a directory
customers_ddl_df.write.format("parquet") \
  .mode("overwrite") \
  .save(parquet_output_volume_path)

# COMMAND ----------

customers_ddl_df = customers_ddl_df.repartition(4)

# COMMAND ----------

# Show the files in the directory
display(dbutils.fs.ls(parquet_output_volume_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the contents of a DataFrame to a Table
# MAGIC We can also save our DataFrame to a table (defined in a catalog and schema in Unity Catalog).

# COMMAND ----------

# Writing our DataFrame to a new table (this is an action)
customers_ddl_df.write.saveAsTable("customers_ddl_df_table")

# COMMAND ----------

# Alternatively you can use the writeTo method which invokes the DataFrameWriterV2 
customers_ddl_df.writeTo(                              
    f"{DA.catalog_name}.{DA.schema_name}.customers_ddl_df_table"
).createOrReplace()
# you have options to partition the table, as well as append, overwrite or createOrReplace

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read back the data in the table
# MAGIC SELECT * FROM customers_ddl_df_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL  customers_ddl_df_table

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL customers_ddl_df_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_name, min(customer_id), max(customer_id)
# MAGIC FROM customers_ddl_df_table 
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers_ddl_df_table
# MAGIC PARTITIONED BY (state)
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM customers_ddl_df_table 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE customers_ddl_df_table ZORDER BY customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE customers_ddl_df_table
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM customers_ddl_df_table 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE customers_ddl_df_table CLUSTER BY AUTO

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC 1. **Reading Data into DataFrames**:
# MAGIC    - Schema inference can be used for CSV files
# MAGIC    - The schema can also be explicitly defined (preferred) using StructType definitions or using a DDL schema
# MAGIC    
# MAGIC 2. **Writing Data from DataFrames**:
# MAGIC    - DataFrames can be written out to a distributed filesystem (like a Unity Catalog volume)
# MAGIC    - DataFrames can also be written out to tables in Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
