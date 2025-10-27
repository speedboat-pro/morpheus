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
# MAGIC # 2.2 Lab - Modularize PySpark Code
# MAGIC
# MAGIC ### Estimated Duration: 15-20 minutes
# MAGIC
# MAGIC By the end of this lab, you will practice analyzing a PySpark script by breaking it down into smaller, reusable functions, and assessing how well their changes improve the code's clarity and ease of maintenance.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. 
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.
# MAGIC
# MAGIC ##### The notebook "2.1 - Modularizing PySpark Code - Required" sets up the catalogs for this course. If you have not run this notebook, the catalogs will not be available.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../Includes/Classroom-Setup-2.2L

# COMMAND ----------

# MAGIC %md
# MAGIC Run the cell below to view your current default catalog and schema. 
# MAGIC
# MAGIC   Confirm the following:
# MAGIC - The default catalog is your unique catalog name (shown above).
# MAGIC - The current schema is **default**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Review the Provided PySpark Code

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the cell below to preview the **samples.nyctaxi.trips** table. Confirm the table exists and view the data.
# MAGIC
# MAGIC     Notice the following:
# MAGIC     - All columns are in lower case
# MAGIC     - **trip_distance** is currently in miles

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM samples.nyctaxi.trips 
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC 2. You have been provided with the following PySpark script that performs the following:
# MAGIC
# MAGIC    a. Reads from the **samples.nyctaxi.trips** table.
# MAGIC
# MAGIC    b. Creates a new column named **trip_distance_km** that converts **trip_distance** to kilometers and rounds it to two decimal places.
# MAGIC
# MAGIC    c. Converts all of the column names to uppercase.
# MAGIC
# MAGIC    d. Saves the DataFrame as a table named **nyc_lab_solution_table** in your specific catalog (`DA.catalog_name`).
# MAGIC
# MAGIC    Run the cell below and confirm that the **nyc_lab_solution_table** table was created with all uppercase column names and the new **trip_distance_km** column.

# COMMAND ----------

## Run the code to view the default catalog the table is being written to.
print(DA.catalog_name)

# COMMAND ----------

# Import necessary libraries
from pyspark.sql import functions as F

# Load the data and create a new column named trip_distance_km
new_taxi = (spark
            .read
            .table("samples.nyctaxi.trips")
            .withColumn("trip_distance_km", F.round(F.col("trip_distance") * 1.60934, 2))
        )


## Upper case all columns
new_taxi = new_taxi.select([F.col(col).alias(col.upper()) for col in new_taxi.columns])


## Save the table to the your catalog
(new_taxi
 .write
 .mode('overwrite')
 .saveAsTable(f'{DA.catalog_name}.default.nyc_lab_solution_table')
)

## View the final table
display(spark.table(f'{DA.catalog_name}.default.nyc_lab_solution_table'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Modularize the PySpark Code

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Your task is to take the provided Spark code from above and break it down into modular functions. Each function should perform a specific part of the task, making it easier to test, reuse, and maintain.
# MAGIC
# MAGIC     There are a variety of ways to solve this problem. For consistency in this example, create the following functions:
# MAGIC
# MAGIC     - `convert_miles_to_km`: Converts a column from miles to kilometers and rounds the result to two decimal places.
# MAGIC
# MAGIC     - `uppercase_column_names`: Converts all column names in the DataFrame to uppercase.
# MAGIC     
# MAGIC     - `load_data`: Reads the table.
# MAGIC
# MAGIC     - `save_to_catalog`: Saves the DataFrame as a new table in your catalog.
# MAGIC
# MAGIC **NOTE:** The `load_data` and `save_to_catalog` functions have already been created for you. 
# MAGIC
# MAGIC **TO DO:** Create the `convert_miles_to_km` and `uppercase_column_names` in the cell below.
# MAGIC
# MAGIC **HINT:** The solution functions can be found in **[./src_lab/lab_functions/transforms.py]($./src_lab/lab_functions/transforms.py)**.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

## load_data has already been created for you
def load_data(table_name):
    return spark.read.table(table_name)


## save_to_catalog has been created for you
def save_to_catalog(df, catalog_name, schema_name, table_name):
    (df
     .write
     .mode('overwrite')
     .saveAsTable(f'{catalog_name}.default.{table_name}')
    )

# COMMAND ----------

## convert_miles_to_km
<FILL-IN>

# COMMAND ----------

## uppercase_column_names
<FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run your functions to obtain the same results as the original PySpark code. The `save_to_catalog` function will name your new table **my_lab_table**. 
# MAGIC
# MAGIC **NOTE:** If you are receiving a schema mismatch error that is because you are trying to overwrite a table you created with a different schema. Delete the table and recreate the table.

# COMMAND ----------

## Load table
df = load_data("samples.nyctaxi.trips")

## Convert miles to km
## TODO - Add your function here

## Upcase column
## TODO - Add your function here

## Save DataFrame as a table in your catalog
save_to_catalog(df, catalog_name = DA.catalog_name, schema_name="default", table_name = "my_lab_table")

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the following cell to test that the original table created in cell 11 (**nyc_km_solution_table**) is the same as your new table created by the functions above (**my_lab_table**). The test uses the PySpark `assertDataFrameEqual` method.
# MAGIC
# MAGIC     If there is an error, it means the original table is not the same as your new table, and you need to fix your functions.

# COMMAND ----------

from pyspark.testing.utils import assertDataFrameEqual

# Read the tables (solution and your created table)
solution_df = spark.read.table(f"{DA.catalog_name}.default.nyc_lab_solution_table")
user_df = spark.read.table(f"{DA.catalog_name}.default.my_lab_table")

# Use assertDataFrameEqual to compare the two tables. Return an error if the tables are different.
assertDataFrameEqual(solution_df, user_df)

print("The tables are identical! Functions were created correctly.")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
