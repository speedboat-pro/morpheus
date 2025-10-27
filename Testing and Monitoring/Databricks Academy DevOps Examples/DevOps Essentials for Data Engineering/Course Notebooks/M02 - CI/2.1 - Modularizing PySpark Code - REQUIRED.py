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
# MAGIC # 2.1 - Modularizing PySpark Code (REQUIRED)
# MAGIC
# MAGIC #### PLEASE READ: This notebook will set up your lab environment for the remainder of this course. Running this notebook is REQUIRED. If you do not run this notebook in your lab you will not have the necessary data and catalogs to complete the rest of this course.
# MAGIC
# MAGIC
# MAGIC In this demonstration, we will transform traditional Apache Spark ETL code into a more modular, maintainable, and testable structure. 
# MAGIC
# MAGIC By breaking the code into smaller, reusable functions and components, this approach gives you the ability to test individual units of functionality, streamline debugging, and improve long-term maintenance. This process is particularly valuable in a DevOps CI/CD pipeline, where automated testing and continuous integration require clean, modular code for efficient deployment and error tracking.
# MAGIC
# MAGIC ## Objectives
# MAGIC
# MAGIC - Refactor traditional Spark code into smaller, modular parts for easier testing and maintenance.
# MAGIC - Compare traditional and modular Spark code to see how modularization helps with testing and debugging.

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
# MAGIC #### All users must execute the following course setup script to setup their course environment. If you do not complete this setup, the remainder of the course will not run properly.
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. 
# MAGIC
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../Includes/Classroom-Setup-2.1-REQUIRED

# COMMAND ----------

# MAGIC %md
# MAGIC Run the following cell to view the value of the `DA.catalog_name` variable. Notice that it is referencing your main catalog: **your_user_name**.

# COMMAND ----------

print(DA.catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Build a Pipeline Using Traditional Spark Code
# MAGIC Let's quickly view a typical code structure to ingest data and create a bronze, silver, and gold table. 
# MAGIC
# MAGIC **NOTE:** When starting a project, you often work within a notebook to explore and interact with your data. The code here serves as a simple example. Our primary focus will be on the process rather than the code itself.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Preview the **dev_health.csv** file again within the **health** volume of your **your_user_name.default** catalog. 
# MAGIC
# MAGIC       Run the code and view the results. Notice that:
# MAGIC    - This dev CSV file contains 7,500 rows of data and the header. This data is a subset of our production data that we will use to develop our project.
# MAGIC    - The dev data masks the **PII** column to avoid data PII issues during development. Depending on the sensitivity of your data, it can be protected in various ways. In this course, we simply mask the column.

# COMMAND ----------

spark.sql(f'''
SELECT * 
FROM text.`/Volumes/{DA.catalog_name}/default/health`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Next, let's ingest the CSV file into a bronze table within the notebook and develop the general flow to the project. The following code is part of the development process and will begin the process of creating our desired pipeline. The code will:
# MAGIC
# MAGIC    a. Ingest the CSV file using a defined schema and save as a bronze table with metadata columns.
# MAGIC
# MAGIC    b. Clean the bronze table and create the silver table.
# MAGIC
# MAGIC     - Categorize the numeric values in the **HighCholest** and **Age** columns into groups.
# MAGIC
# MAGIC     - Drop the metadata columns from the bronze table.
# MAGIC
# MAGIC     - Save the dataframe as the **health_silver_dev** table in our dev catalog.
# MAGIC
# MAGIC    c. Create the final gold table for downstream for dashboards or visualizations
# MAGIC
# MAGIC **NOTE:** We will be migrating this ETL pipeline to a Delta Live Tables(DLT) pipeline later in the project. For the purpose of exploration and development we will read the CSV files with a simple batch read.
# MAGIC
# MAGIC ### We will not go into depth on the PySpark and SQL code below, as it should be familiar to you.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import when, col, current_timestamp

##
## CSV -> Bronze
##

# Set the path to the CSV file in the DEV volume
csv_path = f'/Volumes/{DA.catalog_name}/default/health'


# Define the schema for the CSV file
health_csv_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("PII", StringType(), True),
    StructField("date", DateType(), True),
    StructField("HighCholest", IntegerType(), True),
    StructField("HighBP", DoubleType(), True),
    StructField("BMI", DoubleType(), True),
    StructField("Age", DoubleType(), True),
    StructField("Education", DoubleType(), True),
    StructField("income", IntegerType(), True)
])


# Ingest the CSV file and add metadata columns for the ingested data
health_raw = (
    spark
    .read
    .format("csv") 
    .option("header", "true")             # Use the header row for column names
    .schema(health_csv_schema)            # Apply the defined schema
    .load(csv_path)                       # Load the CSV data
    .select(
        "*",
        "_metadata.file_name",                        # Include file name from metadata
        "_metadata.file_modification_time",           # Include file modification timestamp
        current_timestamp().alias("processing_time")  # Add a processing time column
    )
)


# Save the ingested data as a Bronze table in Delta format
(health_raw
 .write
 .format("delta")
 .mode('overwrite')  # Overwrite existing data
 .saveAsTable(f"{DA.catalog_name}.default.health_bronze_dev")
)

health_bronze = spark.table(f'{DA.catalog_name}.default.health_bronze_dev')


##
## Bronze -> Silver
##

health_silver = (
    health_bronze
    # Create a new column to categorize the HighCholest column
    .withColumn(
        "HighCholest_Group", 
        when(col("HighCholest") == 0, 'Normal')
        .when(col("HighCholest") == 1, 'Above Average')
        .when(col("HighCholest") == 2, 'High')
        .otherwise('Unknown')
    )
    # Create a new column to categorize the Age_Group column
    .withColumn(
        "Age_Group", 
        when(col("Age") <= 9, "0-9")
        .when((col("Age") >= 10) & (col("Age") <= 19), "10-19")
        .when((col("Age") >= 20) & (col("Age") <= 29), "20-29")
        .when((col("Age") >= 30) & (col("Age") <= 39), "30-39")
        .when((col("Age") >= 40) & (col("Age") <= 49), "40-49")
        .when(col("Age") >= 50, "50+")
        .otherwise('Unknown')
    )
    # Drop unnecessary columns (e.g., metadata columns)
    .drop("file_name", "file_modification_time", "processing_time")
)

# Save the transformed data as a Silver table
(health_silver
 .write
 .format("delta")
 .mode("overwrite")  # Overwrite any existing data
 .saveAsTable(f"{DA.catalog_name}.default.health_silver_dev")
)



##
## Silver - Gold
##
chol_age_agg = spark.sql(f'''
    CREATE OR REPLACE TABLE {DA.catalog_name}.default.chol_age_agg_dev AS
    SELECT 
        HighCholest_Group, 
        Age_Group, 
        count(*) as Total
    FROM {DA.catalog_name}.default.health_silver_dev
    GROUP BY HighCholest_Group, Age_Group
''')

## Display the final gold table
spark.table(f'{DA.catalog_name}.default.chol_age_agg_dev').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Modularize the PySpark Code
# MAGIC Modularizing code improves maintainability, reusability, and collaboration by breaking workflows into smaller, manageable units. This approach reduces redundancy, simplifies debugging, and allows for easier scaling. It also enables better team collaboration, as different team members can work on separate modules without conflicts, making the overall codebase more flexible and easier to manage.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. We will modularize the code from above into the following functions:
# MAGIC
# MAGIC     a. `get_health_csv_schema`: Defines and returns the schema for the CSV files.
# MAGIC
# MAGIC     b. `read_health_data`: Reads the CSV file into a DataFrame with metadata.
# MAGIC
# MAGIC     c. `high_cholest_map` and `group_ages_map`: Converts the numeric values to categorical. 
# MAGIC
# MAGIC     d. `save_df_to_delta`: Saves the transformed data to a Delta table.
# MAGIC
# MAGIC     e. `get_cholest_age_agg`: Aggregates the data for the gold table.
# MAGIC
# MAGIC
# MAGIC **NOTE:** We will quickly go over the functions below. Focus on the process of modularizing code, not the code itself. PySpark is a prerequisite for the course.
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import when, col, current_timestamp


##
## Ingest Cloud to Bronze
##

# a. Function to define the schema for the health data CSV
def get_health_csv_schema():
    return StructType([
        StructField("ID", IntegerType(), True),
        StructField("PII", StringType(), True),
        StructField("date", DateType(), True),
        StructField("HighCholest", IntegerType(), True),
        StructField("HighBP", DoubleType(), True),
        StructField("BMI", DoubleType(), True),
        StructField("Age", DoubleType(), True),
        StructField("Education", DoubleType(), True),
        StructField("income", IntegerType(), True)
    ])


# b. Function to read the CSV data into a DataFrame and add metadata columns
def read_health_data(csv_path, schema):
    return (
        spark
        .read
        .format("csv")
        .option("header", "true")  # Use the header row for column names
        .schema(schema)            # Apply the defined schema
        .load(csv_path)            # Load the CSV data
        .select(
            "*",
            "_metadata.file_name",                        # Include file name from metadata
            "_metadata.file_modification_time",           # Include file modification timestamp
            current_timestamp().alias("processing_time")  # Add a processing time column
        )
    )


##
## Mapping Functions for Data Transformation
##

# c. Map the 'HighCholest' column to categories
def high_cholest_map(col_name):
    return (
        when(col(col_name) == 0, 'Normal')
        .when(col(col_name) == 1, 'Above Average')
        .when(col(col_name) == 2, 'High')
        .otherwise('Unknown')
    )


# d. Map the 'Age' column to age groups
def group_ages_map(col_name):
    return (
        when(col(col_name) <= 9, "0-9")
        .when((col(col_name) >= 10) & (col(col_name) <= 19), "10-19")
        .when((col(col_name) >= 20) & (col(col_name) <= 29), "20-29")
        .when((col(col_name) >= 30) & (col(col_name) <= 39), "30-39")
        .when((col(col_name) >= 40) & (col(col_name) <= 49), "40-49")
        .when(col(col_name) >= 50, "50+")
        .otherwise('Unknown')
    )


##
## Save a DataFrame to Delta Table
##

# e. Function to save the DataFrame to a Delta table
def save_df_to_delta(dataframe, uc_table, mode):
    (dataframe
     .write
     .format("delta")
     .mode(mode)             # Specify the save mode (e.g., 'overwrite', 'append')
     .saveAsTable(uc_table)  # Save the DataFrame as a table
    )


##
## Gold Aggregation
##

# f. Function to create a Gold-level table with aggregated counts
def get_cholest_age_agg(catalog, schema, table_name):
    query = f'''
        CREATE OR REPLACE TABLE {catalog}.{schema}.{table_name} AS
        SELECT 
            HighCholest_Group, 
            Age_Group, 
            count(*) as Total
        FROM {catalog}.{schema}.health_silver_dev
        GROUP BY HighCholest_Group, Age_Group
    '''
    return spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Use the functions from above to build your ingest, bronze, silver, and gold pipeline.

# COMMAND ----------

##
## CSV to Bronze
##

# Read the health CSV data into a DataFrame and save it to the Bronze table
health_csv_df = read_health_data(
    csv_path = f"/Volumes/{DA.catalog_name}/default/health", 
    schema = get_health_csv_schema()
)

# Save the raw data as a Bronze table in Delta format
save_df_to_delta(health_csv_df, f"{DA.catalog_name}.default.health_bronze_dev", mode="overwrite")


##
## Bronze to Silver
##

# Transform the data by adding new columns and cleaning up metadata
health_bronze = spark.table(f'{DA.catalog_name}.default.health_bronze_dev')

silver_df = (
    health_bronze
    .withColumn("HighCholest_Group", high_cholest_map("HighCholest"))    # Categorize HighCholest
    .withColumn("Age_Group", group_ages_map("Age"))                     # Categorize Age
    .drop("file_name", "file_modification_time", "processing_time")     # Drop unnecessary metadata columns
)

# Save the transformed data as a Silver table in Delta format
save_df_to_delta(silver_df, f"{DA.catalog_name}.default.health_silver_dev", mode="overwrite")


##
## Gold Table
##

# Aggregate the data at the Gold level by cholesterol and age groups
get_cholest_age_agg(catalog = DA.catalog_name, schema = 'default', table_name ='chol_age_agg_dev')

# COMMAND ----------

# MAGIC %md
# MAGIC 3. View the **health_bronze_dev** table. Notice that it returns the expected 7,500 rows and 12 columns, including the metadata columns.

# COMMAND ----------

spark.table(f'{DA.catalog_name}.default.health_bronze_dev').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. View the **health_silver_dev** table. Notice that it returns the expected 7,500 rows and 11 columns, including the new calculated columns (the new **HighCholest_Group** and **Age_Group** columns), while the metadata columns have been dropped.

# COMMAND ----------

spark.table(f'{DA.catalog_name}.default.health_silver_dev').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 5. View the **chol_age_agg_dev** table and confirm that the results aggregate the count of **HighCholest_Group** and **Age_Group**.

# COMMAND ----------

spark.table(f'{DA.catalog_name}.default.chol_age_agg_dev').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Delete the tables we created during our initial development to clean up our catalog.

# COMMAND ----------

def delete_demo2_tables(catalog):
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.default.health_bronze_dev")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.default.health_silver_dev")
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.default.chol_age_agg_dev")

delete_demo2_tables(DA.catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC In this notebook, we are working in the development environment, where we are building our data pipeline using the dev data.Once the general flow is complete and the code has been modularized, we can begin performing unit tests as part of our continuous integration process.
# MAGIC
# MAGIC Modularizing code is essential for efficient and effective unit testing within CI/CD pipelines. It enables faster and more reliable testing, improves code quality, and supports better maintainability—key factors for successful, automated deployment. By breaking down complex systems into smaller, manageable components, developers can write more focused, effective tests that ensure high-quality applications and smooth continuous delivery.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
