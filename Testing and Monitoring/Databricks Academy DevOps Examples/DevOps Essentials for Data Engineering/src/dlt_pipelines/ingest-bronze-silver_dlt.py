# Databricks notebook source
# MAGIC %md
# MAGIC # Simple Ingest-Bronze-Silver DLT Pipeline Example
# MAGIC This DLT pipeline provides a basic example, demonstrating how to set expectations on tables. It serves as a starting point, and you can easily add more tables or expectations based on your specific needs. For simplicity, this example will focus on the essentials.
# MAGIC
# MAGIC Please check out the following resources for more information.
# MAGIC
# MAGIC - [Manage data quality with pipeline expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html#manage-data-quality-with-pipeline-expectations)
# MAGIC
# MAGIC - [Expectation recommendations and advanced patterns](https://docs.databricks.com/en/delta-live-tables/expectation-patterns.html#expectation-recommendations-and-advanced-patterns)
# MAGIC
# MAGIC - [Applying software development & DevOps best practices to Delta Live Table pipelines](https://www.databricks.com/blog/applying-software-development-devops-best-practices-delta-live-table-pipelines)

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

## Add previous folder to python path to import our helpers package
import sys
sys.path.append('../.')
from helpers import project_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtain Configuration Variables
# MAGIC This raw source data path and catalog will dynamically be set using the configuration variable set in the DLT pipeline for each environment: **development**, **stage**, or **production**.
# MAGIC
# MAGIC - **development** – Reads the dev CSV file from **your_unique_catalog_1_dev.default.health.dev_health.csv**.
# MAGIC
# MAGIC - **stage** – Reads the stage CSV file from **your_unique_catalog_2_stage.default.health.stage_health.csv**.
# MAGIC
# MAGIC - **production** – Reads CSV files in the production volume **your_unique_catalog_2_stage.default.health/*.csv**.
# MAGIC

# COMMAND ----------

## Store the target configuration environment in the variable targert
target = spark.conf.get("target")

## Store the target raw data configuration in the variable raw_data_path
raw_data_path = spark.conf.get("raw_data_path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Ingest CSV Files -> health_bronze

# COMMAND ----------

## The health_bronze table is created using the value based on the target variable.
## development - import the DEV CSV
## stage - import the STAGE CSV
## production - import the daily CSV files from our production source volume


## Simple expectations for the bronze table
valid_rows = {
        "not_null_pii": "PII IS NOT NULL", 
        "valid_date": "date IS NOT NULL"
    }

@dlt.table(
    comment = "This table will be used to ingest the raw CSV files and add metadata columns to the bronze table.",
    table_properties = {"quality": "bronze"}
)

## Fail process if expectation is not met
@dlt.expect_all_or_fail(valid_rows)

def health_bronze():
    return (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header","true")
        .schema(project_functions.get_health_csv_schema())   ## Use the custom schema we created
        .load(raw_data_path)   ## <--------------- Path is based on the configuration parameter set (DEV, STAGE, PROD)
        .select(
            "*",
            "_metadata.file_name",
            "_metadata.file_modification_time",
            F.current_timestamp().alias("processing_time")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Silver Table

# COMMAND ----------

@dlt.table(
    comment = "This table will create, drop and categorize columns from the bronze table.",
    table_properties = {"quality": "bronze"}
)
def health_silver():
    return (
        dlt
        .read_stream("health_bronze")
        .withColumn("HighCholest_Group", project_functions.high_cholest_map("HighCholest"))  # UDF - highcholest_map 
        .withColumn("Age_Group", project_functions.group_ages_map("Age"))                   # UDF - group_ages_map Age
        .drop("file_name", "file_modification_time", "processing_time")         # Drop unnecessary metadata columns
    )
