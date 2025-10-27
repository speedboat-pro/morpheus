# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

## Display the course catalog and schema name for the user.
DA.display_config_values(
  [
    ('Lab catalog reference: DA.catalog_name', DA.catalog_name)
  ]
)

set_default_catalog = spark.sql(f'USE CATALOG {DA.catalog_name}')
