# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

## The classroom-setup-common script will execute with each demonstration and lab.
## Add any code that you would like to run before the classroom starts below.

# COMMAND ----------

## The code below will create the DA object for every demo and lab and display the user's catalog + schema.
DA = DBAcademyHelper()
DA.init()


DA.display_config_values([
                        ('Course Catalog',DA.catalog_name),
                        ('Your Schema',DA.schema_name)
                    ])

# COMMAND ----------

spark.sql(f"create volume if not exists v01 ")
