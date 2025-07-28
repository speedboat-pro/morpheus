# Databricks notebook source
my_catalog = dbutils.widgets.get('catalog_name')
target = dbutils.widgets.get('display_target')

set_default_catalog = spark.sql(f'USE CATALOG {my_catalog}')

print(f'Using the {my_catalog} catalog.')
print(f'Deploying as the {target} pipeline.')

# COMMAND ----------

spark.sql(f'''
CREATE OR REPLACE TABLE {my_catalog}.default.health_silver_demo03 AS
SELECT * EXCEPT (file_name, file_modification_time, load_date)
FROM {my_catalog}.default.health_bronze_demo03
''')
