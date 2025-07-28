# Databricks notebook source
my_catalog = dbutils.widgets.get('catalog_name')
target = dbutils.widgets.get('display_target')

set_default_catalog = spark.sql(f'USE CATALOG {my_catalog}')

print(f'Using the {my_catalog} catalog.')
print(f'Deploying as the {target} pipeline.')

# COMMAND ----------

spark.sql(f'''
CREATE OR REPLACE TABLE {my_catalog}.default.health_bronze_demo_01 AS
SELECT 
  *,
  _metadata.file_name as file_name,
  _metadata.file_modification_time as file_modification_time,
  current_timestamp() as load_date
FROM read_files(
  '/Volumes/{my_catalog}/default/health/',
  format => 'csv',
  header => true
)
''')
