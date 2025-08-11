# Databricks notebook source
# MAGIC %run ./Classroom-Setup-01-Common

# COMMAND ----------

# DBTITLE 1,Create an empty table to accept streaming inputs
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType, TimestampType

schema = StructType([StructField('window',StructType([StructField('start', TimestampType(), True), 
                                                      StructField('end', TimestampType(), True)]), False), 
                     StructField('city', StringType(), True), 
                     StructField('total_revenue', DoubleType(), True)])

empty_df = spark.createDataFrame([], schema=schema)

empty_df.write.saveAsTable(name="revenue_by_city_by_hour")
print("Created table 'revenue_by_city_by_hour' in 'hive_metastore'...")

