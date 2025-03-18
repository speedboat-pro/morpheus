# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-610f09e1-5f74-4b55-be94-2879bd22bbe9
# MAGIC %md
# MAGIC # Stateless Streaming Query
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Build a streaming DataFrame running a stateless transformation
# MAGIC 1. Display streaming query results
# MAGIC 1. Write streaming query results to delta table in hive metastore
# MAGIC 1. Monitor the streaming query
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prototyping in Batch Mode
# MAGIC
# MAGIC Explore the dataset and test out transformation logic using batch dataframes.

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count

batch_df = (spark.read
              .load(DA.paths.events)
              .filter(col("traffic_source") == "email")
              .withColumn("mobile", col("device").isin(["iOS", "Android"]))
              .select("user_id", "event_timestamp", "mobile")
           )

print(batch_df.isStreaming)

display(batch_df)



# COMMAND ----------

# DBTITLE 0,--i18n-e06055f0-b40e-4997-b656-bf8cf94156a0
# MAGIC %md
# MAGIC
# MAGIC ### Building a streaming Dataframe
# MAGIC
# MAGIC Switching from batch to stream is easy! 
# MAGIC
# MAGIC Change the code from `spark.read` to `spark.readStream` - everything else, including the transformation logic remain unchanged.

# COMMAND ----------

from pyspark.sql.functions import col, approx_count_distinct, count
 
streaming_df = (spark.readStream.load(DA.paths.events)
                      .filter(col("traffic_source") == "email")
                      .withColumn("mobile", col("device").isin(["iOS", "Android"]))
                      .select("user_id", "event_timestamp", "mobile")
                   )

print(streaming_df.isStreaming)

display(streaming_df, streamName = "display_user_devices")

# COMMAND ----------

# DBTITLE 0,--i18n-5a10eead-7c3b-41e6-bcbe-0d85095de1d7
# MAGIC %md
# MAGIC
# MAGIC ### Write streaming query results
# MAGIC
# MAGIC Take the final streaming DataFrame (our result table) and write it to a Delta Table sink in `append` mode. In this labs setup, the table will be created in the `hive_metastore`. In the real world, we strongly recommend all new Delta tables to be created and managed in Unity Catalog.
# MAGIC
# MAGIC **NOTE:** `append` mode is the default mode when writing stateless queries to sink.
# MAGIC

# COMMAND ----------

checkpoint_path = f"{DA.paths.working_dir}/email_traffic"

devices_query = (streaming_df
                  .writeStream
                  .outputMode("append")
                  .format("delta")
                  .queryName("email_traffic")
                  .trigger(processingTime="1 second")
                  .option("checkpointLocation", checkpoint_path)
                  .toTable("email_traffic_only")
                )

# COMMAND ----------

DA.block_until_stream_is_ready(devices_query)

# COMMAND ----------

# DBTITLE 0,--i18n-f6ec862b-9ee2-4731-beb9-122c588b165c
# MAGIC %md
# MAGIC ### Monitor streaming query
# MAGIC
# MAGIC Use the streaming query handle to monitor and control it.

# COMMAND ----------

devices_query.name

# COMMAND ----------

# DBTITLE 0,--i18n-2641f7db-c1d9-426f-8b86-587013f738e1
# MAGIC %md
# MAGIC
# MAGIC Query status output

# COMMAND ----------

devices_query.status

# COMMAND ----------

# DBTITLE 0,--i18n-bd250fc5-2e56-48e6-b705-57616414a3f7
# MAGIC %md
# MAGIC [lastProgress](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.lastProgress.html) gives us metrics from the previous query

# COMMAND ----------

devices_query.lastProgress

# COMMAND ----------

import time
# Run for 10 more seconds
time.sleep(10) 

devices_query.stop()

# COMMAND ----------

devices_query.awaitTermination()

# COMMAND ----------

# DBTITLE 0,--i18n-4032abf5-857d-4fcf-8321-53aa97663279
# MAGIC %md
# MAGIC [awaitTermination](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.streaming.StreamingQuery.awaitTermination.html) blocks the current thread until the streaming query is terminated. In our course notebooks we use it in case you use "Run All" to run the notebook to prevent subsequent command cells from executing until the streaming query has fully terminated.
# MAGIC
# MAGIC For stand-alone structured streaming applications, this is used to prevent the main thread from terminating while the streaming query is still executing.

# COMMAND ----------

# DBTITLE 0,--i18n-cc03901d-02a5-48d7-bd4e-da7494350339
# MAGIC %md
# MAGIC
# MAGIC ### Classroom Cleanup
# MAGIC Run the cell below to clean up resources.

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
