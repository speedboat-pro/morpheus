# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# DBTITLE 0,--i18n-610f09e1-5f74-4b55-be94-2879bd22bbe9
# MAGIC %md
# MAGIC # Aggregations by Time Windows with Watermarks
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Build streaming DataFrames with time window aggregates with watermark
# MAGIC 1. Write streaming query results to Delta table using `update` mode and `forEachBatch()`
# MAGIC 1. Monitor the streaming query
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>
# MAGIC - <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html" target="_blank">foreachbatch</a>
# MAGIC - <a href="https://docs.databricks.com/en/structured-streaming/delta-lake.html#upsert-from-streaming-queries-using-foreachbatch&language-python" target="_blank">update mode with foreachbatch</a>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.4

# COMMAND ----------

# DBTITLE 0,--i18n-e06055f0-b40e-4997-b656-bf8cf94156a0
# MAGIC %md
# MAGIC
# MAGIC ### Build Streaming DataFrames
# MAGIC
# MAGIC Obtain an initial streaming DataFrame from a Delta-format file source.

# COMMAND ----------

from pyspark.sql.functions import window, sum, col
from pyspark.sql.types import TimestampType

windowed_df = (spark.readStream
                    .load(DA.paths.events)
                    .withColumn("event_timestamp", col("event_timestamp").cast(TimestampType()))
                    # filter out zero revenue events
                    .filter("ecommerce.purchase_revenue_in_usd IS NOT NULL AND ecommerce.purchase_revenue_in_usd != 0")
                    .withWatermark(eventTime="event_timestamp", delayThreshold="90 minutes")
                    # group by city by hour and add up revenues by city within 60 minute tumbling time windows
                    .groupBy(window(timeColumn="event_timestamp", windowDuration="60 minutes"), "geo.city")
                    .agg(sum("ecommerce.purchase_revenue_in_usd").alias("total_revenue"))
)

# COMMAND ----------

display(windowed_df, streamName = "display_revenue_by_city_by_hour")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Two approaches to write streaming results to a Delta Table
# MAGIC
# MAGIC We have a stream of aggregated data available to us. We will see the different ways we can persist these results in a Delta Table.

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Option 1: Write results `toTable` with the `outputMode = Complete`
# MAGIC
# MAGIC In the below example, the sink table is overwritten with the refreshed results table on each trigger
# MAGIC

# COMMAND ----------

checkpoint_path = f"{DA.paths.working_dir}/query_revenue_by_city_by_hour_complete"

# Write the output of a streaming aggregation query into a Delta table
windowed_query = (windowed_df.writeStream
                  .queryName("query_revenue_by_city_by_hour")
                  .option("checkpointLocation", checkpoint_path)
                  .trigger(availableNow=True)
                  .toTable("revenue_by_city_by_hour_complete", outputMode="complete")
                )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from revenue_by_city_by_hour_complete

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history revenue_by_city_by_hour_complete

# COMMAND ----------

# MAGIC %md
# MAGIC ### Note: Streaming Aggregations, Watermarks and 'Append' output mode
# MAGIC
# MAGIC Streaming aggregation queries will **support append mode ONLY when there is a watermark** applied to the source. If a watermark is not specified, the output mode MUST be 'Complete'.
# MAGIC
# MAGIC Try it out for yourself: Try running the above query without watermarks and in append mode and see what combinations are supported. 
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-5a10eead-7c3b-41e6-bcbe-0d85095de1d7
# MAGIC %md
# MAGIC
# MAGIC ### Option 2: Write streaming query results in `update` mode with `forEachBatch()` processing
# MAGIC
# MAGIC Take the final streaming DataFrame (our result table) and write it to a Delta Table sink in "update" mode. This approach gives much greater control to the developer when it comes to updating the sink, albeit with greater complexity.
# MAGIC
# MAGIC NOTE: The syntax for Writing streaming results to a Delta table or dataset in "update" mode is a little different. It requires use of the `MERGE` command within a `forEachBatch()` function call. This also requires the target table to be pre-created.
# MAGIC

# COMMAND ----------

# Function to upsert microBatchOutputDF into Delta table using merge

def upsertToDelta(microBatchDF, batchId):
  # Set the dataframe to view name
  microBatchDF.createOrReplaceTempView("updates")
  # IMP: You have to use the SparkSession that has been used to define the `updates` dataframe

  # In Databricks Runtime 10.5 and below, you must use the following:
  # microBatchOutputDF._jdf.sparkSession().sql("""
  microBatchDF.sparkSession.sql("""
    MERGE INTO revenue_by_city_by_hour t
    USING updates s
    ON t.window.start = s.window.start AND t.window.end = s.window.end AND t.city = s.city
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """)


# COMMAND ----------

checkpoint_path = f"{DA.paths.working_dir}/query_revenue_by_city_by_hour_update"

# Write the output of a streaming aggregation query into Delta table as updates
windowed_query = (windowed_df.writeStream
                  .foreachBatch(upsertToDelta)
                  # .outputMode("update")
                  .queryName("query_revenue_by_city_by_hour_update")
                  .option("checkpointLocation", checkpoint_path)
                  .trigger(availableNow=True)
                  .start()
                )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from revenue_by_city_by_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history revenue_by_city_by_hour

# COMMAND ----------

for s in spark.streams.active:
  print(s.name)
  s.stop()

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
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
