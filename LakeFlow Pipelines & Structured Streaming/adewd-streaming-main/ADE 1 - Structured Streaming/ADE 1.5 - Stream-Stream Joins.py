# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream-Stream Joins
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Create streams using the Rate source
# MAGIC 1. Perform Stream-Stream Inner Join without Watermarking
# MAGIC 1. Perform Stream-Stream Inner Join with Watermarking
# MAGIC 2. Perform Stream-Stream Inner Join with Watermarking and Event Time Constraints
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC #Stream-Stream Joins
# MAGIC We are going to use the the canonical example of ad monetization, where we want to find out which ad impressions led to user clicks. 
# MAGIC Typically, in such scenarios, there are two streams of data from different sources - ad impressions and ad clicks. 
# MAGIC
# MAGIC Both type of events have a common ad identifier (say, `adId`), and we want to match clicks with impressions based on the `adId`. 
# MAGIC In addition, each event also has a timestamp, which we will use to specify additional conditions in the query to limit the streaming state.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create two streams - Impressions and Clicks
# MAGIC
# MAGIC We simulate live streams in a lab setup by using the built-in `rate` format, that generates data at a given fixed rate. 
# MAGIC
# MAGIC See   <a href="https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html#input-sources" target="_blank">Stream Input Sources</a>  for more information on stream sources for data generation.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import rand

spark.conf.set("spark.sql.shuffle.partitions", "1")

impressions = (
  spark
    .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
    .selectExpr("value AS adId", "timestamp AS impressionTime")
)

clicks = (
  spark
  .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
  .where((rand() * 100).cast("integer") < 10)      # 10 out of every 100 impressions result in a click
  .selectExpr("(value - 50) AS adId ", "timestamp AS clickTime")      # -50 so that a click with same id as impression is generated later (i.e. delayed data).
  .where("adId > 0")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC Let's see what data these two streaming DataFrames generate.
# MAGIC

# COMMAND ----------

display(impressions, streamName="display_impressions")

##################################
## Once finished viewing, click ##
## 'Cancel' before proceeding   ##
##################################

# COMMAND ----------

display(clicks, streamName="display_clicks")

##################################
## Once finished viewing, click ##
## 'Cancel' before proceeding   ##
##################################

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream-Stream Inner Join without Watermark
# MAGIC
# MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by their common key `adId`.

# COMMAND ----------

################################################
## Without Watermark, State continues to grow ##
################################################

display(impressions.join(clicks, "adId"), streamName="naive_streaming_join")

###################################
## Once finished viewing, click  ##
## 'Interrupt' before proceeding ##
###################################

# COMMAND ----------

# MAGIC %md 
# MAGIC After you start this query, within a minute, you will start getting joined impressions and clicks. The delays of a minute is due to the fact that clicks are being generated with delay over the corresponding impressions.
# MAGIC
# MAGIC In addition, if you expand the details of the query above, you will find a few timelines of query metrics - the processing rates, the micro-batch durations, and the size of the state. 
# MAGIC If you keep running this query, you will notice that the state will keep growing in an unbounded manner. This is because the query must buffer all past input as any new input can match with any input from the past. 

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Stream-Stream Inner Join with Watermarking
# MAGIC
# MAGIC To avoid unbounded state, you have to define additional join conditions such that indefinitely old inputs cannot match with future inputs and therefore can be cleared from the state. In other words, you will have to do the following additional steps in the join.
# MAGIC
# MAGIC 1. Define watermark delays on both inputs such that the engine knows how delayed the input can be. 
# MAGIC
# MAGIC 1. Define a constraint on event-time across the two inputs such that the engine can figure out when old rows of one input is not going to be required (i.e. will not satisfy the time constraint) for matches with the other input. This constraint can be defined in one of the two ways.
# MAGIC
# MAGIC   a. Time range join conditions (e.g. `...JOIN ON leftTime BETWEN rightTime AND rightTime + INTERVAL 1 HOUR`),
# MAGIC   
# MAGIC   b. Join on event-time windows (e.g. `...JOIN ON leftTimeWindow = rightTimeWindow`).
# MAGIC
# MAGIC Let's apply these steps to our use case. 
# MAGIC
# MAGIC 1. Watermark delays: Say, the impressions and the corresponding clicks can be delayed/late in event-time by at most "10 seconds" and "20 seconds", respectively. This is specified in the query as watermarks delays using `withWatermark`.
# MAGIC
# MAGIC 1. Event-time range condition: Say, a click can occur within a time range of 0 seconds to 1 minute after the corresponding impression. This is specified in the query as a join condition between `impressionTime` and `clickTime`.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr

# Define watermarks
impressionsWithWatermark = (impressions 
  .selectExpr("adId AS impressionAdId", "impressionTime") 
  .withWatermark("impressionTime", "10 seconds "))
                            
clicksWithWatermark = (clicks 
  .selectExpr("adId AS clickAdId", "clickTime")
  .withWatermark("clickTime", "20 seconds"))        # max 20 seconds late

# COMMAND ----------

# Inner join with Watermark 
display(impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" clickAdId = impressionAdId""")), streamName="streaming_join_with_watermarks")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream-Stream Join with Watermark and Event Time Constraint
# MAGIC
# MAGIC This will enable Structured Streaming to perform full state cleanup. Use this for long-running stream processes.

# COMMAND ----------

# Inner join with watermark + Time conditions - Required for full state cleanup
display(impressionsWithWatermark.join(
    clicksWithWatermark,
    expr(""" clickAdId = impressionAdId AND 
      clickTime >= impressionTime AND 
      clickTime <= impressionTime + interval 1 minutes""")), streamName="streaming_join_with_watermarks_and_event_time_constraints")

# COMMAND ----------

# MAGIC %md 
# MAGIC We are getting the similar results as the previous simple join query. However, if you look at the query metrics now, you will find that after about a couple of minutes of running the query, the size of the state will stabilize as the old buffered events will start getting cleared up.
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Further Information
# MAGIC You can read more about stream-stream joins in the following places:
# MAGIC
# MAGIC - Databricks blog post on stream-stream joins - https://databricks.com/blog/2018/03/13/introducing-stream-stream-joins-in-apache-spark-2-3.html
# MAGIC - Apache Programming Guide on Structured Streaming - https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#stream-stream-joins
# MAGIC - Talk at Spark Summit Europe 2017 - https://databricks.com/session/deep-dive-into-stateful-stream-processing-in-structured-streaming
# MAGIC

# COMMAND ----------

for s in spark.streams.active:
    print(s.name)
    s.stop()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
