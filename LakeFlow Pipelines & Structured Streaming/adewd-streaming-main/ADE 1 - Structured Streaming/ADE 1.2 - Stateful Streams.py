# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Stateful Streaming Query
# MAGIC
# MAGIC ##### Objectives
# MAGIC 1. Build a streaming DataFrame running a stateful transformation
# MAGIC 1. Display streaming query results
# MAGIC 1. Write streaming query results
# MAGIC
# MAGIC ##### Classes
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamReader.html" target="_blank">DataStreamReader</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.DataStreamWriter.html" target="_blank">DataStreamWriter</a>
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.ss/api/pyspark.sql.streaming.StreamingQuery.html" target="_blank">StreamingQuery</a>
# MAGIC
# MAGIC ##### References
# MAGIC - <a href="https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html" target="_blank">Streaming Programming Guide</a>

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.2 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table only_words (word string) TBLPROPERTIES('delta.appendOnly' = true);
# MAGIC
# MAGIC insert into only_words values ("ant");
# MAGIC insert into only_words values ("bat");

# COMMAND ----------

# MAGIC %md
# MAGIC # Stateful Queries
# MAGIC The queries in the previous notebook performed simple transformations on each row. We categorize them as **Stateless Queries** - as no information is carried between successive triggers. The opposite of a stateless query is a **Stateful Query**.
# MAGIC
# MAGIC The **Word Count** query shown below is a good representative.
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://spark.apache.org/docs/3.5.1/img/structured-streaming-example-model.png" alt="A simple stateful pipeline" style="width: 600px">
# MAGIC </div>
# MAGIC
# MAGIC Stateful queries maintain and update state information as they processes streaming data.
# MAGIC * State kept in memory or on disk, allowing the query to maintain a running total, count, or other arbitrary state over time.
# MAGIC * Stateful queries are used for operations such as windowed aggregations, joins, and deduplication, where the query needs to maintain state information to produce correct results.
# MAGIC * By maintaining state, stateful queries can provide more accurate and meaningful insights into streaming data, making them an essential part of many streaming applications.
# MAGIC
# MAGIC Reference: <a href="https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html" target="_blank">Structured Streaming Programming Guide</a>. 
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Calculating Running Word Counts
(spark.readStream
    .table("only_words")
    .groupBy("word")
    .count()
    .writeStream
    .queryName("01: Write Word Counts to Delta")
    .trigger(processingTime="1 second")
    # .trigger(availableNow=True)
    .option("checkpointLocation", f"{DA.paths.working_dir}/01")
    .toTable("word_counts_01", outputMode="complete")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- existing words
# MAGIC insert into only_words values ("ant");
# MAGIC -- new words
# MAGIC insert into only_words values ("cat");
# MAGIC insert into only_words values ("dog");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from word_counts_04
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Mini-Lab
# MAGIC Try adding more words/animals to the source table to see how the counts change. 
# MAGIC
# MAGIC Also try out the `availableNow=true` trigger type to see how the stream stops after processing all new input.

# COMMAND ----------

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
