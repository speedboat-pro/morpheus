# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Streaming Joins
# MAGIC
# MAGIC In this lesson, you'll join streaming heart rate data with the completed workouts table.
# MAGIC
# MAGIC We'll be creating the table **`workout_bpm`** in our architectural diagram.
# MAGIC
# MAGIC This pattern will take advantage of Delta Lake's ability to guarantee that the latest version of a table is returned each time it is queried.
# MAGIC
# MAGIC By the end of this lesson, students will be able to:
# MAGIC - Describe guarantees around versioning and matching for stream-static joins
# MAGIC - Leverage Spark SQL and PySpark to process stream-static joins

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC Note that we will only be streaming from **one** of our tables. The **`completed_workouts`** table is no longer streamable as it breaks the requirement of an ever-appending source for Structured Streaming. However, when performing a stream-static join with a Delta table, each batch will confirm that the newest version of the static Delta table is being used.

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")
lookup_db = spark.conf.get("lookup_db")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Completed Workouts
# MAGIC
# MAGIC Review the **`workouts_silver`** table. For this data, the **`user_id`** and **`session_id`** form a composite key. Each pair should eventually have 2 records present, marking the "start" and "stop" action for each workout.
# MAGIC
# MAGIC The query below matches our start and stop actions, capturing the time for each action. The **`in_progress`** field indicates whether or not a given workout session is ongoing.

# COMMAND ----------

@dlt.table
def completed_workouts():
    return spark.sql(f"""
      SELECT a.user_id, a.workout_id, a.session_id, a.start_time start_time, b.end_time end_time, a.in_progress AND (b.in_progress IS NULL) in_progress
      FROM (
        SELECT user_id, workout_id, session_id, time start_time, null end_time, true in_progress
        FROM LIVE.workouts_silver
        WHERE action = "start") a
      LEFT JOIN (
        SELECT user_id, workout_id, session_id, null start_time, time end_time, false in_progress
        FROM LIVE.workouts_silver
        WHERE action = "stop") b
      ON a.user_id = b.user_id AND a.session_id = b.session_id
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform Stream-Static Join to Align Workouts to Heart Rate Recordings
# MAGIC
# MAGIC Below we'll configure our query to join our stream to our **`completed_workouts`** table. 
# MAGIC
# MAGIC Note that our heart rate recordings only have **`device_id`**, while our workouts use **`user_id`** as the unique identifier. We'll need to use our **`user_lookup`** table to match these values. Because all tables are Delta Lake tables, we're guaranteed to get the latest version of each table during each microbatch transaction.
# MAGIC
# MAGIC **NOTE**: The setup script includes logic to define a **`user_lookup`** table required for the join below.
# MAGIC
# MAGIC Importantly, our devices occasionally send messages with negative recordings, which represent a potential error in the recorded values. We'll need to define predicate conditions to ensure that only positive recordings are processed.

# COMMAND ----------

@dlt.table
def user_lookup():
    return spark.read.table(f"{lookup_db}.user_lookup")

# Stream static join
@dlt.table
def workout_bpm():
    return spark.sql("""
      SELECT d.user_id, d.workout_id, d.session_id, time, heartrate
      FROM STREAM(LIVE.bpm_silver) c
      INNER JOIN (
        SELECT a.user_id, b.device_id, workout_id, session_id, start_time, end_time
        FROM LIVE.completed_workouts a
        INNER JOIN LIVE.user_lookup b
        ON a.user_id = b.user_id) d
      ON c.device_id = d.device_id AND time BETWEEN start_time AND end_time
      WHERE c.bpm_check = 'OK'
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Note that the streaming portion of the join drives this join process. As currently implemented, this means that records from the **`bpm_silver`** table will only appear in our results table if a matching record has been written to the **`completed_workouts`** table prior to processing this query.
# MAGIC
# MAGIC Stream-static joins are not stateful, meaning that we cannot configure our query to wait for records to appear in the right side of the join prior to calculating the results. When leveraging stream-static joins, make sure to be aware of potential limitations for unmatched records. (Note that a separate batch job could be configured to find and insert records that were missed during incremental execution).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
