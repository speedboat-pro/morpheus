# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Dynamic Task Parameters and Iteration
# MAGIC
# MAGIC Databricks Workflow Jobs have the ability to run tasks based on the result of previously run tasks. For example, you can setup a task to only run if a previous task fails.
# MAGIC
# MAGIC In a `For each `task in your Databricks jobs, you can run a nested task in a loop, passing a different set of parameters to each iteration of the task.
# MAGIC
# MAGIC In this lesson, we will configure a pipeline with a dynamic task parameter, and we will learn how to drive a loop based on the results. 
# MAGIC
# MAGIC **Our goal is to create a job that discovers all tables in a schema and runs a task for each of them**

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC   - In the drop-down, select **More**.
# MAGIC
# MAGIC   - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Create a Dynamic Task Parameter as  a JSON Array

# COMMAND ----------

# MAGIC %md
# MAGIC - 1. Create a job named **&lt;your-schema>_Lesson_06&gt;** with three individual tasks.
# MAGIC - 2. Open your new starter job **&lt;your-schema>_Lesson_06&gt;**:
# MAGIC   - a. Navigate to **Workflows** and open it in a new tab.
# MAGIC   
# MAGIC   - b. Select your new job, **&lt;your-schema>_Lesson_06&gt;**.
# MAGIC   
# MAGIC   - c. Select **Tasks** in the top navigation bar.
# MAGIC   
# MAGIC   - d. Configure the task: 
# MAGIC   
# MAGIC       |Task Configuration| Value|
# MAGIC       |---|---|
# MAGIC       |**Task name**| _Iteration Over Tables_| 
# MAGIC       |**Type**| Notebook|
# MAGIC       |**Path**| Navigate to the **Lesson 6 Notebooks** and choose the **Get Tables for Iteration** notebook|
# MAGIC       |**Parameters**| click on the **JSON** button and add:```{"catalog": "samples","schema": "tpch"}```|
# MAGIC   - e. Save the task
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Notebook Results 
# MAGIC The following notebooks use simple code to demonstrate the functionality of using parameters and converting a column of a table into a JSON array
# MAGIC
# MAGIC - The parameters `catalog` and `schema` configure the notebook to set a scope for our SQL query
# MAGIC
# MAGIC - The results of the `SHOW TABLES` SQL statement are then converted to a JSON array `json_table`
# MAGIC
# MAGIC - A `dbutils.jobs.taskValues` key named **tables**: with the array that will drive the loop

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Add the `For Each` Task
# MAGIC We are going to make some changes to this job and configure our main task to query the tables defined in the previous task.
# MAGIC
# MAGIC 1. Remove the **Parameters** from the current task and move them to the Job level **Job Parameters** as individual key/value pairs
# MAGIC 1. Click on the blue **Create task** button hovering above the one task in the job
# MAGIC 1. Configure the task:
# MAGIC       |Task Configuration| Value|
# MAGIC       |---|---|
# MAGIC       |**Task name**| _Foreach_Loop_| 
# MAGIC       |**Type**| For each|
# MAGIC       |**Inputs**| `{{tasks.Iteration_Over_Tables.values.tables}}`|
# MAGIC 1. Click on the **Add a task to loop over** button
# MAGIC 1. Configure the **Foreach_loop_iteration** task:
# MAGIC       |Task Configuration| Value|
# MAGIC       |---|---|
# MAGIC       |**Type**| Notebook|
# MAGIC       |**Path**| Navigate to the **Lesson 6 Notebooks** and choose the **Iterate Through Tables** notebook|
# MAGIC       |**Parameters**| `{"table_name": "{{input}}"}`|
# MAGIC
# MAGIC       Notice that the `catalog` and `schema` **Parameters** are automatically pushed down
# MAGIC
# MAGIC 1. Click on **Create task**
# MAGIC 1. Click **Run now**

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Review the `Foreach_loop` run
# MAGIC
# MAGIC 1. Notice each item from the JSON array is in the **Input** column
# MAGIC
# MAGIC 1. Click on either the  **Start time** or **End time** column to see the output of each run
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
