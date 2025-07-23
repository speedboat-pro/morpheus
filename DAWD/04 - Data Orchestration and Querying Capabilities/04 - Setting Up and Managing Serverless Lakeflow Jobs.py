# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo: Setting Up and Managing Serverless Lakeflow Jobs  
# MAGIC
# MAGIC In this demo, we’ll set up a **Databricks Lakeflow Jobs** leveraging SQL tasks to automate a series of **Data Warehousing** tasks. These tasks include data ingestion, validation, transformation, and generating insights within a **Medallion Architecture** (Bronze, Silver, Gold layers). Additionally, we will explore error handling, retries, scheduling options, and integration with external tools.
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC
# MAGIC By the end of this demo, you will learn how to:
# MAGIC
# MAGIC - Create a Lakeflow Job with **SQL tasks** for a Medallion Architecture pipeline.
# MAGIC - Set up task dependencies and implement conditional logic for lakeflow jobs control.
# MAGIC - Use the Lakeflow Job UI for **monitoring** and **data lineage visualization** to trace data transformations and dependencies.
# MAGIC - Configure error handling and retries for tasks.
# MAGIC - Schedule Lakeflow Job using manual triggers.
# MAGIC - Set up **notifications** for monitoring and analyze the execution history.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC     - In the drop-down, select **More**.
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC     
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC - To run this notebook, you need to use one of the following Databricks runtime(s): `16.3.x-scala2.12`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4

# COMMAND ----------

# MAGIC %md
# MAGIC **Other Conventions:**
# MAGIC
# MAGIC Throughout this demo, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Create a Databricks Serverless Lakeflow Job in the UI
# MAGIC
# MAGIC 1. **Navigate to Workflows**:
# MAGIC    - In your Databricks workspace, click on the **Workflows** icon in the left sidebar.
# MAGIC    
# MAGIC 2. **Create a New Job**:
# MAGIC    - Click on **Create** in the upper-right corner of the Workflows page and Select Job.
# MAGIC    - Name the job "Serverless lakeflow Jobs" or something similar for easy identification.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Add Tasks to the Job:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: Add Tasks - 1 to the Lakeflow Jobs
# MAGIC
# MAGIC 1. **Create First Task**:
# MAGIC    - Name the task `01__Raw_Data_to_Bronze_DLT_Pipeline`.
# MAGIC    - Set **Type** to `Notebook`.
# MAGIC    - **Source** should be set to `Workspace`.
# MAGIC    - Set **Path** to the notebook for data quality assessment (e.g., `../04 - Data Orchestration and Querying Capabilities/Notebooks/01 - Raw Data to Bronze DLT Pipeline`).
# MAGIC    - Select an `Serveless` cluster for this task.
# MAGIC    - Click **Create Task**.
# MAGIC    - Notifications:
# MAGIC       - Add notification to send emails on failure (e.g., ` your-email@databricks.com`).
# MAGIC
# MAGIC This task runs the Raw Data-to-Bronze pipeline to ingest data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Set Conditional Check for Bronze Pipeline
# MAGIC
# MAGIC 1. **Create Conditional Task**:
# MAGIC    - Click on **Add Task**
# MAGIC     - Set **Type** to `If/else condition`.
# MAGIC    - Name the task `DLT1_condition`.
# MAGIC     - Set **Depends on** to `01__Raw_Data_to_Bronze_DLT_Pipeline` to ensure this task runs after data quality checks.
# MAGIC    - Condition: Set the expression to **&lcub;&lcub;tasks.01__Raw_Data_to_Bronze_DLT_Pipeline.values.DLT_SUCCESS_True&rcub;&rcub; == SUCCESS**
# MAGIC    - Click **Save Task**.
# MAGIC
# MAGIC This task evaluates whether the Bronze pipeline execution succeeded or failed, determining the next steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Bronze to Silver DLT Pipeline
# MAGIC
# MAGIC 1. **Create Second Task**:
# MAGIC    - Click on **Add Task**
# MAGIC     - Set **Type** to `Notebook`.
# MAGIC    - Name the task `02__Bronze_to_Silver_DLT_Pipeline`.
# MAGIC    - **Source** should be set to `Workspace`.
# MAGIC    - Set **Path** to the notebook for data quality assessment (e.g., `../04 - Data Orchestration and Querying Capabilities/Notebooks/02 - Bronze to Silver DLT Pipeline`).
# MAGIC    - Select an `Serveless` cluster for this task.
# MAGIC    - Set **Depends** on to **`DLT1_condition(True)`**
# MAGIC    - Click **Create Task**.
# MAGIC    - Notifications:
# MAGIC       - Add notification to send emails on failure (e.g., ` your-email@databricks.com`).
# MAGIC
# MAGIC This task processes data from Bronze to Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.4: Set Conditional Check for Silver Pipeline
# MAGIC 1. **Create Conditional Task**:
# MAGIC    - Click on **Add Task**
# MAGIC    - Set **Type** to `If/else condition`.
# MAGIC    - Name the task `DLT2_condition`.
# MAGIC    - Set **Depends on** to `02__Bronze_to_Silver_DLT_Pipeline`.
# MAGIC    - Condition: Set the expression to  **&lcub;&lcub;tasks.02__Bronze_to_Silver_DLT_Pipeline.values.DLT_SUCCESS_True&rcub;&rcub; == SUCCESS**
# MAGIC    - Click **Save Task**.
# MAGIC
# MAGIC This task evaluates whether the Silver pipeline execution succeeded or failed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.5: Silver to Gold DLT Pipeline
# MAGIC
# MAGIC 1. **Create Third Task**:
# MAGIC    - Click on **Add Task**
# MAGIC    - Set **Type** to `Notebook`.
# MAGIC    - Name the task `03__Silver_to_Gold_DLT_Pipeline`.
# MAGIC    - **Source** should be set to `Workspace`.
# MAGIC    - Set **Path** to the notebook for feature importance analysis (e.g., `../04 - Data Orchestration and Querying Capabilities/Notebooks/03 - Silver to Gold DLT Pipeline`).
# MAGIC    - Use the same cluster as the previous tasks.
# MAGIC    - Set **Depends on** to:
# MAGIC      - `DLT2_condition (True)`.
# MAGIC    - Notifications:
# MAGIC       - Add notification to send emails on failure (e.g., ` your-email@databricks.com`).
# MAGIC    - Click **Create Task**.
# MAGIC
# MAGIC This task processes data from Silver to Gold.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.6: Troubleshooting Notebook
# MAGIC
# MAGIC 1. **Create Fifth Task**:
# MAGIC    - Click on **Add Task**
# MAGIC    - Set **Type** to `Notebook`.
# MAGIC    - Name the task `troubleshooting`.
# MAGIC    - **Source** should be set to `Workspace`.
# MAGIC    - Set **Path** to the notebook for saving the final report (e.g., `../04 - Data Orchestration and Querying Capabilities/Notebooks/troubleshooting`).
# MAGIC    - Use the same cluster as the previous tasks.
# MAGIC    - Set **Depends on** to both:
# MAGIC      - `DLT1_condition (False)` and `DLT2_condition (False)`.
# MAGIC    - Set **Run if dependencies** to "At least one succeeded" to ensure it saves the report regardless of the path taken.
# MAGIC    - Notifications:
# MAGIC       - Add notification to send emails on Success (e.g., ` your-email@databricks.com`).
# MAGIC    - Click **Create Task**.
# MAGIC
# MAGIC This task runs a troubleshooting notebook to analyze and resolve pipeline issues. Example steps in the notebook include querying logs and providing remediation suggestions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.7: Enable Email Notifications
# MAGIC
# MAGIC 1. **Set up Notifications**:
# MAGIC    - In the job's configuration, navigate to the **Notifications** section.
# MAGIC    - Enable email notifications by adding your email to receive updates on job completion.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Trigger the Lakeflow Jobs Manually
# MAGIC
# MAGIC 1. **Run the Job**:
# MAGIC    - Go to the job in the Databricks UI and click on **Run Now** in the top-right corner to manually trigger the job. This will execute all tasks in the Lakeflow Job according to their dependencies and conditions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Monitor the Lakeflow Jobs Execution
# MAGIC
# MAGIC 1. **Navigate to the Runs Tab**:
# MAGIC    - In the job interface, go to the **Runs** tab to view active and completed executions of the job.
# MAGIC
# MAGIC 2. **Observe Task Execution**:
# MAGIC    - Each task’s status is displayed in the **Runs** tab, where you can see which tasks are currently executing or have completed.
# MAGIC    - Click on each task to view its execution details and outputs, allowing you to troubleshoot and verify each stage.
# MAGIC    - Check the logs to see if the Lakeflow Jobs followed the correct path based on the unusual pattern detection condition.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Lineage: Viewing Data Lineage for a Table
# MAGIC Data lineage in Unity Catalog provides end-to-end visibility into how data is sourced, transformed, and consumed. With lineage information, you can:
# MAGIC
# MAGIC - Understand the dependencies of your datasets.
# MAGIC - Identify the upstream and downstream impact of schema changes.
# MAGIC - Debug pipeline issues by tracing data flow through the system.
# MAGIC - Ensure compliance by auditing data usage and transformations.
# MAGIC
# MAGIC **Benefits of Data Lineage**
# MAGIC - **Visibility:** Gain a comprehensive view of data flow across your pipeline.
# MAGIC - **Impact Analysis:** Determine how changes in one dataset affect downstream applications.
# MAGIC - **Governance and Compliance:** Track data transformations and usage for regulatory requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Viewing Lineage in Unity Catalog
# MAGIC The following code helps you access the lineage information for a table directly in the Databricks UI.

# COMMAND ----------

# Generate the workspace URL dynamically
workspace_url = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"

# Define the table name for which to view the lineage
table_name = "order_table_gold"  # Replace with any other table name as needed

# Construct the URL for the data lineage page in Unity Catalog
lineage_url = f"{workspace_url}/explore/data/{DA.catalog_name}/{DA.schema_name}/{table_name}?activeTab=lineage"

# Print a user-friendly message with the lineage URL
print(f"Access the data lineage for the table '{table_name}' using the following URL:")

# Display the URL as a clickable link in Databricks
displayHTML(f'<a href="{lineage_url}" target="_blank">Click here to view the lineage for {table_name}</a>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this demo, you learned how to:
# MAGIC - Configure and execute a Databricks Lakeflow job with multiple tasks.
# MAGIC - Use dependencies and conditional paths to control the flow of tasks based on the conditions.
# MAGIC - Set up email notifications to stay updated on job execution.
# MAGIC - Trigger the Lakeflow job manually and monitor its execution.
# MAGIC
# MAGIC This Lakeflow Jobs setup ensures robust automation for DLT pipelines with integrated troubleshooting and notification mechanisms. The conditional paths provide flexibility to handle success and failure scenarios efficiently, while monitoring and logging enhance visibility into pipeline executions.
# MAGIC
# MAGIC Additionally, you explored how to leverage **data lineage** within Unity Catalog, enabling deeper insights into the relationships between datasets and transformations. This feature enhances governance, auditing, and troubleshooting across your Lakeflow Jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
