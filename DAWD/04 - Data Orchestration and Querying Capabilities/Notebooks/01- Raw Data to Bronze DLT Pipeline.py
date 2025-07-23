# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw Data to Bronze DLT Pipeline
# MAGIC
# MAGIC This notebook is designed to create and execute a Delta Live Tables (DLT) pipeline that uses the Databricks Autoloader to ingest data into the Bronze layer of a data lakehouse architecture. The Bronze layer is typically used to store raw or minimally processed data, ensuring a reliable foundation for further transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC - To run this notebook, you need to use one of the following Databricks runtime(s): `16.3.x-scala2.12`

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup-4

# COMMAND ----------

# MAGIC %md
# MAGIC ##Setup
# MAGIC To set up the environment and prepare for the workflow:
# MAGIC
# MAGIC - **Generate the Pipeline:** Create a pipeline to automate the process of loading data using the Autoloader.

# COMMAND ----------

DA.generate_pipeline(
    pipeline_name="Serverless Workflow",
    notebooks_folder="notebooks",
    pipeline_notebooks=[
        "Pipelines/01 - Raw Data to Bronze"
    ],
    use_schema=DA.schema_name,
    use_configuration={'source': f'{DA.paths.stream_source}'}
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Start and Monitor the Pipeline
# MAGIC - **Start the Pipeline:** Initialize the pipeline and begin the data processing.

# COMMAND ----------

# Start the pipeline
DA.start_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC - **Monitor the Pipeline:** Observe the pipeline's status until it completes or fails.

# COMMAND ----------

DA.monitor_pipeline_status()
try:
    # Retrieve workspace URL dynamically from Databricks configurations
    workspace_url = spark.conf.get('spark.databricks.workspaceUrl')
    pipeline_name = "Serverless Workflow"
    
    # Retrieve and print the clickable pipeline URL
    pipeline_url = get_pipeline_url(workspace_url, pipeline_name)
    print(f"Delta Live Tables Pipeline URL: {pipeline_url}")
except ValueError as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamically Determine Pipeline Status
# MAGIC
# MAGIC To dynamically capture and evaluate the pipeline's final status, we define the determine_dlt_status function:
# MAGIC
# MAGIC - Function: `determine_dlt_status`

# COMMAND ----------

import re
from IPython.display import display, Markdown

def determine_dlt_status():
    """
    Determines the DLT pipeline status by dynamically capturing the actual output logs of the cell.
    Sets the workflow dependency variable based on the final status.
    """
    import sys
    from io import StringIO

    # Capture the actual output of the cell
    logs_io = StringIO()
    sys.stdout = logs_io

    # Assuming DA.monitor_pipeline_status() writes the actual logs to stdout
    DA.monitor_pipeline_status()

    # Reset stdout
    sys.stdout = sys.__stdout__

    # Get the captured logs
    logs = logs_io.getvalue()
    print("Captured Logs:")
    print(logs)

    # Determine the final pipeline status using regex
    completed_match = re.search(r"Pipeline Status: COMPLETED", logs)
    successful_match = re.search(r"Successful", logs)

    # Set the dependency value
    dlt_status = "SUCCESS" if completed_match and successful_match else "FAIL"
    dbutils.jobs.taskValues.set(key="DLT_SUCCESS_True", value=dlt_status)

    # Display the result for visibility
    print(f"Pipeline Status set for dependency: {dlt_status}")
    return dlt_status


# Run the function
determine_dlt_status()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
