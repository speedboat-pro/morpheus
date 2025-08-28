# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo - Lakehouse Monitoring Dashboard
# MAGIC
# MAGIC In this demo, we will show you how to monitor the performance of a machine learning model using Databricks. We will use a diabetes dataset to train a model, track inference data, and analyze its performance using Databricks' built-in features. Additionally, we will detect drift and demonstrate how to handle model retraining and continuous monitoring.
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Train and analyze a machine learning model's inference logs.
# MAGIC - Monitor the model's performance and detect anomalies or drift.
# MAGIC - Handle drift detection and trigger retraining when needed.
# MAGIC - Utilize Databricks Lakehouse Monitoring to continuously track model performance metrics.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC    - In the drop-down, select **More**.
# MAGIC    - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC   
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC - To run this notebook, you need to use one of the following Databricks runtime(s): **`16.3.x-cpu-ml-scala2.12`**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo. Execute the following cell:
# MAGIC
# MAGIC > ****🚨Note:**** If you encounter a "file not found" error when running this cell, simply re-run it. This issue may occur as we transition from DBFS to Unity Catalog.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-3.1

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
# MAGIC # Prepare Model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load dataset
# MAGIC
# MAGIC In this section, we load the dataset for diabetes classification. Since the dataset is small and we want to go straight to training a classic model, we load it directly with Pandas.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import lit, col, from_unixtime, array
from pyspark.sql.types import DoubleType
import time
import json

# Load the Delta table into a Spark DataFrame
dataset_path = f"{DA.paths.working_dir}/diabetes-dataset"
diabetes_df = spark.read.format("delta").load(dataset_path)

# Convert to Pandas DataFrame
diabetes_df_pd = diabetes_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train / New Requests Split
# MAGIC
# MAGIC In a typical model training scenario, we would split the data into **training** and **test** sets to evaluate the model's performance. However, our focus here is on what happens once a model is trained and integrated into a production environment, where it encounters new data that was not part of its original training set.
# MAGIC
# MAGIC To simulate this situation, we will split our existing dataset into a **training** set and a **new requests** set. This separation allows us to explore how the model handles incoming data that might differ from its training data, which can help us identify potential issues such as drift.
# MAGIC
# MAGIC In order to simulate a drift in the model features, we will use the `Age` feature of our data to divide the sets. This approach enables us to study how the model's predictions change when exposed to data with characteristics that differ from the original training data.

# COMMAND ----------

# Split the data into training and request sets based on the 'Age' feature
train_df = diabetes_df_pd[diabetes_df_pd['Age'] <= 9]
request_df = diabetes_df_pd[diabetes_df_pd['Age'] > 9]

# Define the target column
target_col = "Diabetes_binary"

# Prepare training features and labels
X_train = train_df.drop(labels=[target_col, 'id'], axis=1)
y_train = train_df[target_col]

# Prepare request features and labels
X_request = request_df.drop(labels=[target_col], axis=1)
y_request = request_df[target_col]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit a Classification Model
# MAGIC
# MAGIC Let's go ahead and fit a Decision Tree model and register it with Unity Catalog.

# COMMAND ----------

import mlflow
from sklearn.tree import DecisionTreeClassifier
from mlflow.models.signature import infer_signature

# Set the MLflow registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Enable automatic logging with MLflow
mlflow.sklearn.autolog(log_input_examples=True)

# Initialize and train the Decision Tree Classifier
dtc = DecisionTreeClassifier()
dtc_mdl = dtc.fit(X_train, y_train)

# Define the model name using catalog and schema
model_name = f"{DA.catalog_name}.{DA.schema_name}.diabetes_model"

# Infer the model signature
signature = infer_signature(X_train, y_train)

# Log the model to MLflow
mlflow.sklearn.log_model(
    sk_model=dtc_mdl, 
    artifact_path="model-artifacts",
    signature=signature,
    registered_model_name=model_name
)

print(f"Model '{model_name}' has been registered with MLflow.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save the Training Data as Reference for Drift
# MAGIC
# MAGIC We can save the training dataset that was used to train the model. This can be used later during monitoring to provide a reference to determine if a drift has happened between training and the new incoming requests.

# COMMAND ----------

from pyspark.sql.functions import lit, col
from pyspark.sql.types import DoubleType

# Create the Spark DataFrame and rename 'Diabetes_binary' to 'labeled_data', while casting it to DoubleType
spark_df = (spark.createDataFrame(train_df)
            .withColumn('model_id', lit(0))
            .withColumn('labeled_data', col('Diabetes_binary').cast(DoubleType()).alias('labeled_data')))

# Write the DataFrame to Delta format
(
    spark_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(f"{DA.catalog_name}.{DA.schema_name}.demo_baseline_features")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Inference Table Data
# MAGIC
# MAGIC In this section, we focus on extracting and analyzing data logged in the **inference table**. This table contains detailed information on each request and response received by the model. However, the raw format of this data is optimized for storage rather than immediate analysis. To effectively monitor and interpret the data, we will convert it into a more analyzable format using a series of steps.
# MAGIC
# MAGIC **Steps to Process Inference Table Data:**
# MAGIC 1. **Timestamp Conversion**: Convert the timestamp data from milliseconds to a human-readable timestamp format. This adjustment helps in analyzing the data based on when events occurred.
# MAGIC 2. **Unpacking JSON**: Requests and responses in the inference table are stored in JSON format. We'll unpack these JSON strings into a structured DataFrame format, making it easier to work with the data for analysis.
# MAGIC 3. **Exploding Batched Requests**: If the model receives batched requests, these will be exploded into individual records for simplified analysis. Each entry in a batch request is treated as a separate data point to ensure accuracy.
# MAGIC 4. **Schema Transformation**: We will transform the schema of the extracted data to better align it with analytical needs, facilitating easier data interpretation and monitoring.
# MAGIC
# MAGIC These steps will ensure that the inference data is optimized for analysis and monitoring, providing clear insights into how the model is performing over time.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **📝Note for Instructors:**
# MAGIC The **inference table** used in this demo contains simulated or pre-populated data that mimics the behavior of a live inference table receiving real-time requests. This allows students to focus on analyzing and processing the data without needing to deploy a model during the exercise. It is important to guide students on how to handle live inference data in a real-world scenario where new requests are logged continuously.
# MAGIC
# MAGIC **Please note that**: Typically, we need to wait for the inference table to populate with new data, which usually takes about 5-7 minutes after batch predictions are triggered.

# COMMAND ----------

# Read and display the inference table
try:
    inference_df = spark.read.table(f"{DA.catalog_name}.{DA.schema_name}.model_inference_table")
    
    if inference_df.count() > 0:
        display(inference_df)
    else:
        print("The inference table is empty.")
except Exception as e:
    print(f"Error finding the table: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Conversion Helper Functions
# MAGIC The JSON fields within the inference table require transformation into structured columns. We will define helper functions to handle this conversion efficiently.

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F, types as T
import json

# Define a UDF to handle JSON unpacking
def convert_to_record_json(json_str: str) -> str:
    """
    Converts records from different accepted JSON formats into a common, record-oriented
    DataFrame format which can be parsed by the PySpark function `from_json`.
    
    :param json_str: The JSON string containing the request or response payload.
    :return: A JSON string containing the converted payload in a record-oriented format.
    """
    try:
        # Attempt to parse the JSON string
        request = json.loads(json_str)
    except json.JSONDecodeError:
        # If parsing fails, return the original string
        return json_str

    output = []
    if isinstance(request, dict):
        # Handle different JSON formats and convert to a common format
        if "dataframe_records" in request:
            output.extend(request["dataframe_records"])
        elif "dataframe_split" in request:
            dataframe_split = request["dataframe_split"]
            output.extend([dict(zip(dataframe_split["columns"], values)) for values in dataframe_split["data"]])
        elif "instances" in request:
            output.extend(request["instances"])
        elif "inputs" in request:
            output.extend([dict(zip(request["inputs"], values)) for values in zip(*request["inputs"].values())])
        elif "predictions" in request:
            output.extend([{'predictions': prediction} for prediction in request["predictions"]])
        return json.dumps(output)
    else:
        # If the format is unsupported, return the original string
        return json_str
#@F.pandas_udf(T.StringType())
def json_consolidation_udf(json_strs: pd.Series) -> pd.Series:
    """
    A UDF to apply the JSON conversion function to every request/response.
    
    :param json_strs: A Pandas Series containing JSON strings.
    :return: A Pandas Series with the converted JSON strings.
    """
    return json_strs.apply(convert_to_record_json)

# COMMAND ----------

@udf("string")
def foo(str):
    return foo+"l"


# COMMAND ----------

import pyspark.sql.types
spark.udf.register("foo_udf", foo, pyspark.sql.types.StringType())

# COMMAND ----------

df = spark.sql("select * from samples.tpch.orders limit 100")

# COMMAND ----------

df = df.withColumn('result', foo_udf(F.col("o_orderpriority")))
display(df)


# COMMAND ----------

spark.registerFunction("json_consolidation", json_consolidation_udf)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing the Raw Inference Data
# MAGIC We will process the inference data by unpacking JSON fields and converting relevant data into scalar values.

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F, types as T
from pyspark.sql.types import TimestampType 

def process_requests(requests_raw: DataFrame) -> DataFrame:
    """
    Processes a stream of raw requests and:
        - Unpacks JSON payloads for requests
        - Extracts relevant features as scalar values (first element of each array)
        - Converts Unix epoch millisecond timestamps to Spark TimestampType
    """
    # Calculate the current timestamp in seconds
    current_ts = int(spark.sql("SELECT unix_timestamp(current_timestamp())").collect()[0][0])

    # Define the start timestamp for 30 days ago
    start_ts = current_ts - 30 * 24 * 60 * 60  # 30 days in seconds

    # Dynamically calculate the min and max values of timestamp_ms
    min_max = requests_raw.agg(
        F.min("timestamp_ms").alias("min_ts"),
        F.max("timestamp_ms").alias("max_ts")
    ).collect()[0]

    min_ts = min_max["min_ts"] / 1000  # Convert from milliseconds to seconds
    max_ts = min_max["max_ts"] / 1000  # Convert from milliseconds to seconds

    # Transform timestamp_ms to span the last month
    requests_timestamped = requests_raw.withColumn(
        'timestamp', 
        (start_ts + ((F.col("timestamp_ms") / 1000 - min_ts) / (max_ts - min_ts)) * (current_ts - start_ts)).cast(TimestampType())
    ).drop("timestamp_ms")

    # Unpack JSON for the 'request' column only, since 'response' is already structured
    requests_unpacked = requests_timestamped \
        .withColumn("request", json_consolidation_udf(F.col("request"))) \
        .withColumn('request', F.from_json(F.col("request"), F.schema_of_json(
            '[{"HighBP": 1.0, "HighChol": 0.0, "CholCheck": 1.0, "BMI": 26.0, "Smoker": 0.0, "Stroke": 0.0, "HeartDiseaseorAttack": 0.0, "PhysActivity": 1.0, "Fruits": 0.0, "Veggies": 1.0, "HvyAlcoholConsump": 0.0, "AnyHealthcare": 1.0, "NoDocbcCost": 0.0, "GenHlth": 3.0, "MentHlth": 5.0, "PhysHlth": 30.0, "DiffWalk": 0.0, "Sex": 1.0, "Age": 4.0, "Education": 6.0, "Income": 8.0,"id": 1}]'))) 

    # Extract feature columns as scalar values (first element of each array)
    feature_columns = ["HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke", "HeartDiseaseorAttack",
                       "PhysActivity", "Fruits", "Veggies", "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost",
                       "GenHlth", "MentHlth", "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income", "id"]

    for col_name in feature_columns:
        # Extract the first element of each array for all feature columns
        requests_unpacked = requests_unpacked.withColumn(col_name, F.col(f"request.{col_name}")[0])

    # Extract predictions from the 'response' column without using from_json
    requests_unpacked = requests_unpacked.withColumn("Diabetes_binary", F.col("response.predictions")[0])

    # Drop unnecessary columns
    requests_cleaned = requests_unpacked.drop("request", "response")

    # Add a placeholder 'model_id' column
    final_df = requests_cleaned.withColumn("model_id", F.lit(0).cast(T.IntegerType()))

    return final_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analyzing Processed Requests
# MAGIC
# MAGIC After processing and unpacking the logged data from the inference table, the next step is to analyze the successfully answered requests by the model. This involves filtering the data to focus on successful interactions, merging with additional information, and thoroughly examining the model’s performance.
# MAGIC
# MAGIC **Steps for Analyzing Processed Requests:**
# MAGIC 1. **Filtering Requests**: Initially, we filter out the requests to only include those with a successful status code (200). This ensures that we are analyzing only the requests where the model was able to generate predictions.
# MAGIC 2. **Displaying Logs**: The filtered logs are then displayed, providing a clear view of the processed requests and their outcomes.
# MAGIC 3. **Merging Data**: We also merge these logs with additional label data that categorizes the results. This step is crucial for evaluating the model's performance against known outcomes.
# MAGIC 4. **Final Display**: The merged DataFrame is displayed, showing the complete information of the requests along with their corresponding labels. This provides a full picture of how the model is performing in real-world scenarios.
# MAGIC
# MAGIC This detailed view helps in understanding the effectiveness of the model and in making necessary adjustments based on real-world data feedback.
# MAGIC

# COMMAND ----------

# Apply the function to the inference DataFrame
model_logs_df = process_requests(inference_df.where("status_code = 200"))

# Display the updated DataFrame to confirm
model_logs_df.display()

# COMMAND ----------

# Rename the column in the pandas DataFrame and convert it to a Spark DataFrame
label_pd_df = diabetes_df_pd.rename(columns={'Diabetes_binary': 'labeled_data'})
label_pd_df = spark.createDataFrame(label_pd_df)

# Perform the join operation
model_logs_df_labeled = model_logs_df.join(
    label_pd_df.select("id", "labeled_data"), 
    on=["id"], 
    how="left"
).drop("id")

# Display the result
display(model_logs_df_labeled)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persisting Processed Model Logs
# MAGIC
# MAGIC After analyzing the model's responses and merging them with relevant labels, the final step involves saving these enriched logs for long-term monitoring and analysis. This step is critical for maintaining a historical record of model performance and enabling further analytical studies.
# MAGIC
# MAGIC ### Steps for Saving Model Logs:
# MAGIC 1. **Preparing the Data**: The processed and labeled model logs are prepared for storage. At this point, the data includes all necessary details, such as predictions, request metadata, and corresponding labels.
# MAGIC 2. **Setting the Storage Mode**: We use the `append` mode when saving the DataFrame. This approach ensures that new entries are added to the existing dataset without overwriting previous logs, thereby accumulating a comprehensive log over time.
# MAGIC 3. **Saving the DataFrame**: The logs are saved as a table in the Databricks catalog under the specified catalog name and schema. Organizing data in this structured manner facilitates efficient management and easy retrieval for future analysis and monitoring.
# MAGIC 4. **Confirming Save Operation**: Finally, the operation concludes with the confirmation that the logs have been successfully appended to the designated table, indicating successful data persistence.
# MAGIC
# MAGIC By systematically saving these logs, we establish a robust foundation for ongoing monitoring of the model's performance. This enables proactive management, detailed analysis, and continuous optimization of machine learning operations.

# COMMAND ----------

model_logs_df_labeled.write.mode("append").saveAsTable(f'{DA.catalog_name}.{DA.schema_name}.demo_model_logs')

# COMMAND ----------

# MAGIC %md
# MAGIC For efficient execution, enable CDF so monitoring can incrementally process the data.

# COMMAND ----------

spark.sql(f'ALTER TABLE {DA.catalog_name}.{DA.schema_name}.demo_model_logs SET TBLPROPERTIES (delta.enableChangeDataFeed = true)')

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Creating an Inference Monitor with Databricks Lakehouse Monitoring
# MAGIC
# MAGIC Once your model logs are saved and structured for analysis, the next essential step is setting up monitoring to continuously track model performance and detect any anomalies or drift in real-time. You can set up an inference monitor using Databricks Lakehouse Monitoring through two approaches both methods will enable you to monitor your model's performance efficiently, ensuring that any necessary adjustments or retraining can be handled promptly.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1: Using the Notebook
# MAGIC For those who are comfortable with scripting and want more control over the monitoring setup, you can use the provided notebook commands to configure and initiate the monitoring of your model logs. This method allows you to automate and customize the monitoring according to specific needs and thresholds.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType, MonitorInfoStatus, MonitorRefreshInfoState, MonitorMetric
w = WorkspaceClient()
Table_name = f'{DA.catalog_name}.{DA.schema_name}.demo_model_logs'
Baseline_table_name = f"{DA.catalog_name}.{DA.schema_name}.demo_baseline_features"

# COMMAND ----------

help(w.quality_monitors.create)

# COMMAND ----------

#ML problem type, either "classification" or "regression"
PROBLEM_TYPE = MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION

# Window sizes to analyze data over
GRANULARITIES = ["1 day"]   

# Directory to store generated dashboard
ASSETS_DIR = f"/Workspace/Users/{DA.username}/databricks_lakehouse_monitoring/demo_model_logs"

# Optional parameters
SLICING_EXPRS = ["Age < 2", "Age > 15", "Sex = 1", "HighChol = 1"]   # Expressions to slice data with

# COMMAND ----------

print(f"Creating monitor for demo_model_logs")

info = w.quality_monitors.create(
  table_name=Table_name,
  inference_log=MonitorInferenceLog(
    timestamp_col='timestamp',
    granularities=GRANULARITIES,
    model_id_col='model_id',  # Model version number 
    prediction_col='Diabetes_binary',  # Ensure this column is of type DOUBLE
    problem_type=PROBLEM_TYPE,
    label_col='labeled_data'  # Ensure this column is of type DOUBLE
  ),
  baseline_table_name=Baseline_table_name,
  slicing_exprs=SLICING_EXPRS,
  output_schema_name=f"{DA.catalog_name}.{DA.schema_name}",
  assets_dir=ASSETS_DIR
)

# COMMAND ----------

import time

# Wait for monitor to be created
while info.status ==  MonitorInfoStatus.MONITOR_STATUS_PENDING:
  info = w.quality_monitors.get(table_name=Table_name)
  time.sleep(10)

assert info.status == MonitorInfoStatus.MONITOR_STATUS_ACTIVE, "Error creating monitor"

# COMMAND ----------

# A metric refresh will automatically be triggered on creation
refreshes = w.quality_monitors.list_refreshes(table_name=Table_name).refreshes
assert(len(refreshes) > 0)

run_info = refreshes[0]
while run_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  run_info = w.quality_monitors.get_refresh(table_name=Table_name, refresh_id=run_info.refresh_id)
  time.sleep(30)

assert run_info.state == MonitorRefreshInfoState.SUCCESS, "Monitor refresh failed"

# COMMAND ----------

print(w.quality_monitors.get(table_name=Table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC Click the highlighted Dashboard link in the cell output to open the dashboard. You can also navigate to the dashboard from the Catalog Explorer UI.

# COMMAND ----------

import joblib

mdl_raw = joblib.load('/Volumes/dbacademy/labuser8722713_1755799855/input/iris_model_svm.pkl')

type(mdl_raw)

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")

mlflow.end_run()
with mlflow.start_run():
    mlflow.sklearn.log_model(
        sk_model=svm_mdl,
        artifact_path="svm_model"
    )

# COMMAND ----------



# COMMAND ----------

svm_mlflow = mlflow.sklearn.load_model(mdl_raw)

# COMMAND ----------

svm_mdl = mlflow.sklearn.load_model("dbfs:/Volumes/dbacademy/labuser8722713_1755799855/input/iris_model.pkl")

# COMMAND ----------



# COMMAND ----------

# Extract workspace URL
workspace_url = spark.conf.get('spark.databricks.workspaceUrl')

# Construct the monitor dashboard URL
monitor_dashboard_url = f"https://{workspace_url}/explore/data/{DA.catalog_name}/{DA.schema_name}/demo_model_logs?o={DA.schema_name}&activeTab=quality"

print(f"Monitor Dashboard URL: {monitor_dashboard_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect the Metrics Tables
# MAGIC
# MAGIC By default, the metrics tables are saved in the default database.  
# MAGIC
# MAGIC The `create_monitor` call created two new tables: the profile metrics table and the drift metrics table. 
# MAGIC
# MAGIC These two tables record the outputs of analysis jobs. The tables use the same name as the primary table to be monitored, with the suffixes `_profile_metrics` and `_drift_metrics`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orientation to the Profile Metrics Table
# MAGIC
# MAGIC The profile metrics table has the suffix `_profile_metrics`. For a list of statistics that are shown in the table, see the documentation ([AWS](https://docs.databricks.com/lakehouse-monitoring/monitor-output.html#profile-metrics-table)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/monitor-output#profile-metrics-table)).
# MAGIC
# MAGIC - For every column in the primary table, the profile table shows summary statistics for the baseline table and for the primary table. The column `log_type` shows `INPUT` to indicate statistics for the primary table, and `BASELINE` to indicate statistics for the baseline table. The column from the primary table is identified in the column `column_name`.
# MAGIC - For `TimeSeries` type analysis, the `granularity` column shows the granularity corresponding to the row. For baseline table statistics, the `granularity` column shows `null`.
# MAGIC - The table shows statistics for each value of each slice key in each time window, and for the table as whole. Statistics for the table as a whole are indicated by `slice_key` = `slice_value` = `null`.
# MAGIC - In the primary table, the `window` column shows the time window corresponding to that row. For baseline table statistics, the `window` column shows `null`.  
# MAGIC - Some statistics are calculated based on the table as a whole, not on a single column. In the column `column_name`, these statistics are identified by `:table`.

# COMMAND ----------

# Display profile metrics table
profile_table = f"{DA.catalog_name}.{DA.schema_name}.demo_model_logs_profile_metrics"
profile_df = spark.sql(f"SELECT * FROM {profile_table}")
display(profile_df.orderBy(F.rand()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Orientation to the Drift Metrics Table
# MAGIC
# MAGIC The drift metrics table has the suffix `_drift_metrics`. For a list of statistics that are shown in the table, see the documentation ([AWS](https://docs.databricks.com/lakehouse-monitoring/monitor-output.html#drift-metrics-table)|[Azure](https://learn.microsoft.com/azure/databricks/lakehouse-monitoring/monitor-output#drift-metrics-table)).
# MAGIC
# MAGIC - For every column in the primary table, the drift table shows a set of metrics that compare the current values in the table to the values at the time of the previous analysis run and to the baseline table. The column `drift_type` shows `BASELINE` to indicate drift relative to the baseline table, and `CONSECUTIVE` to indicate drift relative to a previous time window. As in the profile table, the column from the primary table is identified in the column `column_name`.
# MAGIC   - At this point, because this is the first run of this monitor, there is no previous window to compare to. So there are no rows where `drift_type` is `CONSECUTIVE`. 
# MAGIC - For `TimeSeries` type analysis, the `granularity` column shows the granularity corresponding to that row.
# MAGIC - The table shows statistics for each value of each slice key in each time window, and for the table as whole. Statistics for the table as a whole are indicated by `slice_key` = `slice_value` = `null`.
# MAGIC - The `window` column shows the time window corresponding to that row. The `window_cmp` column shows the comparison window. If the comparison is to the baseline table, `window_cmp` is `null`.  
# MAGIC - Some statistics are calculated based on the table as a whole, not on a single column. In the column `column_name`, these statistics are identified by `:table`.

# COMMAND ----------

# Display the drift metrics table
drift_table = f"{DA.catalog_name}.{DA.schema_name}.demo_model_logs_drift_metrics"
display(spark.sql(f"SELECT * FROM {drift_table} ORDER BY RAND() LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Look at fairness and bias metrics
# MAGIC Fairness and bias metrics are calculated for boolean type slices that were defined. The group defined by `slice_value=true` is considered the protected group ([AWS](https://docs.databricks.com/en/lakehouse-monitoring/fairness-bias.html)|[Azure](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-monitoring/fairness-bias)).

# COMMAND ----------

fb_cols = ["window", "model_id", "slice_key", "slice_value", "predictive_parity", "predictive_equality", "equal_opportunity", "statistical_parity"]
fb_metrics_df = profile_df.select(fb_cols).filter(f"column_name = ':table' AND slice_value = 'true'")
display(fb_metrics_df.orderBy(F.rand()).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Using the UI
# MAGIC If you prefer a graphical interface for setup, Databricks offers a user-friendly UI that guides you step-by-step to create and configure the inference monitor. This method is straightforward and ideal for those who wish to quickly set up monitoring without writing code.

# COMMAND ----------

print(f'Your catalog name is: {DA.catalog_name}')
print(f'Your schema name is: {DA.schema_name}')
print(f'Your baseline table name is: {DA.catalog_name}.{DA.schema_name}.demo_baseline_features')

# COMMAND ----------

# MAGIC %md
# MAGIC To add a monitor on the log table, 
# MAGIC
# MAGIC 1. Open the **Catalog** menu from the left menu bar.
# MAGIC
# MAGIC 1. Select the table **demo_model_logs** within your catalog and schema. 
# MAGIC
# MAGIC 1. Click on the **Quality** tab then on the **Get started** button.
# MAGIC
# MAGIC 1. As **Profile type** select **Inference profile**.
# MAGIC
# MAGIC 1. As **Problem type** select **classification**.
# MAGIC
# MAGIC 1. As the **Prediction column** select **Diabetes_binary**.
# MAGIC
# MAGIC 1. As the **Label column** select **labeled_data**
# MAGIC
# MAGIC 1. As **Metric granularities** select **5 minutes**, **1 hour**, and **1 day**. We will use the doctored timestamps to simulate requests that have been received over a large period of time. 
# MAGIC
# MAGIC 1. As **Timestamp column** select **timestamp**.
# MAGIC
# MAGIC 1. As **Model ID column** select **model_id**.
# MAGIC
# MAGIC 1. In Advanced Options --> **Unity catalog baseline table name (optional)** enter **Your baseline table name** from above
# MAGIC
# MAGIC 1. Click the **Create** button.

# COMMAND ----------

# MAGIC %md
# MAGIC **⚠️ Note: You may see a CAST_INVALID_INPUT error in the Numerical Feature Quantile Drift widget. This occurs because the automatically generated query attempts to cast the "Baseline" string to a timestamp in the Window column. This doesn’t affect other metrics or drift detection functionality and can be safely ignored for this demo.**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Handling Drift Detection and Triggering Retraining `(Additional)`
# MAGIC
# MAGIC Handle drift detection by analyzing the drift metrics, and trigger retraining and redeployment if drift is detected.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC - **Analyze Metrics:** Load and analyze the drift metrics table to detect any drift in data or model performance.
# MAGIC - **Trigger Retraining:** Retrain the model if drift is detected.
# MAGIC - **Trigger Redeployment:** Redeploy the model if retraining is triggered.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Analyze Drift Metrics
# MAGIC Analyze the drift metrics to determine if there is significant drift. Here is a sample analysis approach:

# COMMAND ----------

import pandas as pd
import json

# Load the drift metrics data from the Delta table
drift_table = f"{DA.catalog_name}.{DA.schema_name}.demo_model_logs_drift_metrics"
drift_metrics_df = spark.read.table(drift_table)

# Convert to Pandas DataFrame
data = drift_metrics_df.toPandas()

# Convert Timestamp objects to strings in 'window' and 'window_cmp'
def convert_timestamp_to_string(d):
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, pd.Timestamp):
                d[k] = v.isoformat()
            elif isinstance(v, dict):
                d[k] = convert_timestamp_to_string(v)
    return d

data['window'] = data['window'].apply(convert_timestamp_to_string)
data['window_cmp'] = data['window_cmp'].apply(convert_timestamp_to_string)

# Ensure JSON fields are strings
data['window'] = data['window'].apply(json.dumps)
data['window_cmp'] = data['window_cmp'].apply(json.dumps)
data['ks_test'] = data['ks_test'].apply(json.dumps) if data['ks_test'].notna().all() else None
data['chi_squared_test'] = data['chi_squared_test'].apply(json.dumps) if data['chi_squared_test'].notna().all() else None

# Convert the JSON string in 'window' and 'window_cmp' to dictionaries
for index, row in data.iterrows():
    row['window'] = json.loads(row['window'])
    row['window_cmp'] = json.loads(row['window_cmp'])
    row['ks_test'] = json.loads(row['ks_test']) if row['ks_test'] else None
    row['chi_squared_test'] = json.loads(row['chi_squared_test']) if row['chi_squared_test'] else None

# Analyze the drift metrics
drift_thresholds = {
    "js_distance": 0.7,
    "ks_statistic": 0.4,
    "ks_pvalue": 0.05,
    "wasserstein_distance": 0.7,
    "population_stability_index": 0.7,
    "chi_squared_statistic": 0.4,
    "chi_squared_pvalue": 0.05,
    "tv_distance": 0.7,
    "l_infinity_distance": 0.7
}

def check_drift(row):
    if row['js_distance'] is not None and row['js_distance'] > drift_thresholds['js_distance']:
        return True
    ks_test = row['ks_test']
    if ks_test and ks_test['statistic'] > drift_thresholds['ks_statistic'] and ks_test['pvalue'] < drift_thresholds['ks_pvalue']:
        return True
    if row['wasserstein_distance'] is not None and row['wasserstein_distance'] > drift_thresholds['wasserstein_distance']:
        return True
    if row['population_stability_index'] is not None and row['population_stability_index'] > drift_thresholds['population_stability_index']:
        return True
    chi_squared_test = row['chi_squared_test']
    if chi_squared_test and chi_squared_test['statistic'] > drift_thresholds['chi_squared_statistic'] and chi_squared_test['pvalue'] < drift_thresholds['chi_squared_pvalue']:
        return True
    if row['tv_distance'] is not None and row['tv_distance'] > drift_thresholds['tv_distance']:
        return True
    if row['l_infinity_distance'] is not None and row['l_infinity_distance'] > drift_thresholds['l_infinity_distance']:
        return True
    return False

data['drift_detected'] = data.apply(check_drift, axis=1)

# Display rows with drift detected
drifted_rows = data[data['drift_detected']]
no_drifted_rows = data[~data['drift_detected']]

print("Rows with drift detected:")
print(drifted_rows)

print("\nRows with no drift detected:")
print(no_drifted_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trigger Retraining and Redeployment
# MAGIC If drift is detected, retrain the model and redeploy it using the same steps from the initial model preparation notebook.
# MAGIC
# MAGIC - **Retrain and Log the Model**
# MAGIC
# MAGIC > - Retrain the model using DecisionTreeClassifier and log the new version with MLflow.

# COMMAND ----------

# Retrain and redeploy the model if drift is detected
if not drifted_rows.empty:
    print("Drift detected. Retraining and redeploying the model...")

    # Prepare data for retraining
    dtc = DecisionTreeClassifier()
    dtc_mdl = dtc.fit(X_train, y_train)

    model_name = model_name
    
    # Retrain the model
    dtc_mdl = dtc.fit(X_train, y_train)
    signature = infer_signature(X_train, y_train)
    
    mlflow.sklearn.log_model(
        sk_model=dtc_mdl, 
        artifact_path="model-artifacts",
        signature=signature,
        registered_model_name=model_name
    )

# COMMAND ----------

# MAGIC %md
# MAGIC # Explore the dashboard!

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC In this demo, we successfully processed and analyzed model inference logs to set up a monitoring framework for tracking the model's performance. We utilized inference data to detect any anomalies or drift and explored how to use Databricks Lakehouse Monitoring to continuously track and alert on key model performance metrics. Additionally, we demonstrated how to handle drift detection and initiate retraining processes to maintain optimal model performance over time. This comprehensive approach ensures that machine learning models remain reliable and effective, allowing for proactive management and continuous improvement.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
