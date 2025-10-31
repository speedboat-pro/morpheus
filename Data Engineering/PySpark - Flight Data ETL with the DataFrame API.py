# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 - Flight Data ETL with the DataFrame API
# MAGIC
# MAGIC This demonstration will walk through common ETL operations using the Flights dataset. We'll cover data loading, cleaning, transformation, and analysis using the DataFrame API.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Implement common ETL operations using Spark DataFrames
# MAGIC - Handle data cleaning and type conversion
# MAGIC - Create derived features through transformations
# MAGIC - Use different column reference methods
# MAGIC - Work with User Defined Functions (UDFs)

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
# MAGIC     - In the drop-down, select **More**.
# MAGIC
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
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
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course. It will also set your default catalog to **dbacademy** and the schema to your specific schema name shown below using the `USE` statements.
# MAGIC <br></br>
# MAGIC
# MAGIC ```
# MAGIC USE CATALOG dbacademy;
# MAGIC USE SCHEMA dbacademy.<your unique schema name>;
# MAGIC ```
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-Common

# COMMAND ----------

# MAGIC %md
# MAGIC View your default catalog and schema.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog(), current_schema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flight Data Processing Requirements
# MAGIC
# MAGIC #### Source Data
# MAGIC Dataset Location: `dbacademy_airline.v01.flights_small`(flight information dataset)
# MAGIC
# MAGIC #### Target
# MAGIC Table name: flight_data
# MAGIC
# MAGIC Schema:
# MAGIC
# MAGIC | Column Name | Data Type | Description |
# MAGIC |-------------|-----------|-------------|
# MAGIC | FlightDateTime | datetime | Datetime of the flight (derived from the Year, Month, DayofMonth, DepTime fields in the source data) |
# MAGIC | FlightNum | integer | Flight number |
# MAGIC | ElapsedTimeDiff | integer | Difference between scheduled elapsed time and actual elapsed time for the flight, derived from the ActualElapsedTime and CRSElapsedTime fields in the source data |
# MAGIC | ArrDelayCategory | string | Categories include "On Time", "Slight Delay", "Moderate Delay" and "Severe Delay" based upon the value of the ArrDelay in the source data |

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Data Loading and Inspection
# MAGIC
# MAGIC First, let's load and inspect the flight data.

# COMMAND ----------

flights_df = spark.sql(f" select * from dbacademy_airline.v01.flights_small")

# COMMAND ----------

# # Read the flights data
# flights_df = spark.read.table("dbacademy_airline.v01.flights_small")

# COMMAND ----------

# Print the schema
flights_df.printSchema()

# COMMAND ----------

flights_df.schema

# COMMAND ----------

len(flights_df.columns)

# COMMAND ----------

flights_df.schema.toDDL()

# COMMAND ----------

# Visually inspect a subset of the data
display(flights_df.limit(10))

# COMMAND ----------

# Let's remove columns we dont need, remember "filter early, filter often"
flights_required_cols_df = flights_df.select(
    "Year",
    "Month",
    "DayofMonth",
    "DepTime",
    "FlightNum",
    "ActualElapsedTime",
    "CRSElapsedTime",
    "ArrDelay")

# Alternatively we could have used the drop() method to remove the columns we didnt want...

# COMMAND ----------

# Get a count of the source data records
initial_count = flights_required_cols_df.count()

print(f"Source data has {initial_count} records")

# COMMAND ----------

# Let's examine the data for invalid values, these can include nulls or invalid values for string columns "ArrDelay", "ActualElapsedTime", "DepTime" which we intend on performing mathematical operations on, we can use the Spark SQL COUNT_IF function to perform the analysis

# Register the DataFrame as a temporary SQL table with cast columns
(flights_required_cols_df 
    .selectExpr(
        "Year",
        "Month",
        "DayofMonth",
        "CAST(DepTime AS INT) AS DepTime",
        "FlightNum",
        "CAST(ActualElapsedTime AS INT) AS ActualElapsedTime",
        "CRSElapsedTime",
        "CAST(ArrDelay AS INT) AS ArrDelay"
    ) 
    .createOrReplaceTempView("flights_temp"))

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# Use Spark SQL to count null values
invalid_counts_sql = spark.sql("""
SELECT 
    COUNT_IF(Year IS NULL) AS Null_Year_Count,
    COUNT_IF(Month IS NULL) AS Null_Month_Count,
    COUNT_IF(DayofMonth IS NULL) AS Null_DayOfMonth_Count,
    COUNT_IF(DepTime IS NULL) AS Null_DepTime_Count,
    COUNT_IF(FlightNum IS NULL) AS Null_FlightNum_Count,
    COUNT_IF(ActualElapsedTime IS NULL) AS Null_ActualElapsedTime_Count,
    COUNT_IF(CRSElapsedTime IS NULL) AS Null_CRSElapsedTime_Count,
    COUNT_IF(ArrDelay IS NULL) AS Null_ArrDelay_Count
FROM flights_temp
""")

display(invalid_counts_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Comparing Spark SQL to DataFrame API Operations
# MAGIC Spark SQL DataFrame queries and their equivalent operations in the DataFrame API are evaluated to the same physical plans, let's prove this.

# COMMAND ----------

# this is the equivalent of the preceding Spark SQL query using the DataFrame API
from pyspark.sql.functions import col, sum, when

# Make sure to work with the same temporary view that the SQL is using
flights_temp_df = spark.table("flights_temp")

# Use DataFrame API to count null values
invalid_counts_df = flights_temp_df.select(
    sum(when(col("Year").isNull(), 1).otherwise(0)).alias("Null_Year_Count"),
    sum(when(col("Month").isNull(), 1).otherwise(0)).alias("Null_Month_Count"),
    sum(when(col("DayofMonth").isNull(), 1).otherwise(0)).alias("Null_DayOfMonth_Count"),
    sum(when(col("DepTime").isNull(), 1).otherwise(0)).alias("Null_DepTime_Count"),
    sum(when(col("FlightNum").isNull(), 1).otherwise(0)).alias("Null_FlightNum_Count"),
    sum(when(col("ActualElapsedTime").isNull(), 1).otherwise(0)).alias("Null_ActualElapsedTime_Count"),
    sum(when(col("CRSElapsedTime").isNull(), 1).otherwise(0)).alias("Null_CRSElapsedTime_Count"),
    sum(when(col("ArrDelay").isNull(), 1).otherwise(0)).alias("Null_ArrDelay_Count")
)

display(invalid_counts_df)

# COMMAND ----------

invalid_counts_df == invalid_counts_sql

# COMMAND ----------

# Get the explain plans for the SQL and DF versions of our query
sql_plan = invalid_counts_sql.explain() #Getting SQL Plan Details

# COMMAND ----------

df_plan = invalid_counts_df.explain() # Getting DF Plan Details

# COMMAND ----------

# Show that the two approaches evaluate to the same physical plan
sql_plan == df_plan

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Using the Databricks AI Assistant
# MAGIC The Databricks AI Assistant feature can be used to generate code or to visualize metrics from DataFrames, from the code cell below click on the __generate__ link and enter:
# MAGIC
# MAGIC ```generate a bar chart showing nulls for each column in the flights_temp_df dataframe```
# MAGIC
# MAGIC **NOTE:** Click on AI assistance toggle button and Enter the given prompt.

# COMMAND ----------

import matplotlib.pyplot as plt
import pyspark.sql.functions as F
from pyspark.sql.functions import *

# Add your AI generated code from above cell
import matplotlib.pyplot as plt

# Calculate null counts for each column
null_counts = flights_temp_df.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c)
    for c in flights_temp_df.columns
])

# Convert to Pandas for plotting
null_counts_pd = null_counts.toPandas().T
null_counts_pd.columns = ['null_count']
null_counts_pd = null_counts_pd.sort_values('null_count', ascending=False)

# Plot bar chart
plt.figure(figsize=(12, 6))
plt.bar(null_counts_pd.index, null_counts_pd['null_count'])
plt.xticks(rotation=90)
plt.ylabel('Number of Nulls')
plt.title('Nulls per Column in flights_temp_df')
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Data Cleaning
# MAGIC
# MAGIC The flights data contains some invalid and missing values, lets find them and clean them (in this case we will drop them)

# COMMAND ----------

# To drop rows where any specified columns are null, we can use the na.drop DataFrame method
non_null_flights_df = flights_required_cols_df.na.drop(
    how='any',
    subset=['CRSElapsedTime']
)

# COMMAND ----------

flights_required_cols_df.explain()

# COMMAND ----------

from pyspark.sql.functions import col

# Let's remove rows with invalid values for "ArrDelay", "ActualElapsedTime" and "DepTime" columns
flights_with_valid_data_df = non_null_flights_df.filter(
    col("ArrDelay").cast("integer").isNotNull() & 
    col("ActualElapsedTime").cast("integer").isNotNull() &
    col("DepTime").cast("integer").isNotNull()
)

# COMMAND ----------

# Now that we know "ArrDelay" and "ActualElapsedTime" contain integer values only, lets cast them from strings to integers (replacing the existing columns)
clean_flights_df = flights_with_valid_data_df \
    .withColumn("ArrDelay", col("ArrDelay").cast("integer")) \
    .withColumn("ActualElapsedTime", col("ActualElapsedTime").cast("integer"))

clean_flights_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Data Enrichment
# MAGIC
# MAGIC Now let's create a useful derived column to categorize delays.

# COMMAND ----------

# Let's start by deriving the "FlightDateTime" column from the "Year", "Month", "DayofMonth", "DepTime" columns, then drop the constituent columns
from pyspark.sql.functions import col, make_timestamp_ntz, lpad, substr, lit

flights_with_datetime_df = clean_flights_df.withColumn(
    "FlightDateTime",
    make_timestamp_ntz(
        col("Year"),
        col("Month"),
        col("DayofMonth"),
        substr(lpad(col("DepTime"), 4, "0"), lit(1), lit(2)).cast("integer"),
        substr(lpad(col("DepTime"), 4, "0"), lit(3), lit(2)).cast("integer"),
        lit(0)
    )
).drop("Year", "Month", "DayofMonth", "DepTime")

# COMMAND ----------

flights_with_datetime_df.explain()

# COMMAND ----------

# Show the result
display(flights_with_datetime_df.limit(10))

# COMMAND ----------

# Lets derive the "ElapsedTimeDiff" column from the "ActualElapsedTime" and "CRSElapsedTime" columns

from pyspark.sql.functions import col

flights_with_elapsed_time_diff_df = flights_with_datetime_df.withColumn(
    "ElapsedTimeDiff", col("ActualElapsedTime") - col("CRSElapsedTime")
    ).drop("ActualElapsedTime", "CRSElapsedTime")

# COMMAND ----------

display(flights_with_elapsed_time_diff_df.limit(10))

# COMMAND ----------

# Now lets categorize the "ArrDelay" column into categories: "On Time", "Slight Delay", "Moderate Delay", "Severe Delay"

from pyspark.sql.functions import when

enriched_flights_df = flights_with_elapsed_time_diff_df \
    .withColumn("delay_category", when(col("ArrDelay") <= 0, "On Time")
        .when(col("ArrDelay") <= 15, "Slight Delay")
        .when(col("ArrDelay") <= 60, "Moderate Delay")
        .otherwise("Severe Delay")) \
       .drop("ArrDelay")

# COMMAND ----------

display(enriched_flights_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Analyze Delays
# MAGIC
# MAGIC Let's analyze our delay categories using various column referencing approaches.

# COMMAND ----------

# Direct reference to list 100 random records
display(enriched_flights_df.select("FlightNum", "delay_category").limit(100))

# COMMAND ----------

# Column object
display(enriched_flights_df.select(col("FlightNum").alias("carrier_code"), col("delay_category")).limit(100))

# COMMAND ----------

# String expressions
display(enriched_flights_df.selectExpr("FlightNum", "ElapsedTimeDiff", "ElapsedTimeDiff > 0 as LongerThanScheduled"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Working with UDFs
# MAGIC
# MAGIC Let's use a vectorized UDF to calculate the z-score (standard deviations from the mean) for delays for each flight

# COMMAND ----------

from pyspark.sql.functions import pandas_udf

# Pandas UDF (vectorized)
@pandas_udf("double")
def normalized_diff(diff_series):
    return (diff_series - diff_series.mean()) / diff_series.std()

# Apply both UDFs
udf_example = enriched_flights_df \
    .withColumn("diff_normalized", normalized_diff("ElapsedTimeDiff"))

display(udf_example)

# Note: In practice, prefer built-in functions over UDFs when possible

# COMMAND ----------

udf_example.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Putting it altogether
# MAGIC
# MAGIC Let's put this together in a chained operation to manipulate data from a source system and save it to a new target (overwriting any existing data)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the target table in case it exists already
# MAGIC DROP TABLE IF EXISTS cleaned_and_enriched_flights;

# COMMAND ----------

from pyspark.sql.functions import col, make_timestamp_ntz, lpad, substr, lit, when, pandas_udf
# or more simply...
from pyspark.sql.functions import *

@pandas_udf("double")
def normalized_diff(diff_series):
    return (diff_series - diff_series.mean()) / diff_series.std()

(spark.read.table("dbacademy_airline.v01.flights_small")
    .selectExpr(
        "Year",
        "Month",
        "DayofMonth",
        "CAST(DepTime AS INT) AS DepTime",
        "FlightNum",
        "CAST(ActualElapsedTime AS INT) AS ActualElapsedTime",
        "CRSElapsedTime",
        "CAST(ArrDelay AS INT) AS ArrDelay"
    )
    .na.drop()
    .withColumn(
        "FlightDateTime",
        make_timestamp_ntz(
            col("Year"),
            col("Month"),
            col("DayofMonth"),
            substr(lpad(col("DepTime"), 4, "0"), lit(1), lit(2)).cast("integer"),
            substr(lpad(col("DepTime"), 4, "0"), lit(3), lit(2)).cast("integer"),
            lit(0)
        )
    )
    .drop("Year", "Month", "DayofMonth", "DepTime")
    .withColumn(
        "ElapsedTimeDiff", col("ActualElapsedTime") - col("CRSElapsedTime")
        )
    .drop("ActualElapsedTime", "CRSElapsedTime")
    .withColumn("delay_category", when(col("ArrDelay") <= 0, "On Time")
        .when(col("ArrDelay") <= 15, "Slight Delay")
        .when(col("ArrDelay") <= 60, "Moderate Delay")
        .otherwise("Severe Delay")) \
    .drop("ArrDelay")
    .withColumn("diff_normalized", normalized_diff("ElapsedTimeDiff"))
    # Write optimized
    .write
    .mode("overwrite")
    .saveAsTable("cleaned_and_enriched_flights"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_name, count(*) FROM cleaned_and_enriched_flights GROUP BY ALL;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _metadata.file_name, count(*) FROM dbacademy_airline.v01.flights_small GROUP BY ALL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Data Cleaning Best Practices**:
# MAGIC    - Validate and clean data types early
# MAGIC    - Handle missing values appropriately
# MAGIC    - Document cleaning assumptions
# MAGIC
# MAGIC 2. **Data Enrichment**:
# MAGIC    - Create meaningful derived columns
# MAGIC    - Consider business requirements
# MAGIC    - Use functions (built-in or user defined to enrich datasets)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
