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
# MAGIC # 2.5 Lab - Create and Execute Unit Tests
# MAGIC
# MAGIC ### Estimated Duration: 15-20 minutes
# MAGIC
# MAGIC By the end of this lab, you will have practiced creating and executing unit tests for the modularized functions that were created in the previous lab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. View the Functions in the Python File

# COMMAND ----------

# MAGIC %md
# MAGIC 1. From the **./Course Notebooks/M02 - CI** folder, navigate to the file **[./src_lab/lab_functions/transforms.py]($./src_lab/lab_functions/transforms.py)**. This Python file contains the modularized functions from the previous lab. 
# MAGIC
# MAGIC     Confirm that the file contains the `convert_miles_to_km` and `uppercase_column_names` functions.
# MAGIC
# MAGIC
# MAGIC **Code in the transforms.py file:**
# MAGIC ```
# MAGIC from pyspark.sql import functions as F
# MAGIC
# MAGIC
# MAGIC def convert_miles_to_km(df, new_column_name, miles_column):
# MAGIC     return df.withColumn(new_column_name, F.round(F.col(miles_column) * 1.60934, 2))
# MAGIC
# MAGIC
# MAGIC def uppercase_columns_names(df):
# MAGIC     return df.select([F.col(col).alias(col.upper()) for col in df.columns])
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Create Unit Tests
# MAGIC
# MAGIC Create two unit tests, one for each of the functions in the file above. 
# MAGIC
# MAGIC It's typically easier to develop the unit tests within the notebook (or locally) and then move them to a separate **.py** file later to execute them with `pytest`.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Start by importing the `transforms` functions from the `lab_functions` module located in the `src_lab` directory, making them available for use in the current notebook.
# MAGIC
# MAGIC **HINT:** The **src_lab** folder is in the same directory as this notebook. You don't have to use `sys.path.append()` to append the python path. The current path is appended by default.

# COMMAND ----------

from src_lab.lab_functions import transforms

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete the unit test `test_uppercase_columns_function` function to test the custom `transforms.uppercase_column_names()` function. 
# MAGIC
# MAGIC     Use the starter code below to help guide you. After you are done, run the unit test function and confirm that it does not return an error.
# MAGIC
# MAGIC **NOTE:** There are a variety of ways to test this function too. We will keep it simple for this lab.
# MAGIC
# MAGIC **SOLUTION:** Solution can be found in the **[./tests_lab/lab_unit_test_solution.py]($./tests_lab/lab_unit_test_solution.py)** file.

# COMMAND ----------

def test_uppercase_columns_function():

    ## Fake DataFrame with random column names
    data = [(1, 5.0, 1, 1, 1, 1)]
    columns = ["id", "trip_distance", "My_Column", "WithNumbers123", "WithSymbolX@#", "With Space"]
    df = spark.createDataFrame(data, columns)

    ## Apply the transforms.uppercase_columns_names function to return the actual column names
    actual_df = transforms.uppercase_columns_names(df)
    actual_columns = actual_df.columns

    ## Create a list of the expected column names
    expected_columns = ['ID', 'TRIP_DISTANCE', 'MY_COLUMN', 'WITHNUMBERS123', 'WITHSYMBOLX@#', "WITH SPACE"]

    ## Perform a test of the actual columns names and expected column names using a simple python assert statement
    assert actual_columns == expected_columns
    print('Test Passed!')

test_uppercase_columns_function()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Complete the unit test `test_convert_miles_to_km_function` to test the custom `transforms.convert_miles_to_km` function. Use the `pyspark.testing.utils.assertDataFrameEqual` function to test the actual DataFrame against the expected DataFrame.
# MAGIC
# MAGIC     Use the starter code below to help guide you. After you are done, run the unit tests and confirm that it does not return an error.
# MAGIC
# MAGIC **NOTE:** There are a variety of unit tests you can run on the function. This is a simple example that tests the function on positive and null values. We should also test this function on negative values, but we will ignore those for this lab for simplicity.
# MAGIC
# MAGIC **HINT:** [pyspark.testing.assertDataFrameEqual](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.testing.assertDataFrameEqual.html)

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.testing.utils import assertDataFrameEqual


def test_convert_miles_to_km_function():
    # Prepare a DataFrame with sample data
    data = [(1.0,), (5.5,), (None,)]
    schema = StructType([
        StructField("trip_distance_miles", DoubleType(), True)  # Allow null values by setting nullable=True
    ])
    actual_df = spark.createDataFrame(data, schema)


    ## Apply the function on the sample data and store the actual DataFrame
    actual_df = transforms.convert_miles_to_km(df = actual_df, 
                                               new_column_name="trip_distance_km",   ## Name of the new column
                                               miles_column="trip_distance_miles")   ## Name of the source miles column


    ## Create an expected DataFrame with a defined schema using StructField DoubleType for each column
    data = [
        (1.0, 1.61),   # Row with values
        (5.5, 8.85),   # Row with values
        (None, None) # Row with null values
    ]

    ## Define schema
    schema = StructType([
        StructField("trip_distance_miles", DoubleType(), True),
        StructField("trip_distance_km", DoubleType(), True)
    ])

    ## Create expected DataFrame
    expected_df = spark.createDataFrame(data, schema)


    ## Compare the actual and expected DataFrames using assertDataFrameEqual
    assertDataFrameEqual(actual_df, expected_df)
    print('Test Passed!')


## Run the unit test
test_convert_miles_to_km_function()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Use `pytest` to Execute the Unit Tests
# MAGIC
# MAGIC Next, use `pytest` to execute the unit tests. For this portion of the lab, you can do one of the following:
# MAGIC
# MAGIC **C1. DURING A LIVE CLASS**
# MAGIC - Use `pytest` to execute the unit tests in the solution Python file that is already provided for you: **./tests_lab/lab_unit_test_solution.py**.
# MAGIC
# MAGIC **C2. CHALLENGE (COMPLETE AFTER CLASS)**
# MAGIC - Migrate your unit tests from above into a new **your-file-name.py** file in the **tests_lab/** folder, and then use `pytest` to execute your file. Make sure to add your `pytest` fixture to create a Spark session and import the necessary packages to run the unit tests.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Import the `pytest` package version 8.3.4.

# COMMAND ----------

!pip install pytest==8.3.4

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 2. If you are creating your own **.py** file for the challenge you can enable the autoreload extension to reload any imported modules automatically so that the command runs pick up those updates as you make them in the .py file. 
# MAGIC
# MAGIC     Use the following commands in any notebook cell or Python file to enable the autoreload extension.
# MAGIC
# MAGIC     Documentation: [Autoreload for Python modules](https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules)

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Execute `pytest` on the **./tests_lab/lab_unit_test_solution.py** file. Run the cell and confirm both unit tests pass.
# MAGIC
# MAGIC **NOTE:** If you are completing the challenge, modify the path to test your specific **.py** file.
# MAGIC
# MAGIC **NOTE:** In the Vocareum lab environment you might see an Error in callback message, and it is safe to ignore since it does not affect your code or test results.

# COMMAND ----------

import pytest
import sys

sys.dont_write_bytecode = True

retcode = pytest.main(["./tests_lab/lab_unit_test_solution.py", "-v", "-p", "no:cacheprovider"])

assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
