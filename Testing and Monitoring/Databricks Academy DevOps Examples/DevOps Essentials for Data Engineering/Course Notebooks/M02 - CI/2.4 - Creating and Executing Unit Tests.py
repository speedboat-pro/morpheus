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
# MAGIC # 2.4 - Creating and Executing Unit Tests
# MAGIC
# MAGIC A unit test is a type of software testing that focuses on verifying the smallest parts of an application, typically individual functions or methods in isolation. The goal is to ensure that each unit of code works as expected, producing the correct output for a given input. Unit tests are typically automated and run frequently during development to catch bugs early and maintain code quality.
# MAGIC
# MAGIC ## Objectives
# MAGIC
# MAGIC - Write simple unit tests within a notebook to verify the functionality of the code.
# MAGIC - Store unit tests in an external .py file, evaluating the advantages of externalizing tests for maintainability.
# MAGIC - Use the `pytest` package to automatically discover and execute test functions.

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
# MAGIC ## A. Create Simple Unit Tests in a Databricks Notebook
# MAGIC
# MAGIC Let's perform simple unit tests on the functions we created in the previous demonstration. To create a unit test for a function:
# MAGIC
# MAGIC   **a. Create the Initial Function**  
# MAGIC   Create the initial function you want to use in production. Ensure it has clear inputs and outputs that can be easily validated.
# MAGIC
# MAGIC **b. Create One or More Test Functions**  
# MAGIC Write a unit test function (or multiple functions to test different expectations) that calls the initial function with sample inputs and checks if the actual function output matches the defined expected result using assertions. Name the test function with the keyword `test_` followed by a description of what you are testing.  
# MAGIC
# MAGIC **c. Execute the Unit Test Functions**  
# MAGIC Run and review the test using a testing framework like `pytest`. Review the results to ensure the function behaves as expected, and fix any issues if the test fails.
# MAGIC
# MAGIC **NOTE:** There are many available frameworks you can use. We will use `pytest` in this course. The framework you choose should be discussed with your team or organization.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Complete the following to view the custom functions for this project:
# MAGIC
# MAGIC    a. Navigate to the main course folder **DevOps Essentials for Data Engineering**.  
# MAGIC    
# MAGIC    b. Select **src**.  
# MAGIC
# MAGIC    c. Then select **helpers**.  
# MAGIC
# MAGIC    d. Right-click on **[project_functions.py]($../../src/helpers/project_functions.py)**  and select *Open in new tab*.  
# MAGIC
# MAGIC    e. Review the **project_functions.py** file. Notice it contains the following functions that we saw in the previous notebook: 
# MAGIC
# MAGIC      - `get_health_csv_schema` - Returns the schema for the health data CSVs.  
# MAGIC
# MAGIC      - `highcholest_map` - Maps a cholesterol value to a categorical label.  
# MAGIC      
# MAGIC      - `group_ages_map` - Maps an age value to an age group category.  
# MAGIC
# MAGIC    f. Close the **project_functions.py** tab.
# MAGIC    
# MAGIC    g. Run the cell below to import the custom functions from the **helpers** module.
# MAGIC
# MAGIC **NOTE:** `sys.path.append()` Adds the root folder path to the system path (this allows you to import modules from this folder).

# COMMAND ----------

# Adds the parent directory of the current working directory to the Python search path
import sys
import os

# Get the current working directory
current_path = os.getcwd()

# Get the root folder path by navigating two levels up from the current path
root_folder_path = os.path.dirname(os.path.dirname(current_path))

# Add the root folder path to the system path (this allows you to import modules from this folder)
sys.path.append(root_folder_path)

# Print a message confirming the root folder path added to sys.path
print(f'Add the following path: {root_folder_path}')

# COMMAND ----------

## Import the custom functions
from src.helpers import project_functions

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Create the test function `test_get_health_csv_schema_match` to test the `get_health_csv_schema` function from above. 
# MAGIC
# MAGIC       Within the test function:
# MAGIC
# MAGIC    - The variable **actual_schema** holds the schema result from the `get_health_csv_schema` function.
# MAGIC
# MAGIC    - The variable **expected_schema** specifies the schema we expect the function to return if it works as expected.
# MAGIC
# MAGIC    - The `assertSchemaEqual` statement from `pyspark.testing.utils` compares the **expected_schema** with **actual_schema**. If the values are not equal, an error will be raised.
# MAGIC
# MAGIC
# MAGIC    Run the cell and confirm that no errors are returned. This shows that the `get_health_csv_schema` function is working as expected.
# MAGIC
# MAGIC    [Pytest Testing Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.testing.html#testing)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, LongType
from pyspark.sql.functions import when, col

from pyspark.testing.utils import assertSchemaEqual

def test_get_health_csv_schema_match():

    # Get schema from our function
    actual_schema = project_functions.get_health_csv_schema()
    
    # Define the expected schema that the function should return. If that function is changed during development the unit test will pick up the error and the test will fail.
    expected_schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("PII", StringType(), True),
        StructField("date", DateType(), True),
        StructField("HighCholest", IntegerType(), True),
        StructField("HighBP", DoubleType(), True),
        StructField("BMI", DoubleType(), True),
        StructField("Age", DoubleType(), True),
        StructField("Education", DoubleType(), True),
        StructField("income", IntegerType(), True)
    ])

    # Assert the actual schema matches the expected schema
    assertSchemaEqual(actual_schema, expected_schema)
    print('Test passed!')

test_get_health_csv_schema_match()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Create the test function `test_high_cholest_column_valid_map` to test the `highcholest_map` function from above. Within the test function:
# MAGIC
# MAGIC    - Import the `assertDataFrameEqual` function from the `pyspark.testing.utils` module to compare two PySpark DataFrames and assert that they are equal, checking both the data and schema.
# MAGIC    
# MAGIC    - The variable **actual_df** holds the new column created from the result of the `highcholest_map` function.
# MAGIC    
# MAGIC    - The variable **expected_df** contains the expected dataframe we anticipate from the function.
# MAGIC    
# MAGIC    - The `assertDataFrameEqual` function from `pyspark.testing.utils` compares the dataframes **actual_df** and **expected_df**. If the data or schema are not equal, an error will be raised.
# MAGIC
# MAGIC    Run the cell and confirm that no errors are returned. This shows that the `highcholest_map` function is working as expected.
# MAGIC
# MAGIC    [Pytest Testing Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.testing.html#testing)

# COMMAND ----------

## Import assertDataFrameEqual to compare your DataFrames
from pyspark.testing.utils import assertDataFrameEqual

def test_high_cholest_column_valid_map():

    # Create the sample DataFrame to test the function
    data = [
        (0,),
        (1,), 
        (2,), 
        (3,), 
        (4,), 
        (None,)
    ]
    sample_df = spark.createDataFrame(data, ["value"])

    # Apply the function on the sample data
    actual_df = sample_df.withColumn("actual", project_functions.high_cholest_map("value"))

    # Create a static DataFrame with the expected results of the highcholest_map function above
    expected_df = spark.createDataFrame(
        [
            (0, "Normal"),
            (1, "Above Average"),
            (2, "High"),
            (3, "Unknown"),
            (4, "Unknown"),
            (None, "Unknown")
        ],
        schema=StructType([
            StructField("value", LongType(), True),
            StructField("actual", StringType(), True)
        ])
    )

    ## Check to make sure the column in the sample dataframe and expected dataframe are the same. If not equal an error will be returned.
    assertDataFrameEqual(actual_df, expected_df)
    print('Test passed!')

test_high_cholest_column_valid_map()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. In cell below the **expected_df** variable has been modified from `(0, "Normal")` to `(0, "Bad Value Cause Error")`. This will cause the unit test to fail because the expected results were incorrectly specified and will not match the **actual_df**.
# MAGIC
# MAGIC     Run the unit test below. Notice that the test function returns an error because the dataframes don't match. 
# MAGIC     
# MAGIC **NOTE:** If your unit tests fail you will need to assess where the failure occurred and fix it.

# COMMAND ----------

################################################################################
## Cell causes an error because the actual df does not match the expected df
################################################################################

def test_high_cholest_column_invalid_map():

    # Create the sample DataFrame to test the function on
    data = [
        (0,),
        (1,), 
        (2,), 
        (3,), 
        (4,), 
        (None,)
    ]
    sample_df = spark.createDataFrame(data, ["value"])

    # Apply the function on the sample data
    actual_df = sample_df.withColumn("actual", project_functions.high_cholest_map("value"))

    # Create a static DataFrame with the expected results of the highcholest_map function above
    expected_df = spark.createDataFrame(
        [
            (0, "Bad Value Cause Error"),     ####### <--- Value has been changed to cause an error
            (1, "Above Average"),
            (2, "High"),
            (3, "Unknown"),
            (4, "Unknown"),
            (None, "Unknown")
        ],
        schema=StructType([
            StructField("value", LongType(), True),
            StructField("actual", StringType(), True)
        ])
    )

    ## Check to make sure the column in the sample dataframe and expected dataframe are the same. If not equal an error will be returned.
    assertDataFrameEqual(actual_df, expected_df)

test_high_cholest_column_invalid_map()

# COMMAND ----------

# MAGIC %md
# MAGIC Developing good unit tests are important because they help ensure individual components of your code work as expected, catching errors early in the development process. This leads to more reliable, maintainable code and can save time in debugging later on.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Create a File for the Unit Tests
# MAGIC
# MAGIC In the previous cells, we implemented our unit tests within the Databricks notebook.
# MAGIC
# MAGIC Typically, unit tests are placed in a separate location, such as a **tests/unit_tests/tests_file_name.py** file (or in multiple Python files), to keep them separate from the main code and maintain a clean project structure. A testing framework is then used to execute the tests.
# MAGIC
# MAGIC This approach makes it easier to manage, update, and run tests without affecting the application’s core functionality.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Let's start by importing our testing framework package `pytest` on the cluster.
# MAGIC
# MAGIC     [pytest documentation](https://docs.pytest.org/en/stable/)

# COMMAND ----------

!pip install pytest==8.3.4

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete the following steps to view how to define the unit test in your Python file for this project. The unit test functions have already been placed in main course folder in the **tests/unit_tests/test_spark_helper_functions.py** file for you.
# MAGIC
# MAGIC     a. Navigate to the main course folder **DevOps Essentials for Data Engineering**.  
# MAGIC    
# MAGIC     b. Open the **tests/unit_tests** folder.
# MAGIC
# MAGIC     c. Right-click on the **[tests/unit_tests/test_spark_helper_functions.py]($../../tests/unit_tests/test_spark_helper_functions.py)** file and select *Open in a new tab*. Review the file. 
# MAGIC     
# MAGIC     Notice the following:
# MAGIC   
# MAGIC       - On line 13, our custom `project_functions` is being imported from the `src.helpers` module.
# MAGIC
# MAGIC          **NOTE:** We are defining the Python path setting for `pytest` within the **pytest.ini** file in the main course directory. This ensures that the specified directory is added to the Python module search path when `pytest` is run. This is useful for allowing `pytest` to import code from the project root or sibling directories without needing to manually modify `sys.path`. This makes the functions available for use in the current script.
# MAGIC       
# MAGIC       - `@pytest.fixture` is a pytest fixture named **spark** with a session scope, which means it will be set up once per test session and shared across multiple test functions. It creates a SparkSession using `SparkSession.builder.getOrCreate()` (which either retrieves an existing session or creates a new one if none exists) and then yields the spark session to the test functions that use this fixture, allowing them to access the Spark environment for their tests.
# MAGIC
# MAGIC       - The remainder of the file creates the unit test functions: 
# MAGIC          - `test_get_health_csv_schema_match`
# MAGIC
# MAGIC          - `test_highcholest_column_map`
# MAGIC
# MAGIC          - `test_age_group_column_map`
# MAGIC
# MAGIC     d. After reviewing the file, close the tab.
# MAGIC
# MAGIC **NOTE:** The complexity and thoroughness of your tests depend on your organization's best practices and guidelines. This is a simple demonstration example. This course is simply introducing unit testing and will not go deep into `pytest` or any other testing framework.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3.  In the next cell use `pytest` to execute all the tests (3 tests) within the **tests/unit_tests/test_spark_helper_functions.py** file.
# MAGIC
# MAGIC     To execute pytest within Databricks complete the following:
# MAGIC
# MAGIC     - `sys.dont_write_bytecode = True` - By default, Python generates .pyc bytecode files in a __pycache__ directory. Setting  it to `True` prevents this, which is useful in read-only environments.
# MAGIC
# MAGIC     - `pytest.main()` - This is the function that starts pytest and runs the tests.
# MAGIC
# MAGIC     - `./test_simple/test_simple_functions.py` - The path to the test file you want to run.
# MAGIC
# MAGIC     - `-v` - This stands for "verbose". It tells pytest to provide more detailed output during test execution, including the names of tests and their results.
# MAGIC
# MAGIC     - `-p no:cacheprovider"` - This disables pytest's cache provider. pytest uses a caching mechanism to speed up repeated test runs, but in some cases, you may want to disable it. The no:cacheprovider option tells pytest to avoid using the cache, which may be useful if you’re running tests in an environment where caching could cause issues.
# MAGIC
# MAGIC     When executing pytest, it looks for:
# MAGIC
# MAGIC     - Test files: By default, files named **test_\*.py** or **\*_test.py**. In this example we are specifically testing the functions within the **tests/unit_tests/test_spark_helper_functions.py** file.
# MAGIC
# MAGIC     - Test functions: Functions starting with `test_*()`.
# MAGIC
# MAGIC     - For example, if you have a file like **test_example.py** with a function `test_addition()`, pytest will automatically detect and run the tests in that file.
# MAGIC  
# MAGIC     Run the cell below and view the results.
# MAGIC
# MAGIC
# MAGIC **NOTE:** Depending on your organization's best practices, you can run unit tests within Databricks or locally in the IDE of your choice based on your needs.

# COMMAND ----------

## import the `pytest` and `sys` modules.
import pytest
import sys

sys.dont_write_bytecode = True

retcode = pytest.main(["../../tests/unit_tests/test_spark_helper_functions.py", "-v", "-p", "no:cacheprovider"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------

# MAGIC %md
# MAGIC 4. View the output of the cell above. Notice that:
# MAGIC
# MAGIC     - `pytest` automatically discovered all the unit test functions within the **/tests/test_spark_helper_functions.py** file.
# MAGIC     
# MAGIC     - The **test_spark_helper_functions.py** file contained three test functions: 
# MAGIC       - `test_get_health_csv_schema_match()`
# MAGIC
# MAGIC       - `test_highcholest_column_map()`
# MAGIC
# MAGIC       - `test_age_group_column_map()`
# MAGIC     
# MAGIC     - All three unit tests passed.
# MAGIC
# MAGIC     For more information
# MAGIC       
# MAGIC     [Pyspark Testing Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.testing.html#testing)
# MAGIC
# MAGIC     [Testing Pyspark Documentation](https://spark.apache.org/docs/latest/api/python/getting_started/testing_pyspark.html#Testing-PySpark)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC Unit testing is important because it helps ensure that individual parts of your code (like functions or methods) work correctly. It catches bugs early, improves code reliability, and makes it easier to maintain and refactor your code with confidence, knowing that existing functionality is still working as expected. You can perform your unit testing within Databricks or locally using the IDE of your choice.
# MAGIC
# MAGIC This demonstration was a simple, quick introduction to unit testing with the `pytest` framework. There are a variety of other frameworks available, and you must decide which one works best for your team. Also, it's important to decide on unit test best practices for your team and organization.
# MAGIC
# MAGIC #### Next Steps
# MAGIC You can set up a continuous integration and continuous delivery or deployment (CI/CD) system, such as GitHub Actions, to automatically run your unit tests whenever your code changes. For an example, see the coverage of GitHub Actions in [Software engineering best practices for notebooks](https://docs.databricks.com/en/notebooks/best-practices.html). 
# MAGIC
# MAGIC
# MAGIC #### Additional Unit Testing Resources
# MAGIC - [pytest](https://docs.pytest.org/en/stable/)
# MAGIC - [chispa](https://github.com/MrPowers/chispa)
# MAGIC - [nutter](https://github.com/microsoft/nutter)
# MAGIC - [unittest](https://docs.python.org/3/library/unittest.html)
# MAGIC - [Best Practices for Unit Testing PySpark](https://www.youtube.com/watch?v=TbWcCyP2MgE)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
