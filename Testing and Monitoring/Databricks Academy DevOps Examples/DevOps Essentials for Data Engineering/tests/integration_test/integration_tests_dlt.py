# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Integration Tests Using Expectations
# MAGIC This DLT pipeline is a simple example pipeline that includes a few integration checks using expectations on a simple project to teach the basics. There are additional expectations you can set or unit tests you can build to streamline the project, but we're keeping it simple.
# MAGIC
# MAGIC Please check out the following resources for more information.
# MAGIC
# MAGIC - [Manage data quality with pipeline expectations](https://docs.databricks.com/en/delta-live-tables/expectations.html#manage-data-quality-with-pipeline-expectations)
# MAGIC
# MAGIC - [Expectation recommendations and advanced patterns](https://docs.databricks.com/en/delta-live-tables/expectation-patterns.html#expectation-recommendations-and-advanced-patterns)
# MAGIC
# MAGIC - [Applying software development & DevOps best practices to Delta Live Table pipelines](https://www.databricks.com/blog/applying-software-development-devops-best-practices-delta-live-table-pipelines)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtain Configuration Variable for the Target Environment
# MAGIC This path will use the configuration variable set in the DLT pipeline for **development, stage and production**.
# MAGIC
# MAGIC - If target is **development** or **stage** run all integration tests. 
# MAGIC - If target is **production**, only run the gold table integration test.

# COMMAND ----------

import dlt

## Store the target configuration environment in the variable targert
target = spark.conf.get("target")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Dictionary for Integration Test Values
# MAGIC
# MAGIC Create a dictionary containing the necessary values for integration tests in both **development** and **stage** environments. There are several approaches to achieve this, but this is a straightforward method.
# MAGIC
# MAGIC For more information, refer to the [Portable and Reusable Expectations](https://docs.databricks.com/en/delta-live-tables/expectation-patterns.html#portable-and-reusable-expectations) documentation.
# MAGIC
# MAGIC

# COMMAND ----------

## Based on the deployed target, obtain the specific validation metrics for the tables.
target_integration_tests_validation = {
    'development': {
        'health_bronze': {
            'total_rows': 7500
        },
        'health_silver': {
            'total_rows': 7500
        }
    },
    'stage': {
        'health_bronze': {
            'total_rows': 35000
        },
        'health_silver': {
            'total_rows': 35000
        }
    }
}


## Store the expected values for the total rows in the tables tables in the variables based on the target if in development or stage
if target in ('development', 'stage'):
    total_expected_bronze = target_integration_tests_validation[target]['health_bronze']['total_rows']
    total_expected_silver = target_integration_tests_validation[target]['health_silver']['total_rows']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Function to Count the Total Number of Rows in a Table
# MAGIC The `test_count_table_total_rows` function creates a materialized view that counts the total number of rows in the specified table.

# COMMAND ----------

def test_count_table_total_rows(table_name, total_count, target):
    '''
    Count the number of rows in the specified table and compare with the expected values for development and stage data. 
    Fail the update if the count does not match the specified values.
    '''
    @dlt.table(
        name=f"TEST_{target}_{table_name}_total_rows_verification",
        comment=f"Confirms all rows were ingested from the {target} raw data to {table_name}"
    )

    @dlt.expect_all_or_fail({"valid count": f"total_rows = {total_count}"}) 

    def count_table_total_rows():
        return spark.sql(f"""
            SELECT COUNT(*) AS total_rows FROM LIVE.{table_name}
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Function to Confirm the Column Values in the Gold Materialized View
# MAGIC The `test_gold_table_columns` function creates a materialized view that checks the values in the columns **Age_Group** and **HighCholest_Group** in **chol_age_agg**.

# COMMAND ----------

def test_gold_table_columns():
    '''
    This function will check unique values in the columns Age_Group and HighCholest_Group in the gold table chol_age_agg.

    This confirms that the distinct values for these columns in the gold table are correct.
    ''' 
    ## Set expectations for the columns
    check_silver_calc_columns = {
        "valid age group": "Age_Group in ('0-9', '10-19', '20-29', '30-39', '40-49', '50+', 'Unknown')",
        "valid cholest group": "HighCholest_Group in ('Normal', 'Above Average', 'High', 'Unknown')"
    }

    @dlt.table(comment="Check age group and high cholest group in the gold table")

    ## Fail if expectations are not met
    @dlt.expect_all_or_fail(check_silver_calc_columns)

    def test_calculated_columns_age_cholesterol():
        return (dlt
                .read("chol_age_agg")
                .select("Age_Group", "HighCholest_Group")
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execute the Specified Integration Tests
# MAGIC Execute the specified integration tests based on the target environment.

# COMMAND ----------

## Run the specified tests based on the target environment (development, stage or production)

if target in ('development','stage'):  ## Dynamic integration test for dev or stage tables
    test_count_table_total_rows('health_bronze',  total_expected_bronze, target)
    test_count_table_total_rows('health_silver',  total_expected_silver, target)
    test_gold_table_columns()
elif target == 'production':  ## Only test the gold table in production
    test_gold_table_columns()
