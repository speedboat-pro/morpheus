##
## The file imports pytest and a variety of other pyspark packages.
##
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType, LongType
from pyspark.sql.functions import when, col
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual


## This file needs to be executed by pytest. If you execute the .py file here an error is returned. You will have to add the path to the src.helpers modules using sys.path.append(sys.path.append('/path/to/file'))
## Import the functions from src > helpers
from src.helpers import project_functions


# This is a pytest fixture that sets up a Spark session for the tests.
# The fixture is called 'spark' and it will be set up once per test session.
@pytest.fixture(scope="session")
def spark():
    # Create a Spark session. If one already exists, reuse it.
    spark = SparkSession.builder.getOrCreate()
    
    # Pass the Spark session to the test function.
    yield spark
    # After the test, you can add cleanup code if needed.



def test_get_health_csv_schema_match():
    # Define the expected schema that the function should return and test with the
    # This is the reference schema that the function's output will be compared against.
    # If the function's behavior changes, the test will fail if the schema doesn't match.
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

    # Call the function that retrieves the schema from the health CSV.
    # This should return a schema that will be validated against the expected schema.
    actual_schema = project_functions.get_health_csv_schema()

    # Assert that the actual schema returned by the function matches the expected schema.
    # If the schemas don't match, the test will fail and indicate an issue with the function's output.
    assertSchemaEqual(actual_schema, expected_schema)




def test_high_cholest_column_valid_map(spark):
    # Define the sample data and create Spark DataFrame to test
    data = [
        (0,),
        (1,), 
        (2,), 
        (3,), 
        (4,), 
        (None,)
    ]
    sample_df = spark.createDataFrame(data, ["value"])

    # Apply the transformation on the sample data
    actual_df = sample_df.withColumn("actual", project_functions.high_cholest_map("value")) ## add the module prior to the function

    # Create a static DataFrame with the expected results of the highcholest_map function above. If the highcholest_map function is modified and does not reproduce these results, an error will be returned
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

    ## Check to make sure the colume in the sample dataframe and expected dataframe are the same. If not equal an error will be returned.
    assertDataFrameEqual(actual_df.select(col('value')), expected_df.select(col('value')))




def test_age_group_column_valid_map(spark):
    # Define the sample data and create Spark DataFrame to test
    data = [
        (0,),
        (9,), 
        (10,), 
        (20,), 
        (30,), 
        (40,),
        (50,), 
        (60,),
        (-1,), 
        (None,)
    ]
    sample_df = spark.createDataFrame(data, ["value"])

    # Apply the transformation on the sample data
    actual_df = sample_df.withColumn("actual", project_functions.group_ages_map("value"))  ## add the module prior to the function

    # Create a static DataFrame with the expected results of the highcholest_map function above. If the highcholest_map function is modified and does not reproduce these results, an error will be returned
    data = [
        (0, "0-9"),
        (9, "0-9"),
        (10, "10-19"),
        (20, "20-29"),
        (30, "30-39"),
        (40, "40-49"),
        (50, "50+"),
        (60, "50+"),
        (-1, "Unknown"),
        (None, "Unknown")  # Null value
    ]

    # Define the schema of the DataFrame
    schema = StructType([
        StructField("value", LongType(), True),
        StructField("actual", StringType(), True)
    ])

    # Create the DataFrame
    expected_df = spark.createDataFrame(data, schema)

    ## Check to make sure the colume in the sample dataframe and expected dataframe are the same. If not equal an error will be returned.
    assertDataFrameEqual(actual_df.select(col('value')), expected_df.select(col('value')))