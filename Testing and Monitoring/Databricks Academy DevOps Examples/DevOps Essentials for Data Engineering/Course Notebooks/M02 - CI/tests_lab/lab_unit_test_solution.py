## This file needs to be executed by pytest. If you execute the .py file here an error is returned.
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.testing.utils import assertDataFrameEqual


## Import the functions from src > helpers
from src_lab.lab_functions import transforms


## Defines the spark session to run before the test functions
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.getOrCreate()
    
    yield spark



def test_uppercase_columns_function(spark):

    ## Fake DataFrame with random column names
    data = [(1, 5.0, 1, 1, 1, 1)]
    columns = ["id", "trip_distance", "My_Column", "WithNumbers123", "WithSymbolX@#", "With Space"]
    df = spark.createDataFrame(data, columns)

    ## Apply function and return column names
    actual_df = transforms.uppercase_columns_names(df)
    actual_columns = actual_df.columns

    ## Expected column names
    expected_columns = ['ID', 'TRIP_DISTANCE', 'MY_COLUMN', 'WITHNUMBERS123', 'WITHSYMBOLX@#', "WITH SPACE"]

    ## Perform tests
    assert actual_columns == expected_columns, 'Test Passed!'




def test_convert_miles_to_km_function(spark):
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


    ## Create a DataFrame with the expected values
    data = [
        (1.0, 1.61),   # Row with values
        (5.5, 8.85),   # Row with values
        (None, None) # Row with null values
    ]

    ## Define schema (optional but recommended for clarity)
    schema = StructType([
        StructField("trip_distance_miles", DoubleType(), True),
        StructField("trip_distance_km", DoubleType(), True)
    ])

    ## Create expected DataFrame
    expected_df = spark.createDataFrame(data, schema)


    ## Compare the actual and expected DataFrames using assertDataFrameEqual
    assertDataFrameEqual(actual_df,expected_df), 'Test Passed!'