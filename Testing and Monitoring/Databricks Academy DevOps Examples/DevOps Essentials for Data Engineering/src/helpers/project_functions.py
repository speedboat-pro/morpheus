from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType
from pyspark.sql.functions import when, col


# Function to define the schema for the health data CSV
def get_health_csv_schema():
    """
    Returns the schema for the health data CSVs.

    This function defines the structure of the CSV files that contains health-related data. 

    Returns:
        StructType: The schema that defines the data types for each column in the CSV.
    """
    return StructType([
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



def high_cholest_map(col_name: str):
    """
    Maps a cholesterol value to a categorical label.

    This function transforms a numeric column containing cholesterol levels 
    into a categorical label based on predefined ranges:
        - 0 -> 'Normal'
        - 1 -> 'Above Average'
        - 2 -> 'High'
        - Any other value -> 'Unknown'

    Args:
        col_name (str): The name of the column to be transformed.

    Returns:
        pyspark.sql.column.Column: A new column with categorical labels.
    """
    return (
        when(col(col_name) == 0, 'Normal')
        .when(col(col_name) == 1, 'Above Average')
        .when(col(col_name) == 2, 'High')
        .otherwise('Unknown')
    )

    

def group_ages_map(col_name: str):
    """
    Maps an age value to an age group category.

    This function transforms a numeric column containing age values into 
    categorical age groups based on predefined ranges:
        - 0-9 -> "0-9"
        - 10-19 -> "10-19"
        - 20-29 -> "20-29"
        - 30-39 -> "30-39"
        - 40-49 -> "40-49"
        - 50+ -> "50+"
        - Any other value -> "Unknown"

    Args:
        col_name (str): The name of the column to be transformed.

    Returns:
        pyspark.sql.column.Column: A new column with categorized age groups.
    """
    return (
        when((col(col_name) >= 0) & (col(col_name) <= 9), "0-9")  # If age is between 0-9, label as "0-9"
        .when((col(col_name) >= 10) & (col(col_name) <= 19), "10-19")  # If age is between 10-19, label as "10-19"
        .when((col(col_name) >= 20) & (col(col_name) <= 29), "20-29")  # If age is between 20-29, label as "20-29"
        .when((col(col_name) >= 30) & (col(col_name) <= 39), "30-39")  # If age is between 30-39, label as "30-39"
        .when((col(col_name) >= 40) & (col(col_name) <= 49), "40-49")  # If age is between 40-49, label as "40-49"
        .when(col(col_name) >= 50, "50+")  # If age is 50 or older, label as "50+"
        .otherwise('Unknown')  # For any other age value, label as "Unknown"
    )

