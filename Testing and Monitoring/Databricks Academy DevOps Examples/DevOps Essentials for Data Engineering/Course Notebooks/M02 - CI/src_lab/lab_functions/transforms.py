from pyspark.sql import functions as F


def convert_miles_to_km(df, new_column_name, miles_column):
    return df.withColumn(new_column_name, F.round(F.col(miles_column) * 1.60934, 2))


def uppercase_columns_names(df):
    return df.select([F.col(col).alias(col.upper()) for col in df.columns])