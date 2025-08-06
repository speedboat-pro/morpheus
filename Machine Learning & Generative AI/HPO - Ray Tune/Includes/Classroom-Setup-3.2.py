# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

import numpy as np
np.set_printoptions(precision=2)

import logging
logging.getLogger("tensorflow").setLevel(logging.ERROR)
import pandas as pd
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

# COMMAND ----------

import pandas as pd
@DBAcademyHelper.add_method
def create_large_wine_quality_table(self):
    spark.sql(f"USE CATALOG {DA.catalog_name}")
    spark.sql(f"USE SCHEMA {DA.schema_name}")
    
    # Load the original wine dataset
    data_path = f"{DA.paths.datasets.wine_quality}/data"
    df = spark.read.format("delta").load(data_path)

    # Function to create a larger dataset for demonstration purposes
    def generate_large_wine_dataset(df, num_copies=100):
        pandas_df = df.toPandas()  # Convert to Pandas for duplication
        large_df = pd.concat([pandas_df.sample(frac=1).reset_index(drop=True)] * num_copies, ignore_index=True)  # Duplicate and shuffle
        return large_df

    # Convert back to Spark DataFrame and save as Delta table
    large_wine_df = spark.createDataFrame(generate_large_wine_dataset(df, num_copies=100))
    output_delta_table = f"{DA.paths.working_dir}/v01/large_wine_quality_delta"
    large_wine_df.write.format("delta").mode("overwrite").save(output_delta_table)

    print(f"Created large Delta table at {output_delta_table}")

# COMMAND ----------

# Initialize DBAcademyHelper
DA = DBAcademyHelper() 
DA.init()          # Performs basic initialization including creating schemas and catalogs
DA.create_large_wine_quality_table()

# COMMAND ----------

def create_distributed_predictions_table():
    # Define feature columns and assemble them into a vector
    feature_columns = [
        "fixed_acidity", "volatile_acidity", "citric_acid", "residual_sugar", 
        "chlorides", "free_sulfur_dioxide", "total_sulfur_dioxide", "density", 
        "pH", "sulphates", "alcohol"
    ]
    
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    
    # Load the large wine quality table created earlier
    table_path = f"{DA.paths.working_dir}/v01/large_wine_quality_delta"
    df = spark.read.format('delta').load(table_path)

    # Assemble features into a vector and split data
    df_with_features = assembler.transform(df)
    train_df, test_df = df_with_features.randomSplit([0.8, 0.2], seed=42)

    # Define and train a simple DecisionTreeRegressor model
    dt = DecisionTreeRegressor(featuresCol="features", labelCol="quality", maxDepth=5)
    pipeline = Pipeline(stages=[dt])
    
    # Train the model
    dt_model = pipeline.fit(train_df)
    
    # Perform inference on the test data using the trained model
    predictions = dt_model.transform(test_df)

    # Save predictions to a Delta table
    table_name = f"{DA.catalog_name}.{DA.schema_name}.distributed_predictions_table"

    predictions.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)
    copy_table_name = f"{DA.catalog_name}.{DA.schema_name}.predictions_before_optimize"
    predictions.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(copy_table_name)

    print(f"Created predictions Delta table at {table_name}")
    
# Create the distributed predictions table
create_distributed_predictions_table()
