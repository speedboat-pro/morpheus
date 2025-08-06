# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# Initialize DBAcademyHelper
DA = DBAcademyHelper() 
DA.init()

# COMMAND ----------

# Load the wine dataset from Delta table
data_path = f"{DA.paths.datasets.wine_quality}/data"
df = spark.read.format("delta").load(data_path)

# Define feature columns and label column
feature_columns = ["fixed_acidity", 
                   "volatile_acidity", 
                   "citric_acid", 
                   "residual_sugar", 
                   "chlorides", 
                   "free_sulfur_dioxide", 
                   "total_sulfur_dioxide", 
                   "density", 
                   "pH", 
                   "sulphates", 
                   "alcohol"]
                   
label_column = "quality"

# Assemble the feature vector using VectorAssembler
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df = assembler.transform(df)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Drop the 'ID' column if it already exists
if "ID" in df.columns:
    df = df.drop("ID")

# Add a new 'ID' column
df = df.withColumn("ID", monotonically_increasing_id().cast("int"))

# Overwrite the Delta table (avoid schema merge issues)
df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{DA.catalog_name}.{DA.schema_name}.wine_quality_features")

print(f"Successfully saved table: {DA.catalog_name}.{DA.schema_name}.wine_quality_features")
