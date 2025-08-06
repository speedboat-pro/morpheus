# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

@DBAcademyHelper.add_init
def create_feature_table(self):
    from pyspark.ml.feature import VectorAssembler
    from databricks.feature_store import FeatureStoreClient
    from pyspark.sql.functions import monotonically_increasing_id, col
    from pyspark.ml.functions import vector_to_array  # Import the function

    # Load the wine dataset from Delta table
    data_path = f"{DA.paths.datasets.wine_quality}/data"
    df = spark.read.format("delta").load(data_path)

    # Create a feature store client
    fe = FeatureStoreClient()

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

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df = assembler.transform(df)

    # Convert features column to an array of numeric types using vector_to_array
    df = df.withColumn("features_array", vector_to_array("features"))

    # Add an ID column to the dataset using monotonically_increasing_id
    df = df.withColumn("ID", monotonically_increasing_id())
    # Create the feature store table (dropping the original 'features' column)
    fe.create_table(
        name = f"{DA.catalog_name}.{DA.schema_name}.wine_quality_features",
        primary_keys = ["ID"],
        df = df.drop("features"),
        description = "Wine quality features"
    )

    return print(f'Created Feature Table {DA.catalog_name}.{DA.schema_name}.wine_quality_features')

# COMMAND ----------

# Initialize DBAcademyHelper
DA = DBAcademyHelper() 
DA.init()
