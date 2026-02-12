# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo: Pandas APIs  
# MAGIC In this demo, we will explore the key operations of Pandas, Spark, and Pandas API (Koalas) DataFrames, focusing on their performance, conversions, and using UDFs and functions for distributed processing.
# MAGIC
# MAGIC **Learning Objectives:**  
# MAGIC _By the end of this demo, you will be able to:_
# MAGIC 1. **Compare the performance** of Pandas, Spark, and Pandas API (Koalas) DataFrames for numerical operations.
# MAGIC 2. **Convert between** Pandas, Spark, and Pandas API DataFrames to leverage their unique capabilities.
# MAGIC 3. **Apply Pandas UDFs and Functions** to a Spark DataFrame for distributed inference using pre-trained models.
# MAGIC 4. **Train group-specific machine learning models** using Pandas Function APIs for customized modeling.
# MAGIC 5. **Perform group-specific inference** by loading models from MLflow and running predictions across groups.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need a classic cluster running one of the following Databricks runtime(s): **16.3.x-cpu-ml-scala2.12**. **Do NOT use serverless compute to run this notebook**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Install required libraries.

# COMMAND ----------

# MAGIC %pip install pandas pyspark koalas
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting the demo, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run "../Includes/Classroom-Setup"

# COMMAND ----------

# MAGIC %md
# MAGIC **Other Conventions:**
# MAGIC
# MAGIC Throughout this demo, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading the Airbnb Dataset
# MAGIC
# MAGIC In this section, we will load the **Airbnb listings dataset**. The dataset contains important features like price, neighborhood, and property details. We will load this data into a **Spark DataFrame** and then explore how to convert it into both a Pandas and a Pandas API on Spark DataFrames to understand their capabilities and performance.
# MAGIC
# MAGIC **Steps to Follow:**
# MAGIC
# MAGIC 1. **Load the dataset into a Spark DataFrame**: We'll use Spark's `read.csv` method to load the Airbnb dataset from a Delta table, which contains cleaned data prepared for machine learning tasks.
# MAGIC 2. **Inspect the data**: After loading, we will display the first few rows of the dataset to ensure it has been loaded correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC For more details about working with Spark DataFrames, visit the [PySpark DataFrame Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html).

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps

# Load the Airbnb dataset into a Spark DataFrame
data_path = f"{DA.paths.datasets.airbnb}/sf-listings/airbnb-cleaned-mlflow.csv"
airbnb_df = spark.read.csv(data_path, header=True, inferSchema=True)

airbnb_df_large = airbnb_df
# Let's make a larger table (~75 rows) out of the original table to help measure performance. 
for i in range(1,5):
    airbnb_df_large = airbnb_df_large.union(airbnb_df_large)

# Display the first few rows of the dataset
display(airbnb_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Comparing Performance of Pandas, Spark, and Pandas API DataFrames
# MAGIC
# MAGIC In this section, we will explore the performance differences between **Pandas**, **Spark**, and **Pandas API on Spark (Koalas)** DataFrames. By calculating the mean of numeric columns, we will compare how each framework performs and understand the advantages of using distributed vs. non-distributed DataFrames in various environments.
# MAGIC
# MAGIC **Steps to Follow:**
# MAGIC
# MAGIC 1. **Convert Spark DataFrame to Pandas DataFrame**.
# MAGIC 2. **Calculate the mean for numeric columns** using Pandas.
# MAGIC 3. **Calculate the mean for numeric columns** using Spark DataFrame.
# MAGIC 4. **Use Pandas API on Spark (Koalas)** to compute the same operation in a distributed environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Preprocessing for Performance Test
# MAGIC
# MAGIC Before testing the performance of the three DataFrames, we need to select numeric columns and convert the Spark DataFrame into Pandas and Pandas API on Spark DataFrames. This will prepare the data for the performance test.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg
from pyspark.sql.types import DoubleType, IntegerType


# Select numeric columns for averaging
numeric_columns_spark_df = [col for col in airbnb_df_large.columns if airbnb_df_large.schema[col].dataType in [DoubleType(), IntegerType()]]

# Convert Spark DataFrame to Pandas DataFrame
airbnb_pandas_df = airbnb_df_large.toPandas()


# Convert Spark DataFrame to Pandas API on Spark DataFrame
airbnb_pandas_on_spark_df = ps.DataFrame(airbnb_df_large)

print(f"Our test dataframe has {airbnb_df_large.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Performance Test: Comparing Pandas, Spark, and Pandas API on Spark
# MAGIC
# MAGIC Now that the DataFrames are ready, we will compute the mean of all numeric columns and measure the time it takes for each framework to complete the operation.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Performance Test: Comparing Pandas, Spark, and Pandas API on Spark
# MAGIC
# MAGIC In this part of the demo, we can compare the performance of **Pandas**, **Spark**, and **Pandas API on Spark (Koalas)** for various data operations, such as computing the mean of numeric columns. This comparison helps in understanding the efficiency of each framework, particularly in different computational environments (single-node vs. distributed).
# MAGIC
# MAGIC The image below serves as a benchmark comparison, illustrating the execution times for each framework across different operations. Key insights:
# MAGIC - **Suitability of Each Framework**: Spark’s distributed processing is highly effective with large datasets, making it ideal for big data tasks, while Pandas excels on smaller datasets due to its minimal overhead.
# MAGIC - **Framework-Specific Strengths and Limitations**: Some operations, like joins, may perform differently based on the framework used, with Spark often handling large data joins more efficiently than Pandas or Pandas API on Spark.
# MAGIC - **Importance of Tool Selection**: Choosing the appropriate tool for a given data size and complexity can significantly impact performance, which is crucial in production settings where optimization is key.
# MAGIC
# MAGIC ![image_metrics](files/images/machine-learning-at-scale-1.2.2/image_metrics.png)

# COMMAND ----------

import time 

start_time_spark = time.time()
# Calculate the mean for each numeric column using Spark
spark_mean = airbnb_df_large.select([avg(col).alias(f"avg_{col}") for col in numeric_columns_spark_df])
spark_time = time.time() - start_time_spark

start_time_pandas = time.time()
# Calculate the mean using Pandas
pandas_mean = airbnb_pandas_df.mean()
pandas_time = time.time() - start_time_pandas 

start_time_pandas_api_on_spark = time.time()
# Calculate mean using Pandas API on Spark
pandas_on_spark_mean = airbnb_pandas_on_spark_df.mean()
pandas_on_spark_time = time.time() - start_time_pandas_api_on_spark

# Display the time taken for each framework
print(f"Spark DataFrame: {spark_time} seconds")
print(f"Pandas DataFrame: {pandas_time} seconds")
print(f"Pandas API on Spark: {pandas_on_spark_time} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC For more details on working with **PySpark SQL Functions** and **Pandas API on Spark (Koalas)**, explore the following resources:
# MAGIC
# MAGIC - [PySpark SQL Functions Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
# MAGIC - [Pandas API on Spark Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html)
# MAGIC
# MAGIC These resources provide comprehensive guidance on leveraging both PySpark's built-in functions and the Pandas API on Spark for distributed data processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Converting Between DataFrames
# MAGIC
# MAGIC In this section, we will explore how to convert between different types of DataFrames: Spark, Pandas, and Pandas API on Spark. This flexibility allows you to take advantage of the specific features and performance benefits of each framework based on your data processing needs.
# MAGIC
# MAGIC By converting between DataFrame types, you can use the familiar syntax of Pandas with the distributed capabilities of Spark, or switch to Pandas when you're working with smaller datasets locally.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Convert Spark DataFrame to Pandas API on Spark DataFrame
# MAGIC
# MAGIC In some cases, you might want to work with a Pandas-like syntax while still leveraging Spark’s distributed architecture. This can be achieved by converting a Spark DataFrame into a Pandas API on Spark DataFrame (previously Koalas). Let’s see how to do that:

# COMMAND ----------

# Convert Spark DataFrame to Pandas API on Spark DataFrame
pandas_on_spark_df = airbnb_df.to_pandas_on_spark()

# Display the DataFrame in Pandas API on Spark format
display(pandas_on_spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Convert Pandas API on Spark DataFrame Back to Spark DataFrame
# MAGIC
# MAGIC Once you have finished operations using Pandas API on Spark, you might need to convert it back to a Spark DataFrame to use Spark-specific functions or take advantage of Spark’s distributed processing features for tasks like machine learning with Spark MLlib.
# MAGIC

# COMMAND ----------

# Convert Pandas API on Spark DataFrame back to Spark DataFrame
spark_df = pandas_on_spark_df.to_spark()

# Display the converted Spark DataFrame
display(spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Convert Pandas API on Spark DataFrame to Pandas DataFrame
# MAGIC
# MAGIC In some cases, you may want to bring the data back into a local Pandas DataFrame, such as when working with a smaller subset of the data or applying operations exclusive to Pandas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC For more on Pandas API on Spark, you can refer to the official [Pandas API on Spark Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/).

# COMMAND ----------

# Convert Pandas API on Spark DataFrame to Pandas DataFrame
pandas_df = pandas_on_spark_df.to_pandas()

# Display the Pandas DataFrame
display(pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Applying Pandas UDF to a Spark DataFrame
# MAGIC
# MAGIC In this section, you will use a **pre-trained RandomForest model** from Scikit-learn and apply it to a Spark DataFrame using a **Pandas UDF**. This process allows you to leverage the trained model to make predictions on distributed Spark DataFrames, providing scalable and efficient predictions across large datasets.
# MAGIC
# MAGIC **Why Use Pandas UDFs?**
# MAGIC
# MAGIC Pandas UDFs are optimized for distributed data processing in Spark. By applying Pandas UDFs, you can run functions on Spark DataFrames that expect Pandas DataFrames as input, which is useful for machine learning models trained outside of the Spark ecosystem, like Scikit-learn models.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Training a RandomForest Model
# MAGIC
# MAGIC First, we will train a **RandomForest model** using Scikit-learn on a **local Pandas DataFrame**. This model will predict the `price` column based on features in the Airbnb dataset.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC 1. Convert the Spark DataFrame to a Pandas DataFrame.
# MAGIC 2. Separate the features (`X`) and target variable (`y`).
# MAGIC 3. Train the RandomForest model on the training data.

# COMMAND ----------

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

# Convert Spark DataFrame to Pandas DataFrame for local model training
airbnb_pandas_df = airbnb_df.toPandas()

# Features and target variable
X = airbnb_pandas_df.drop(columns=["price"])  # Exclude target column
y = airbnb_pandas_df["price"]  # Target column

# Train the RandomForest model
model = RandomForestRegressor()
model.fit(X, y)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Define and Apply a Pandas UDF
# MAGIC
# MAGIC Now, we will define a **Pandas UDF** that takes in the features from the Spark DataFrame, uses the trained RandomForest model to make predictions, and returns the predicted values for each row.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC 1. Define the Pandas UDF to apply the pre-trained RandomForest model. We do this by appending our function `predict_udf` with the decorator `@pandas_udf`.
# MAGIC 2. Use the UDF to process the input columns and generate predictions.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

# Define a Pandas UDF to apply the trained RandomForest model
@pandas_udf("double")  # Output type of the UDF is a double (for predicted price)
def predict_udf(*cols: pd.Series) -> pd.Series:
    # Combine input columns into a single DataFrame for model prediction
    features = pd.concat(cols, axis=1)
    # Return predictions from the trained RandomForest model
    return pd.Series(model.predict(features))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Applying the Pandas UDF to the Spark DataFrame
# MAGIC
# MAGIC Now, you can apply the `predict_udf` to the Spark DataFrame to generate predictions for each row. This allows you to distribute the prediction process across the cluster, making it scalable for larger datasets.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC 1. Apply the `predict_udf` to the Spark DataFrame.
# MAGIC 2. Exclude the target column (`price`) when applying the UDF.
# MAGIC 3. Add a new column, `prediction`, containing the predicted price.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import pandas_udf
import pandas as pd

# Define the features used for prediction, excluding "price"
feature_names = [col for col in airbnb_df.columns if col != "price"]

@pandas_udf("double")
def predict_udf(*cols: pd.Series) -> pd.Series:
    # Combine input columns into a DataFrame for model prediction
    features = pd.concat(cols, axis=1)
    features.columns = feature_names  # Set the correct feature names
    
    # Return predictions from the trained RandomForest model
    return pd.Series(model.predict(features))

# Apply the Pandas UDF to the Spark DataFrame for predictions, excluding "price"
prediction_df = airbnb_df.select([col for col in feature_names]).withColumn(
    "prediction", 
    predict_udf(*[airbnb_df[col] for col in feature_names])
)

# Display the DataFrame with predictions
display(prediction_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Training Group-Specific Models with Pandas Function API
# MAGIC
# MAGIC In this section, you will learn how to **train group-specific models** for each neighborhood using the **Pandas Function API**. By splitting the dataset based on a grouping criterion (in this case, `neighbourhood_cleansed`), we can train individual machine learning models for each group and log them using **MLflow**. This approach is useful when different groups of data may benefit from customized models.
# MAGIC
# MAGIC **Why Group-Specific Models?**
# MAGIC
# MAGIC Group-specific models allow for fine-tuned predictions by training separate models for each group, which can often yield better results compared to training a single model for the entire dataset. In this example, each **neighborhood** will have its own trained model.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Define the Group-Specific Model Training Function
# MAGIC
# MAGIC We will define a function that:
# MAGIC
# MAGIC - **Trains a RandomForest model** for each neighborhood group.
# MAGIC - **Logs the model** in **MLflow**.
# MAGIC - **Calculates the Mean Squared Error (MSE)** for model performance evaluation.
# MAGIC - **Returns key metrics**, including the model path, MSE, and the number of records used in the training.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC 1. Extract the `neighbourhood_cleansed` for each group.
# MAGIC 2. Define the feature set (`X_group`) and the target (`y_group`).
# MAGIC 3. Train a RandomForest model for the group.
# MAGIC 4. Log the model in MLflow.
# MAGIC 5. Return the model's URI, MSE, and other group-specific metrics.

# COMMAND ----------

from pyspark.sql.types import StringType, IntegerType, DoubleType, StructType, StructField
from sklearn.metrics import mean_squared_error
import mlflow
import mlflow.sklearn

# Define a pandas function to train a group-specific model and log each model with MLflow
def train_group_model(df_pandas: pd.DataFrame) -> pd.DataFrame:
    # Get the neighborhood name
    neighbourhood = df_pandas['neighbourhood_cleansed'].iloc[0]
    
    # Define features (X) and target variable (y)
    X_group = df_pandas.drop(columns=["price", "neighbourhood_cleansed"])
    y_group = df_pandas["price"]
    
    # Train a RandomForest model for the group
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_group, y_group)
    
    # Log the model using MLflow
    with mlflow.start_run(nested=True):
        mlflow.sklearn.log_model(model, "random_forest_model")
        model_uri = mlflow.get_artifact_uri("random_forest_model")
    
    # Calculate Mean Squared Error (MSE) for the group
    predictions = model.predict(X_group)
    mse = mean_squared_error(y_group, predictions)

    # Return a DataFrame containing group information and model performance
    return pd.DataFrame({
        "neighbourhood_cleansed": [str(neighbourhood)],  # Neighborhood name
        "model_path": [str(model_uri)],                  # MLflow model URI
        "mse": [float(mse)],                             # Mean Squared Error
        "n_used": [int(len(df_pandas))]                  # Number of records used in training
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Apply the Group-Specific Function Using Pandas API
# MAGIC
# MAGIC Now, we will use the **Pandas Function API** to apply the group-specific model training function across each group. The `applyInPandas` method is applied on the grouped data, allowing us to train a model for each neighborhood.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC 1. Define the schema for the output DataFrame, which includes the group name, model path, MSE, and the number of records used in training.
# MAGIC 2. Group the DataFrame by the `neighbourhood_cleansed` column and apply the `train_group_model` function.
# MAGIC 3. Display the results, showing the model path, MSE, and other metrics for each neighborhood.

# COMMAND ----------

# MAGIC %md
# MAGIC For more information on using Pandas Function APIs in PySpark, refer to the [Pandas Function API Documentation](https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html).

# COMMAND ----------

# Define the schema for the output DataFrame
schema = StructType([
    StructField("neighbourhood_cleansed", StringType(), True),
    StructField("model_path", StringType(), True),
    StructField("mse", DoubleType(), True),
    StructField("n_used", IntegerType(), True)
])

# Apply the group-specific model training function using 'applyInPandas'
result_df = airbnb_df.groupby("neighbourhood_cleansed").applyInPandas(train_group_model, schema=schema)

# Display the result DataFrame showing the model path, MSE, and number of records for each group
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Group-Specific Inference Using Pandas Function API
# MAGIC
# MAGIC In this part, you will perform **inference for each neighborhood** using the models we previously trained and logged in **MLflow**. The goal is to load the appropriate model for each neighborhood and make predictions based on the features from the test data. After that, we will compare the predicted prices to the actual prices and evaluate the accuracy of the models.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Define the Inference Function
# MAGIC
# MAGIC We will define a function that:
# MAGIC
# MAGIC - **Loads the trained model** from MLflow for each neighborhood.
# MAGIC - **Uses the correct feature set** to make predictions for that group of data.
# MAGIC - **Returns the predictions** and the **actual prices** for comparison.
# MAGIC
# MAGIC **Steps:**
# MAGIC 1. Extract the model path for the neighborhood from the DataFrame.
# MAGIC 2. Load the trained model from MLflow.
# MAGIC 3. Select the feature columns for the model.
# MAGIC 4. Make predictions for the current group using the loaded model.
# MAGIC 5. Return the predictions and actual prices for comparison.

# COMMAND ----------

# Define a pandas function for group-specific inference using MLflow models
def apply_model(df_pandas: pd.DataFrame) -> pd.DataFrame:
    # Retrieve the model path from the DataFrame (assumes model_path is present)
    model_path = df_pandas["model_path"].iloc[0]
    
    # Load the model from MLflow
    model = mlflow.sklearn.load_model(model_path)
    
    # Define the feature columns that were used during training
    feature_columns = [
        "host_total_listings_count", "zipcode", "latitude", "longitude", "property_type",
        "room_type", "accommodates", "bathrooms", "bedrooms", "beds", "bed_type",
        "minimum_nights", "number_of_reviews", "review_scores_rating", "review_scores_accuracy",
        "review_scores_cleanliness", "review_scores_checkin", "review_scores_communication",
        "review_scores_location", "review_scores_value"
    ]
    
    # Select only the feature columns for inference
    X = df_pandas[feature_columns]
    
    # Make predictions using the loaded model
    predictions = model.predict(X)
    
    # Return a DataFrame with both predictions and the actual values (price)
    return pd.DataFrame({
        "prediction": predictions,           # Predicted price
        "actual_price": df_pandas["price"]   # Actual price for comparison
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Apply the Inference Function
# MAGIC
# MAGIC Now, we will apply the `apply_model` function to each group of data (neighborhood) using the **Pandas Function API**. This will allow us to make predictions for each group of data using the corresponding trained model.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC 1. Define the schema for the output DataFrame, which will contain the predicted and actual prices.
# MAGIC 2. Join the original data with the trained model results to ensure each group has the corresponding model path.
# MAGIC 3. Apply the `apply_model` function using `applyInPandas`.
# MAGIC 4. Calculate the **accuracy** of the predictions by comparing the predicted prices with the actual prices.

# COMMAND ----------

# MAGIC %md
# MAGIC For more details on using MLflow to track models, check out the [MLflow Documentation](https://mlflow.org/docs/latest/index.html).

# COMMAND ----------

from pyspark.sql.functions import abs

# Define the schema to include the prediction and actual price columns
inference_schema = "prediction double, actual_price double"

# Ensure model_path and price are in the inference DataFrame by joining the training results
inference_df = result_df.join(airbnb_df, "neighbourhood_cleansed")

# Apply the model using Pandas Function API, grouped by 'neighbourhood_cleansed'
inference_df = inference_df.groupby("neighbourhood_cleansed").applyInPandas(
    apply_model, 
    schema=inference_schema
)

# Display the result DataFrame with predictions and actual prices
display(inference_df)

# Calculate overall accuracy (percentage of predictions within 10% of actual prices)
inference_df = inference_df.withColumn(
    "accuracy", 
    (abs(inference_df["prediction"] - inference_df["actual_price"]) / inference_df["actual_price"]) < 0.1
)
overall_accuracy = inference_df.filter("accuracy = true").count() / inference_df.count() * 100

# Display overall accuracy
print(f"Overall prediction accuracy: {overall_accuracy:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Conclusion
# MAGIC In this demo, we explored the versatility and efficiency of using Pandas, Spark, and Pandas API DataFrames for data processing, conversion, and model training. We compared the performance of these frameworks, learned how to convert between different DataFrame types, and applied Pandas UDFs to Spark DataFrames for distributed model inference. Furthermore, we demonstrated how to train group-specific models using Pandas Function APIs and performed group-specific inference using models logged with MLflow. This demo showcased how to combine Spark's distributed power with Pandas' ease of use for scalable machine learning workflows.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
# MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/">Support</a>
