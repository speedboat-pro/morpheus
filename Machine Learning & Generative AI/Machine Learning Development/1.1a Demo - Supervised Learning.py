# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Supervised Learning
# MAGIC
# MAGIC In this demo, we will guide you through using a supervised learning model. In particular, we will be working through how to train a regression model. In these demos, you will learn how to retrieve data and fit the model using Databricks Notebooks and track the model's development using MLflow. In addition, you will learn how to interpret results using visualization tools and various model metrics. 
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC
# MAGIC *By the end of this demo, you will be able to:*
# MAGIC
# MAGIC * Fit a linear regression model on modeling data using the sklearn API.
# MAGIC
# MAGIC * Interpret the fit of an sklearn linear model’s coefficients and intercept.
# MAGIC
# MAGIC * Fit a decision tree model using sklearn API and training data.
# MAGIC
# MAGIC * Visualize an sklearn tree’s split points.
# MAGIC
# MAGIC * Identify which metrics are tracked by MLflow.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC    - In the drop-down, select **More**.
# MAGIC    - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC   
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need to use one of the following Databricks runtime(s): **16.3.x-cpu-ml-scala2.12**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo. Execute the following cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-1.1a

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
# MAGIC ## Prepare Dataset
# MAGIC
# MAGIC In this section, we are going to prepare the dataset for our machine learning models. The dataset we'll be working with is the **California housing dataset**. 
# MAGIC
# MAGIC The dataset has been loaded, cleaned and saved to a **feature table**. We will read data directly from this table.
# MAGIC
# MAGIC Then, we will split the dataset into **train and test** sets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Dataset
# MAGIC
# MAGIC This dataset contains information about housing districts in California and **aims to predict the median house value** for California districts, based on various features.
# MAGIC
# MAGIC While data cleaning and feature engineering is out of the scope of this demo, we will only map the `ocean_proximity` field. 
# MAGIC

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
fe = FeatureEngineeringClient()

# read data from the feature store
table_name = f"{DA.catalog_name}.{DA.schema_name}.ca_housing"
feature_data_pd = fe.read_table(name=table_name).toPandas()
feature_data_pd = feature_data_pd.drop(columns=['unique_id'])

# COMMAND ----------

ocean_proximity_mapping = {
    'NEAR BAY': 1,
    '<1H OCEAN': 2,
    'INLAND': 3,
    'NEAR OCEAN': 4,
    'ISLAND': 5  
}

# Replace values in the DataFrame
feature_data_pd['ocean_proximity'] = feature_data_pd['ocean_proximity'].replace(ocean_proximity_mapping).astype(float)

# Print the updated DataFrame
feature_data_pd = feature_data_pd.fillna(0)

display(feature_data_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train / Test Split
# MAGIC
# MAGIC Split the dataset into training and testing sets. This is essential for evaluating the performance of machine learning models.

# COMMAND ----------

from sklearn.model_selection import train_test_split

print(f"We have {feature_data_pd.shape[0]} records in our source dataset")

# split target variable into its own dataset
target_col = "median_house_value"
X_all = feature_data_pd.drop(labels=target_col, axis=1)
y_all = feature_data_pd[target_col]

# test / train split
X_train, X_test, y_train, y_test = train_test_split(X_all, y_all, train_size=0.8, random_state=42)
print(f"We have {X_train.shape[0]} records in our training dataset")
print(f"We have {X_test.shape[0]} records in our test dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine for Potential Co-linearity
# MAGIC
# MAGIC Now, let's examine the correlations between predictors to identify potential co-linearity. Understanding the relationships between different features can provide insights into the dataset and help us make informed decisions during the modeling process.
# MAGIC
# MAGIC Let's review the **correlation matrix** in **tabular format**. Also, we can create a **graph based on the correlation matrix** to easily inspect the matrix.

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# Combine X and y into a single DataFrame for simplicity
data = pd.concat([X_train, y_train], axis=1)

# Calculate correlation matrix
corr = data.corr()

# display correlation matrix
pd.set_option('display.max_columns', 10)
print(corr)

# COMMAND ----------

# display correlation matrix visually

# Initialize figure
plt.figure(figsize=(8, 8))
for i in range(len(corr.columns)):
    for j in range(len(corr.columns)):
        # Determine the color based on positive or negative correlation
        color = 'blue' if corr.iloc[i, j] > 0 else 'red'

        # don't fill in circles on the diagonal
        fill = not( i == j )

        # Plot the circle with size corresponding to the absolute value of correlation
        plt.gca().add_patch(plt.Circle((j, i), 
                                       0.5 * np.abs(corr.iloc[i, j]), 
                                       color=color, 
                                       edgecolor=color,
                                       fill=fill,
                                       alpha=0.5))



plt.xlim(-0.5, len(corr.columns) - 0.5)
plt.ylim(-0.5, len(corr.columns) - 0.5)
plt.gca().set_aspect('equal', adjustable='box')
plt.xticks(np.arange(len(corr.columns)), corr.columns, rotation=90)
plt.yticks(np.arange(len(corr.columns)), corr.columns)
plt.title('Correlogram')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fit a Regression Model
# MAGIC
# MAGIC To enhance the performance of our regression model, we'll scale our input variables so that they are on a common (standardized) scale. **Standardization ensures that each feature has a mean of 0 and a standard deviation of 1**, which can be beneficial for certain algorithms, including linear regression.

# COMMAND ----------

from math import sqrt

import mlflow.sklearn

from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_percentage_error

# turn on autologging
mlflow.sklearn.autolog(log_input_examples=True)

# apply the Standard Scaler to all our input columns
std_ct = ColumnTransformer(transformers=[("scaler", StandardScaler(), ["total_bedrooms", "total_rooms", "housing_median_age", "latitude", "longitude", "median_income", "population", "ocean_proximity", "households"])])

# pipeline to transform inputs and then pass results to the linear regression model
lr_pl = Pipeline(steps=[
  ("tx_inputs", std_ct),
  ("lr", LinearRegression() )
])

# fit our model
lr_mdl = lr_pl.fit(X_train, y_train)

# evaluate the test set
predicted = lr_mdl.predict(X_test)
test_r2 = r2_score(y_test, predicted)
test_mse = mean_squared_error(y_test, predicted)
test_rmse = sqrt(test_mse)
test_mape = mean_absolute_percentage_error(y_test, predicted)
print("Test evaluation summary:")
print(f"R^2: {test_r2}")
print(f"MSE: {test_mse}")
print(f"RMSE: {test_rmse}")
print(f"MAPE: {test_mape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine Model Result
# MAGIC
# MAGIC Now, let's inspect the results of our linear regression model. We'll examine both the intercept and the coefficients of the fitted model. Additionally, we'll perform a **t-test on each coefficient to assess its significance in contributing to the overall model**.

# COMMAND ----------

lr_mdl

# COMMAND ----------

import pandas as pd
import numpy as np
from scipy import stats

# Extracting coefficients and intercept
coefficients = np.append([lr_mdl.named_steps['lr'].intercept_], lr_mdl.named_steps['lr'].coef_)
coefficient_names = ['Intercept'] + X_train.columns.to_list()

# Calculating standard errors and other statistics (this is a simplified example)
# In a real scenario, you might need to calculate these values more rigorously
n_rows, n_cols = X_train.shape
X_with_intercept = np.append(np.ones((n_rows, 1)), X_train, axis=1)
var_b = test_mse * np.linalg.inv(np.dot(X_with_intercept.T, X_with_intercept)).diagonal()
standard_errors = np.sqrt(var_b)
t_values = coefficients / standard_errors
p_values = [2 * (1 - stats.t.cdf(np.abs(i), (len(X_with_intercept) - 1))) for i in t_values]

# Creating a DataFrame for display
summary_df = pd.DataFrame({'Coefficient': coefficients,
                           'Standard Error': standard_errors,
                           't-value': t_values,
                           'p-value': p_values},
                          index=coefficient_names)

# Print the DataFrame
print(summary_df)

# COMMAND ----------

import matplotlib.pyplot as plt

# Plotting the feature importances
plt.figure(figsize=(10, 6))
y_pos = np.arange(len(coefficient_names))
plt.bar(y_pos, coefficients, align='center', alpha=0.7)
plt.xticks(y_pos, coefficient_names, rotation=45)
plt.ylabel('Coefficient Size')
plt.title('Coefficients in Linear Regression')

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC In conclusion, this demonstration provided a comprehensive walkthrough of training a linear regression model using the scikit-learn library. We covered essential steps such as data preparation, model fitting, and result examination. Understanding the coefficients and intercept of the model is crucial for interpreting its predictive power. Moreover, we discussed the significance of each feature through t-tests, offering insights into the statistical relevance of predictors. Armed with this knowledge, practitioners can make informed decisions about the importance of variables in their regression models. This demo serves as a foundational guide for those seeking a practical understanding of linear regression modeling in a machine learning context.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
