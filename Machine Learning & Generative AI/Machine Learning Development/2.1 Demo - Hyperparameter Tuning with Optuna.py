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
# MAGIC # Hyperparameter Tuning with Optuna
# MAGIC
# MAGIC In this hands-on demo, you will learn how to leverage **Optuna**, a powerful optimization library, for efficient model tuning. We'll guide you through the process of performing **hyperparameter optimization**, demonstrating how to define the search space, objective function, and algorithm selection. Throughout the demo, you will utilize *MLflow* to seamlessly track the model tuning process, capturing essential information such as hyperparameters, metrics, and intermediate results. By the end of the session, you will not only grasp the principles of hyperparameter optimization but also be proficient in finding the best-tuned model using various methods such as the **MLflow API** and **MLflow UI**.
# MAGIC
# MAGIC By integrating Optuna and MLflow, you can efficiently optimize hyperparameters and maintain comprehensive records of your machine learning experiments, facilitating reproducibility and collaborative research.
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC
# MAGIC *By the end of this demo, you will be able to:*
# MAGIC
# MAGIC - Perform hyperparameter optimization using Optuna.
# MAGIC - Track the model tuning process with MLflow.
# MAGIC - Query previous runs from an experiment using the `MLflowClient`.
# MAGIC - Review an MLflow Experiment for visualizing results and selecting the best run.
# MAGIC - Read in the best model, make a prediction, and register the model to Unity Catalog. 
# MAGIC
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

# MAGIC %pip install -U -qq optuna
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-2.1

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
# MAGIC Before we start fitting a model, we need to prepare dataset. First, we will load dataset, then we will split it to train and test sets.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Dataset
# MAGIC
# MAGIC In this demo we will be using the CDC Diabetes dataset from the Databricks Marketplace. This dataset has been read in and written to a feature table called `diabetes` in our working schema.

# COMMAND ----------

# load data from the feature table
table_name = f"{DA.catalog_name}.{DA.schema_name}.diabetes"
diabetes_dataset = spark.read.table(table_name)
diabetes_pd = diabetes_dataset.drop('unique_id').toPandas()

# review dataset and schema
display(diabetes_pd)
print(diabetes_pd.info())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Train/Test Split
# MAGIC
# MAGIC Next, we will divide the dataset to training and testing sets.

# COMMAND ----------

from sklearn.model_selection import train_test_split

print(f"We have {diabetes_pd.shape[0]} records in our source dataset")

# split target variable into it's own dataset
target_col = "Diabetes_binary"
X_all = diabetes_pd.drop(labels=target_col, axis=1)
y_all = diabetes_pd[target_col]

# test / train split
X_train, X_test, y_train, y_test = train_test_split(X_all, y_all, train_size=0.95, random_state=42)

y_train = y_train.astype(float)
y_test = y_test.astype(float)

print(f"We have {X_train.shape[0]} records in our training dataset")
print(f"We have {X_test.shape[0]} records in our test dataset")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hyperparameter Tuning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the Objective Function
# MAGIC
# MAGIC An objective function in Optuna is a Python function that defines the optimization target. It takes a single argument, typically named trial, which is an instance of the optuna.Trial class. This function is responsible for:
# MAGIC
# MAGIC 1. Defining the hyperparameter search space
# MAGIC
# MAGIC 1. Training the model with the suggested hyperparameters
# MAGIC
# MAGIC 1. Evaluating the model's performance
# MAGIC
# MAGIC 1. Returning a scalar value that Optuna will try to optimize (minimize or maximize)
# MAGIC
# MAGIC In our case, we are working with scikit-learn's `DecisionTreeClassifier`. Start by defining the search space for the model. Our hyperparameters are:
# MAGIC - `criterion`: chooses between `gini` and `entropy`. Defining the criterion parameter allows the algorithm to try both options during tuning and can assist in identifying which criterion works best. [TPE](https://optuna.readthedocs.io/en/stable/reference/samplers/generated/optuna.samplers.TPESampler.html#optuna.samplers.TPESampler) is the [default](https://optuna.readthedocs.io/en/stable/tutorial/10_key_features/003_efficient_optimization_algorithms.html#pruning), though there are [other sampling methods](https://optuna.readthedocs.io/en/stable/reference/samplers/index.html) like GPSampler and BruteForceSampler.
# MAGIC - `max_depth`: an integer between 5 and 50
# MAGIC - `min_samples_split`: an integer between 2 and 40
# MAGIC - `min_samples_leaf`: an integer between 1 and 20
# MAGIC
# MAGIC The objective function will also have a nested MLflow runs for logging each trial start a new MLflow run for each trial using `with mlflow.start_run()`. We will also manually log metrics and the scikit-learn model within the objective function. Note that the training process is using cross-validation (5-fold CV in fact) and returns the negative mean of the fold results. 
# MAGIC
# MAGIC
# MAGIC >   - _**Gini impurity** measures how often a randomly chosen sample would be incorrectly classified if randomly labeled according to the current class distribution. It quantifies the probability of misclassification._
# MAGIC >  - _**Entropy** measures the amount of uncertainty or disorder in the dataset. It quantifies how “impure” a node is in terms of class distribution, with higher entropy meaning more disorder (i.e., more uncertainty in classification)._

# COMMAND ----------

from pyspark.ml.linalg import Vectors
df = spark.sparkContext.parallelize(Vectors.dense([6]))

# COMMAND ----------

display(df)

# COMMAND ----------

import optuna
import mlflow
import mlflow.sklearn
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import cross_validate
from mlflow.models.signature import infer_signature

# Define the objective function
def optuna_objective_function(trial):
    # Define hyperparameter search space
    params = {
        'criterion': trial.suggest_categorical('criterion', ['gini', 'entropy']),
        'max_depth': trial.suggest_int('max_depth', 5, 50),
        'min_samples_split': trial.suggest_int('min_samples_split', 2, 40),
        'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 20)
    }
    
    # Start an MLflow run for logging
    with mlflow.start_run(nested=True, run_name=f"Model Tuning with Optuna - Trial {trial.number}"):

        # Log parameters with MLflow
        mlflow.log_params(params)

        dtc = DecisionTreeClassifier(**params)
        scoring_metrics = ['accuracy', 'precision', 'recall', 'f1']
        cv_results = cross_validate(dtc, X_train, y_train, cv=5, scoring=scoring_metrics, return_estimator=True)
        
        # Log cross-validation metrics to MLflow
        for metric in scoring_metrics:
            mlflow.log_metric(f'cv_{metric}', cv_results[f'test_{metric}'].mean())

        # Train the model on the full training set
        final_model = DecisionTreeClassifier(**params)
        final_model.fit(X_train, y_train)

        # Create input signature using the first row of X_train
        input_example = X_train.iloc[[0]]
        signature = infer_signature(input_example, final_model.predict(input_example))

        # Log the model with input signature
        mlflow.sklearn.log_model(final_model, "decision_tree_model", signature=signature, input_example=input_example)

        # Compute the mean from cross-validation
        f1_score_mean = cv_results['test_f1'].mean()

        # Metric to be minimized
        return -f1_score_mean

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize the Scikit-Learn Model on Single-Machine Optuna and Log Results with MLflow
# MAGIC
# MAGIC Before running the optimization, we need to perform two key steps:
# MAGIC
# MAGIC 1. **Initialize an Optuna Study** using `optuna.create_study()`.  
# MAGIC    - A *study* represents an optimization process consisting of multiple trials.  
# MAGIC    - A *trial* is a single execution of the *objective function* with a specific set of hyperparameters.  
# MAGIC
# MAGIC 2. **Run the Optimization** using `study.optimize()`.  
# MAGIC    - This tells Optuna how many trials to perform and allows it to explore the search space.  
# MAGIC
# MAGIC Each trial will be logged to MLflow, including the hyperparameters tested and their corresponding cross-validation results. Optuna will handle the optimization while training continues.
# MAGIC
# MAGIC #### **Steps:**
# MAGIC - **Set up an Optuna study** with `optuna.create_study()`. 
# MAGIC - **Start an MLflow run** with `mlflow.start_run()` to log experiments. 
# MAGIC - **Optimize hyperparameters** using `study.optimize()` within the MLflow context.
# MAGIC
# MAGIC > **Note on `n_jobs` in `study.optimize()`:**  
# MAGIC > The `n_jobs` argument controls the **number of trials running in parallel** using multi-threading **on a single machine**.  
# MAGIC > - If `n_jobs=-1`, Optuna will use **all available CPU cores** (e.g., on a 4-core machine, it will likely use all 4 cores).  
# MAGIC > - If `n_jobs` is **undefined (default)**, trials run **sequentially (single-threaded)**.  
# MAGIC > - **Important:** `n_jobs` does **not** distribute trials across multiple nodes in a Spark cluster. To parallelize across nodes, use `SparkTrials()` instead.
# MAGIC
# MAGIC > **Why We Don't Use `MLflowCallback`:**  
# MAGIC > Optuna provides an [`MLflowCallback`](https://optuna.readthedocs.io/en/v2.0.0/reference/generated/optuna.integration.MLflowCallback.html) for automatic logging. However, in this demo, we are demonstrating how to integrate the MLflow API with Optuna separate from `MLflowCallback`.

# COMMAND ----------

# MAGIC %md
# MAGIC First, we will delete all previous runs to keep our workspace and experiment tidy.

# COMMAND ----------

# Set the MLflow experiment name and get the id
experiment_name = f"/Users/{DA.username}/Hyperparameter_Tuning_with_Optuna_{DA.schema_name}"
print(f"Experiment Name: {experiment_name}")
mlflow.set_experiment(experiment_name)
experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
print(f"Experiment ID: {experiment_id}")

print("Clearing out old runs (If you want to add more runs, change the n_trial parameter in the next cell) ...")
# Get all runs
runs = mlflow.search_runs(experiment_ids=[experiment_id], output_format="pandas")

if runs.empty:
    print("No runs found in the experiment.")
else:
    # Iterate and delete each run
    for run_id in runs["run_id"]:
        mlflow.delete_run(run_id)
        print(f"Deleted run: {run_id}")

    print("All runs have been deleted.")

# COMMAND ----------

study = optuna.create_study(
    study_name="optuna_hpo",
    direction="maximize"
)

with mlflow.start_run(run_name='demo_optuna_hpo_max') as parent_run:
    # Run optimization
    study.optimize(
        optuna_objective_function, 
        n_trials=10
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Review Tuning Results
# MAGIC
# MAGIC We can use the MLflow API to review the trial results.

# COMMAND ----------

import mlflow
import pandas as pd

# Define your experiment name or ID
experiment_id = parent_run.info.experiment_id # Replace with your actual experiment ID

# Fetch all runs from the experiment
df_runs = mlflow.search_runs(
  experiment_ids=[experiment_id]
  )

df_runs = df_runs[df_runs['tags.mlflow.runName'] != 'demo_optuna_hpo']

display(df_runs)

# COMMAND ----------

# MAGIC %md
# MAGIC We can use the Optuna study to get the best parameters and F1-score. Validate this agrees with the table results from the previous cell's output.

# COMMAND ----------

# Display the best hyperparameters and metric
print(f"Best hyperparameters: {study.best_params}")
print(f"Best negative-F1 score: {study.best_value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the Best Run Based on F1-Score
# MAGIC
# MAGIC In this section, we will search for registered models. There are couple of ways to achieve this. We will show how to search runs using MLflow API and the UI.
# MAGIC
# MAGIC **The output links for using Optuna gave the best runs. Why can't we just use that?**
# MAGIC
# MAGIC You totally can! But this is the same as using the UI to navigate to the trial that was the best (which is shown below).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1: Find the Best Run - MLFlow API
# MAGIC
# MAGIC Using the MLFlow API, you can search runs in an experiment, which returns results into a Pandas DataFrame.

# COMMAND ----------

experiment_id = parent_run.info.experiment_id
print(f"Experiment ID: {experiment_id}")

# COMMAND ----------

from mlflow.entities import ViewType

search_runs_pd = mlflow.search_runs(
    experiment_ids=[experiment_id],
    order_by=["metrics.cv_f1 DESC"],
    max_results=1
)

display(search_runs_pd)

# COMMAND ----------

from mlflow.entities import ViewType

search_runs_pd = mlflow.search_runs(
    experiment_ids=[experiment_id],
    order_by=["metrics.cv_f1 DESC"],
    max_results=1
)

search_runs_pd = mlflow.search_runs(
    experiment_ids=[experiment_id],
    order_by=["metrics.cv_f1 ASC"],
    max_results=1
)

display(search_runs_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2 - Find the Best Run - MLflow UI
# MAGIC
# MAGIC The simplest way of seeing the tuning result is to use MLflow UI. 
# MAGIC
# MAGIC 1. Click on **Experiments** from left menu.
# MAGIC
# MAGIC 1. Select experiment which has the same name as this notebook's title (**`Hyperparameter_Tuning_with_Optuna_{schema_name}`**).
# MAGIC
# MAGIC 1. Click on the graph icon at the top left under **Runs**.
# MAGIC
# MAGIC 1. Click on the parent run or manually select all 10 runs to compare. The graphs on the right of the screen will appear for inspection.

# COMMAND ----------

# MAGIC %md
# MAGIC # Visualize the Hyperparameters 
# MAGIC
# MAGIC By now, we have determined which trial had the best run according to the f1-score. Now, let's visually inspect our other search space elements with respect to this metric.

# COMMAND ----------

import matplotlib.pyplot as plt


# Ensure the necessary parameters exist in the DataFrame before plotting
required_params = ["params.min_samples_leaf", "params.max_depth", "params.min_samples_split", "metrics.cv_f1", "tags.mlflow.runName"]
df_filtered = df_runs.dropna(subset=required_params, how="any")

# Convert parameters to appropriate types
df_filtered["params.min_samples_split"] = df_filtered["params.min_samples_split"].astype(float)
df_filtered["params.max_depth"] = df_filtered["params.max_depth"].astype(float)
df_filtered["metrics.cv_f1"] = df_filtered["metrics.cv_f1"].astype(float)

# Identify the best run index (assuming higher f1 is better)
best_run_index = df_filtered["metrics.cv_f1"].idxmax()
best_run_name = df_filtered.loc[best_run_index, "tags.mlflow.runName"]

# Extract run names for x-axis labels
run_names = df_filtered["tags.mlflow.runName"]

# Create a figure and axis for bar chart
fig, ax1 = plt.subplots(figsize=(12, 6))

# Bar chart for min_samples_split and max_depth
df_filtered[["params.min_samples_split", "params.max_depth"]].plot(kind="bar", ax=ax1, edgecolor="black")

ax1.set_xlabel("Run Name")
ax1.set_ylabel("Parameter Values")
ax1.set_title("Hyperparameters & cv_f1 Score per Run")
ax1.legend(["Max Features", "Max Depth"])
ax1.set_xticks(range(len(df_filtered)))
ax1.set_xticklabels(run_names, rotation=45, ha="right")  # Rotate for readability

# Create a second y-axis for the cv_f1 score line chart
ax2 = ax1.twinx()
ax2.plot(
    range(len(df_filtered)),  # X-axis indices
    df_filtered["metrics.cv_f1"],
    marker="o",
    linestyle="-",
    color="blue",
    label="cv_f1 Score"
)

# Highlight the best run with a bold marker
ax2.plot(
    df_filtered.index.get_loc(best_run_index),  # Get positional index
    df_filtered.loc[best_run_index, "metrics.cv_f1"],
    marker="o",
    markersize=10,
    color="red",
    label="Best Run"
)

# Add a vertical dashed line to indicate the best run
ax2.axvline(df_filtered.index.get_loc(best_run_index), color="red", linestyle="--")

ax2.set_ylabel("cv_f1 Score")

# Add legend
fig.legend(loc="upper left", bbox_to_anchor=(0.1, 0.9))
plt.show()

# Pie chart for criterion
plt.figure(figsize=(8, 8))
df_filtered["params.criterion"].value_counts().plot(kind="pie", autopct="%1.1f%%", startangle=90)
plt.title("Criterion Distribution")
plt.ylabel("")  # Hide y-label for better visualization
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the Best Model and Parameters
# MAGIC To load the model and make a prediction, let's use the information from Option 2 shown above. Run the next cell to get the value.
# MAGIC
# MAGIC
# MAGIC ### Copy and Paste Option
# MAGIC Alternatively, you can set the variables shown below manually. Using either the output from Option 1 or Option 2 or the UI from Option 3, locate the `run_id` and the `experiment_id`. With Option 1 or 2, this is simply the value in the first two columns. In the UI, this is presented to you in the Details table when clicking on the specific run.

# COMMAND ----------

type(search_runs_pd)

# COMMAND ----------

#convert search_runs_pd to pyspark dataframe
search_runs_sd = spark.createDataFrame(search_runs_pd)


# Get the string value from run_id and experiment_id from PySpark DataFrame hpo_runs_df
run_id = search_runs_sd.select("run_id").collect()[0][0]
experiment_id = search_runs_sd.select("experiment_id").collect()[0][0]

print(f"Run ID: {run_id}")
print(f"Experiment ID: {experiment_id}")

# COMMAND ----------

import mlflow
import json
from mlflow.models import Model

# Grab an input example from the test set
input_example = X_test.iloc[[0]]

model_path = f"dbfs:/databricks/mlflow-tracking/{experiment_id}/{run_id}/artifacts/decision_tree_model"

# Load the model using the run ID
loaded_model = mlflow.pyfunc.load_model(model_path)

# Retrieve model parameters
client = mlflow.tracking.MlflowClient()
params = client.get_run(run_id).data.params

# Display model parameters
print("Best Model Parameters:")
print(json.dumps(params, indent=4))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Make a Prediction

# COMMAND ----------

# Make a prediction
test_prediction = loaded_model.predict(input_example)
# X_test is a pandas dataframe - let's add the test_prediction output as a new column
input_example['prediction'] = test_prediction
display(input_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register the Model to Unity Catalog
# MAGIC
# MAGIC After running the following cell, navigate to our working catalog and schema (see course setup above) and validate the model has been registered.

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
model_uri = f'runs:/{run_id}/decision_tree_model'
mlflow.register_model(model_uri=model_uri, name=f"{DA.catalog_name}.{DA.schema_name}.demo_optuna_model")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this demo, we've explored how to enhance your model's performance using **Optuna** for hyperparameter optimization and **MLflow** for tracking the tuning process. By employing Optuna's efficient search algorithms, you've learned to fine-tune your model's parameters effectively. Simultaneously, MLflow has facilitated seamless monitoring and logging of each trial, capturing essential information such as hyperparameters, metrics, and intermediate results. Additionally, you learned how to register the best model within Unity Catalog. Moving forward, integrating these tools into your workflow will be instrumental in improving your model's performance and simplifying the fine-tuning process.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
