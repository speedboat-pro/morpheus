# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo: HPO with Ray Tune
# MAGIC
# MAGIC In this demo, you will learn how to use **Ray Tune** — a powerful hyperparameter optimization framework — to tune machine learning models in **Databricks**. 
# MAGIC
# MAGIC We will demonstrate how to implement the Ray Tune framework using a **Random Forest Regressor** from Scikit-Learn, covering:
# MAGIC - **Defining search spaces**
# MAGIC - **Creating objective functions**
# MAGIC - **Optimizing hyperparameters** 
# MAGIC
# MAGIC
# MAGIC Additionally, we will track and log the results using **MLflow**, enabling efficient management and monitoring of the tuning process.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## **Learning Objectives**
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC - Define an **objective function** specific to Ray Tune.
# MAGIC - Set up **Optuna-style search spaces** within Ray Tune.
# MAGIC - Configure **Ray Tune's Tuner and compute resources**.
# MAGIC - Optimize hyperparameters using **parallel execution**.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Tuning with Ray Tune and a Single-Machine Model
# MAGIC In this part, we will use **Ray for distributed hyperparameter optimization** while training a **Scikit-Learn** model.
# MAGIC
# MAGIC ### How This Works:
# MAGIC - **Data is converted from a Spark DataFrame to Pandas** to enable single-machine model training.
# MAGIC - **Ray Tune runs on a single machine** but distributes trial execution across *multiple* CPU threads to speed up hyperparameter tuning.
# MAGIC - **MLflow tracks the experiment**, logging the best hyperparameters and model performance.
# MAGIC
# MAGIC This approach allows us to use **Ray for distributed hyperparameter search**, while **training the model on a single node** to take advantage of Scikit-Learn’s efficient implementations.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC    
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need a classic cluster running one of the following Databricks runtime(s): **16.3.x-cpu-ml-scala2.12**. **Do NOT use serverless compute to run this notebook**.

# COMMAND ----------

# MAGIC %pip install -U optuna optuna-integration mlflow
# MAGIC %pip install --upgrade ray[tune]
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Before starting the demo, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-02.1b

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
print(f"Dataset Location:  {DA.paths.datasets.wine_quality}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure a Ray Cluster
# MAGIC Let's begin by setting up a single-machine (driver-only) Ray cluster by defining the following:
# MAGIC - 3 CPU cores allocated for the head node, leaving 1 core for Spark
# MAGIC > The Vocareum environment allocates 4 CPUs per user for this demonstration. 
# MAGIC - 1 worker node
# MAGIC - 4 CPUs per worker
# MAGIC
# MAGIC Additionally, we will initialize the Ray cluster on spark with the above configurations with `ray.init()` and defining the environment variable `RAY_ADDRESS`.

# COMMAND ----------

import os
import ray
from ray.util.spark import setup_ray_cluster, shutdown_ray_cluster

# Attempt to shut down any existing Ray cluster
try:
    shutdown_ray_cluster()
    print("Existing Ray cluster shutdown successfully.")
except Exception as e:
    print(f"Warning: No active Ray cluster to shut down. Details: {e}")

# Set up configurations for a single-machine (driver-only) Ray cluster
num_cpus_head_node = 3  # Use 3 CPU cores, leaving 1 for Spark
num_worker_nodes = 1  # Single-node setup (driver only)
num_cpu_cores_per_worker = 4  # Unused since there's only one worker

# Initialize the Ray cluster on Spark
ray_conf = setup_ray_cluster(
    min_worker_nodes=num_worker_nodes,  
    max_worker_nodes=num_worker_nodes,
    num_cpus_head_node=num_cpus_head_node,  
    num_gpus_head_node=0  # No GPU usage
)

# Initialize Ray with the configured settings
ray.init(ignore_reinit_error=True)
print(f"Ray initialized with address: {ray_conf[0]}")

# Set Ray address for Spark integration
os.environ['RAY_ADDRESS'] = ray_conf[0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Preparation for Distributed Optuna with Single-Machine Training
# MAGIC
# MAGIC In this task, before we define the objective function, we need to convert our Spark DataFrame to a Pandas DataFrame. This will allow us to split the data into training and test sets and standardize the features. These steps are essential for conducting single-node model training using **Scikit-learn**.
# MAGIC
# MAGIC **Instructions**:
# MAGIC
# MAGIC 1. **Convert the Spark DataFrame** into a Pandas DataFrame for single-node processing.
# MAGIC 2. **Split the dataset** into training and test sets using **Scikit-learn**'s `train_test_split` method.
# MAGIC 3. **Standardize the features** to ensure the model performs optimally.
# MAGIC

# COMMAND ----------

import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

# Convert Spark DataFrame to Pandas for single-machine training
train_pandas = train_df.toPandas()

# Separate features and labels
X = train_pandas[feature_columns]
y = train_pandas[label_column]

# Split the data into training and test sets using Scikit-learn
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Standardize the features for better model performance
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.transform(X_test)

# Display the shapes of the training and test sets
print(f"Training data shape: {X_train.shape}, Test data shape: {X_test.shape}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the Ray Tune Objective Function and Search Space for Distributed Hyperparameter Search (Single-Machine Training)
# MAGIC
# MAGIC In this step, we will define the **objective function** for hyperparameter tuning using **Ray Tune**. 
# MAGIC
# MAGIC This function will:
# MAGIC - Suggest hyperparameters dynamically.
# MAGIC - Train and evaluate a `RandomForest` model using **Scikit-learn**.
# MAGIC - Utilize **cross-validation** for performance evaluation.
# MAGIC
# MAGIC #### **Key Differences from the Previous Demonstration**
# MAGIC - Unlike the previous approach, we now integrate **cross-validation** for model evaluation.
# MAGIC - Cross-validation is feasible in **single-node** training but can be **resource-intensive** in a distributed setup.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### **Instructions**
# MAGIC 1. **Define the Hyperparameter Search Space**  
# MAGIC    - Use **Ray Tune's API** for defining search spaces.  
# MAGIC    - [Tune Search Space API](https://docs.ray.io/en/latest/tune/api/search_space.html)
# MAGIC
# MAGIC 2. **Configure the Search Algorithm**  
# MAGIC    - Use **Ray Tune’s Search Algorithms** for efficient exploration.  
# MAGIC    - [Tune Search Algorithms](https://docs.ray.io/en/latest/tune/api/suggestion.html)
# MAGIC
# MAGIC 3. **Implement the Objective Function**  
# MAGIC    - Train a **RandomForest model** using **Scikit-learn**.  
# MAGIC    - Optimize hyperparameters within the function.
# MAGIC
# MAGIC 4. **Set Up & Execute the Tuning Process**  
# MAGIC    - Use **Ray Tune Execution (`tune.Tuner`)** to configure and run the tuning process.  
# MAGIC    - Apply key configurations like:
# MAGIC      - `TuneConfig`
# MAGIC      - `RunConfig`
# MAGIC      - `CheckpointConfig`
# MAGIC      - `FailureConfig`  
# MAGIC    - [Tune Execution (`tune.Tuner`)](https://docs.ray.io/en/latest/tune/api/execution.html)
# MAGIC    - [`ray.tune.with_parameters`](https://docs.ray.io/en/latest/tune/api/doc/ray.tune.with_parameters.html)
# MAGIC
# MAGIC 5. **Evaluate Model Performance**  
# MAGIC    - Use **cross-validation** and return **negative RMSE** (to be minimized).
# MAGIC

# COMMAND ----------

from ray import tune

# Define the hyperparameter search space for RandomForest tuning
search_space = {
    "n_estimators": tune.randint(50, 300),  # Number of trees in the forest (wider range)
    "max_depth": tune.randint(3, 30)  # Depth of the trees (realistic range)
}

# COMMAND ----------

import mlflow
from mlflow.types.utils import _infer_schema
from mlflow.exceptions import MlflowException
from mlflow.models.signature import infer_signature
from mlflow.utils.databricks_utils import get_databricks_env_vars
from ray import tune
from ray.air.integrations.mlflow import MLflowLoggerCallback, setup_mlflow
from ray.tune.search import ConcurrencyLimiter
from ray.tune.search.optuna import OptunaSearch

# Retrieve Databricks MLflow credentials
mlflow_db_creds = get_databricks_env_vars("databricks")

if not mlflow_db_creds:
    raise ValueError("Databricks MLflow credentials could not be retrieved.")

# Set up MLflow experiment
# MLflow Experiment Setup
experiment_name_ray = os.path.join(
    os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()),
    "02b - Model Tuning with Ray"
)
mlflow.set_experiment(experiment_name_ray)
experiment_id_ray = mlflow.get_experiment_by_name(experiment_name_ray).experiment_id

# Define Optuna search algorithm
searcher = OptunaSearch(metric="rmse", mode="min")  # Minimize RMSE
algo = ConcurrencyLimiter(searcher, max_concurrent=3)  # Limit concurrent trials to 3

# COMMAND ----------

import pandas as pd
import mlflow
import os
import ray
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import cross_val_score
from typing import Dict, Any

def objective_ray_scikit(
    config: Dict[str, Any],
    parent_run_id: str,
    X_train_in: pd.DataFrame,
    y_train_in: pd.Series,
    experiment_name_in: str,
    mlflow_db_creds_in: Dict[str, str]
):
    """
    Objective function for Ray Tune hyperparameter optimization using cross-validation.

    Args:
        config (Dict[str, Any]): Hyperparameter configuration from Ray Tune.
        parent_run_id (str): MLflow parent run ID for nested tracking.
        X_train_in (pd.DataFrame): Training features.
        y_train_in (pd.Series): Training labels.
        experiment_name_in (str): MLflow experiment name.
        mlflow_db_creds_in (Dict[str, str]): Databricks MLflow credentials.
    
    Returns:
        Dict[str, float]: Dictionary containing RMSE (lower is better).
    """

    try:
        # Update Databricks credentials for Ray (executors restart each run)
        if mlflow_db_creds_in:
            os.environ.update(mlflow_db_creds_in)

        # Start nested MLflow run under the parent run
        with mlflow.start_run(nested=True, experiment_id=experiment_id_ray, tags={"mlflow.parentRunId": parent_run_id}):
            # Extract hyperparameters from Ray Tune config
            n_estimators = config["n_estimators"]
            max_depth = config["max_depth"]

            # Initialize RandomForest Regressor
            model = RandomForestRegressor(
                n_estimators=n_estimators, 
                max_depth=max_depth, 
                random_state=42
            )

            # Perform 3-fold cross-validation and compute RMSE
            scores = cross_val_score(
                model, X_train_in, y_train_in, 
                scoring='neg_root_mean_squared_error', 
                cv=3
            )

            mean_rmse = -scores.mean()  # Convert negative RMSE to positive

            # Log hyperparameters and metrics in MLflow
            mlflow.log_params(config)
            mlflow.log_metric("RMSE", mean_rmse)

            return {"rmse": mean_rmse}

    except Exception as e:
        print(f"Error in objective function: {e}")
        return {"rmse": float("inf")}  # Return a large RMSE in case of failure

# COMMAND ----------

# Start the parent MLflow run
with mlflow.start_run(run_name="ray_tune", experiment_id=experiment_id_ray) as parent_run:
    os.environ.update(mlflow_db_creds)  # Ensure Ray executors have credentials

    # Set up and execute Ray Tune with the objective function and search space
    tuner = tune.Tuner(
        tune.with_parameters(
            objective_ray_scikit,
            parent_run_id=parent_run.info.run_id,
            X_train_in=X_train,
            y_train_in=y_train,
            experiment_name_in=experiment_name_ray,
            mlflow_db_creds_in=mlflow_db_creds
        ),
        tune_config=tune.TuneConfig(
            search_alg=algo,
            num_samples=10,
            reuse_actors=True  # Keeps actors alive for efficiency
        ),
        param_space=search_space
    )

    # Run tuning and retrieve the best result
    multinode_results = tuner.fit()
    best_result = multinode_results.get_best_result(metric="rmse", mode="min", scope="last")

    if best_result is None:
        raise ValueError("No best trial found. Ensure the tuning job ran successfully.")

    # Extract best trial details
    best_trial_number = best_result.metrics.get("trial_id", "N/A")  # Default if missing
    best_model_params = best_result.config
    best_model_params["random_state"] = 42  # Ensures reproducibility
    best_rmse = best_result.metrics["rmse"]

    # Train the best model using the best hyperparameters
    best_model = RandomForestRegressor(**best_model_params)
    best_model.fit(X_train, y_train)

    # Enable MLflow autologging (disable model logging to avoid conflicts)
    mlflow.sklearn.autolog(log_input_examples=True, log_models=False, silent=True)

    # Infer model output schema
    try:
        output_schema = _infer_schema(y_train)
    except Exception as e:
        warnings.warn(f"Could not infer model output schema: {e}")
        output_schema = None

    # Infer model signature
    input_example = X_train[:3]  # Use a small subset as an example
    signature = infer_signature(X_train, best_model.predict(X_train))

    # Set model name for MLflow registration
    model_name = f"{DA.catalog_name}.{DA.schema_name}.hpo_model_ray_tune_optuna"

    # Display results
    print(f"Best Trial Number: {best_trial_number}")
    print(f"Best Hyperparameters: {best_model_params}")
    print(f"Best RMSE: {best_rmse:.4f}")

    # Log the best model to MLflow
    with mlflow.start_run(run_name="best_trial_ray_scikit_results", nested=True):
        mlflow.sklearn.log_model(
            sk_model=best_model,
            artifact_path="model",
            signature=signature,
            input_example=input_example,
            registered_model_name=model_name
        )
        mlflow.log_params(best_model_params)
        mlflow.log_metric("Best RMSE", best_rmse)

# Ensure MLflow run is properly closed
mlflow.end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC Shut down the ray cluster.

# COMMAND ----------

ray.shutdown()

# COMMAND ----------

# MAGIC %md
# MAGIC # Conclusion
# MAGIC
# MAGIC In this demo, we explored how to setup and execute model training with Ray Tune on a single-machine. Additionally, we walked through the core components needed to perform model training with the Ray framework such as defining a search space, building objective functions, and optimization of hyperparameters.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
