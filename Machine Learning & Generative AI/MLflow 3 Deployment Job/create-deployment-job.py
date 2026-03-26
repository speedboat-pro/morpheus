# Databricks notebook source
# MAGIC %md
# MAGIC # INSTRUCTIONS
# MAGIC Running this notebook will create a Databricks Job templated as a MLflow 3.0 Deployment Job. This job will have three tasks: Evaluation, Approval_Check, and Deployment. The Evaluation task will evaluate the model on a dataset, the Approval_Check task will check if the model has been approved for deployment using UC Tags and the Approval button in the UC Model UI, and the Deployment task will deploy the model to a serving endpoint.
# MAGIC
# MAGIC 1. Copy the example deployment jobs template notebooks ([AWS](https://docs.databricks.com/aws/mlflow/deployment-job#example-template-notebooks) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#example-template-notebooks) | [GCP](https://docs.databricks.com/gcp/mlflow/deployment-job#example-template-notebooks)) into your Databricks Workspace.
# MAGIC 2. Create a UC Model or use an existing one. For example, see the MLflow 3 examples ([AWS](https://docs.databricks.com/aws/mlflow/mlflow-3-install#example-notebooks) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/mlflow-3-install#example-notebooks) | [GCP](https://docs.databricks.com/gcp/mlflow/mlflow-3-install#example-notebooks)).
# MAGIC 3. Update the REQUIRED values in the next cell before running the notebook.
# MAGIC 4. After running the notebook, the created job will not be connected to any UC Model. You will still need to **connect the job to a UC Model** in the UC Model UI as indicated in the documentation or using MLflow as shown in the final cell ([AWS](https://docs.databricks.com/aws/mlflow/deployment-job#connect) | [Azure](https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#connect) | [GCP](https://docs.databricks.com/gcp/mlflow/deployment-job#connect)).

# COMMAND ----------

# MAGIC %pip install mlflow --upgrade
# MAGIC %restart_python

# COMMAND ----------

# DBTITLE 1,Set up MLflow

# COMMAND ----------

user = dbutils.widgets.get("user")
repo_name = dbutils.widgets.get("repo_name")


# COMMAND ----------

# REQUIRED: Update these values as necessary
model_name = f"dbacademy.{user.split('@')[0]}.custom_ml_model" # The name of the already created UC Model
model_version = "1" # The version of the already created UC Model
job_name = "example_deployment_job" # The desired name of the deployment job

# REQUIRED: Create notebooks for each task and populate the notebook path here, replacing the INVALID PATHS LISTED BELOW.
# These paths should correspond to where you put the notebooks templated from the example deployment jobs template notebook
# in your Databricks workspace. Choose an evaluation notebook based on if the model is for GenAI or classic ML
evaluation_notebook_path = f"/Workspace/Users/{user}/{repo_name}/Machine Learning & Generative AI/MLflow 3 Deployment Job//evaluation"
approval_notebook_path = f"/Workspace/Users/{user}/{repo_name}/Machine Learning & Generative AI/MLflow 3 Deployment Job//approval"
deployment_notebook_path = f"/Workspace/Users/{user}/{repo_name}/Machine Learning & Generative AI/MLflow 3 Deployment Job//deployment"

# COMMAND ----------

# Create job with necessary configuration to connect to model as deployment job
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()
job_settings = jobs.JobSettings(
    name=job_name,
    tasks=[
        jobs.Task(
            task_key="Evaluation",
            notebook_task=jobs.NotebookTask(notebook_path=evaluation_notebook_path),
            max_retries=0,
        ),
        jobs.Task(
            task_key="Approval_Check",
            notebook_task=jobs.NotebookTask(
                notebook_path=approval_notebook_path,
                base_parameters={"approval_tag_name": "{{task.name}}"}
            ),
            depends_on=[jobs.TaskDependency(task_key="Evaluation")],
            max_retries=0,
        ),
        jobs.Task(
            task_key="Deployment",
            notebook_task=jobs.NotebookTask(notebook_path=deployment_notebook_path),
            depends_on=[jobs.TaskDependency(task_key="Approval_Check")],
            max_retries=0,
        ),
    ],
    parameters=[
        jobs.JobParameter(name="model_name", default=model_name),
        jobs.JobParameter(name="model_version", default=model_version),
    ],
    queue=jobs.QueueSettings(enabled=True),
    max_concurrent_runs=1,
)

created_job = w.jobs.create(**job_settings.__dict__)
print("Use the job name " + job_name + " to connect the deployment job to the UC model " + model_name + " as indicated in the UC Model UI.")
print("\nFor your reference, the job ID is: " + str(created_job.job_id))
print("\nDocumentation: \nAWS: https://docs.databricks.com/aws/mlflow/deployment-job#connect \nAzure: https://learn.microsoft.com/azure/databricks/mlflow/deployment-job#connect \nGCP: https://docs.databricks.com/gcp/mlflow/deployment-job#connect")

# COMMAND ----------

model_name

# COMMAND ----------

# Optionally, you can programmatically link the deployment job to a UC model

import mlflow
from mlflow.tracking.client import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")

try:
  if client.get_registered_model(model_name):
    client.update_registered_model(model_name, deployment_job_id=created_job.job_id)
except mlflow.exceptions.RestException:
  client.create_registered_model(model_name, deployment_job_id=created_job.job_id)

# COMMAND ----------


