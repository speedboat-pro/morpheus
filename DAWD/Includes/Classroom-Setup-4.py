# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

from databricks.sdk.service import pipelines
import os
import time
from databricks.sdk import WorkspaceClient


class DataFactory:
    def __init__(self):
        
        # Bind the stream-source to DA because we will use it again later.
        DA.paths.stream_source = f"{DA.paths.working_dir}/stream-source"
        
        self.source_dir = f"{DA.paths.datasets.retail}/retail-pipeline"
        self.target_dir = DA.paths.stream_source
        
        # All three datasets *should* have the same count, but just in case,
        # We are going to take the smaller count of the three datasets
        orders_count = len(dbutils.fs.ls(f"{self.source_dir}/orders/stream_json"))
        status_count = len(dbutils.fs.ls(f"{self.source_dir}/status/stream_json"))
        customer_count = len(dbutils.fs.ls(f"{self.source_dir}/customers/stream_json"))
        self.max_batch = min(min(orders_count, status_count), customer_count)
        
        self.current_batch = 0
        
    def load(self, continuous=False, delay_seconds=5):
        import time
        self.start = int(time.time())
        
        if self.current_batch >= self.max_batch:
            print("Data source exhausted\n")
            return False
        elif continuous:
            while self.load():
                time.sleep(delay_seconds)
            return False
        else:
            print(f"Loading batch {self.current_batch+1} of {self.max_batch}", end="...")
            self.copy_file("customers")
            self.copy_file("orders")
            self.copy_file("status")
            self.current_batch += 1
            print(f"{int(time.time())-self.start} seconds")
            return True
            
    def copy_file(self, dataset_name):
        source_file = f"{self.source_dir}/{dataset_name}/stream_json/{self.current_batch:02}.json/"
        target_file = f"{self.target_dir}/{dataset_name}/{self.current_batch:02}.json"
        dbutils.fs.cp(source_file, target_file)


@DBAcademyHelper.add_method
def generate_pipeline(self, pipeline_name, notebooks_folder, pipeline_notebooks, use_schema, use_configuration=None, use_serverless=True, use_continuous=False):
    """
    Generates a Databricks pipeline based on the specified configuration parameters.
    """
    pipeline_name = f"{pipeline_name}" 
    use_catalog = f"{self.catalog_name}"

    # Get the current notebook folder path
    current_folder_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    main_course_folder_path = "/Workspace" + "/".join(current_folder_path.split("/")[:-1])

    # Create paths for the specified notebooks
    notebooks_paths = [
        f"{main_course_folder_path}/{notebook}" for notebook in pipeline_notebooks
    ]

    # Create the pipeline
    pipeline_info = self.workspace.pipelines.create(
        allow_duplicate_names=True,
        name=pipeline_name,
        catalog=use_catalog,
        target=use_schema,
        serverless=use_serverless,
        continuous=use_continuous,
        development=True,  # Development mode
        configuration=use_configuration,
        libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=path)) for path in notebooks_paths]
    )

    # Store the pipeline ID
    self.current_pipeline_id = pipeline_info.pipeline_id
    print(f"Successfully created the DLT pipeline '{pipeline_name}'.")


@DBAcademyHelper.add_method
def start_pipeline(self):
    """
    Starts or restarts the pipeline using the `start_update` method for a full refresh.
    """
    if not self.current_pipeline_id:
        raise ValueError("Pipeline ID is not set. Please create a pipeline first.")

    print(f"Starting the pipeline '{self.current_pipeline_id}' for a full refresh.")
    self.workspace.pipelines.start_update(self.current_pipeline_id)


@DBAcademyHelper.add_method
def monitor_pipeline_status(self):
    """
    Monitors the status of the Delta Live Tables pipeline.
    Prints 'Successful' for COMPLETED and 'Failed' for FAILED or CANCELED.
    """
    if not self.current_pipeline_id:
        raise ValueError("Pipeline ID is not set. Please create and start a pipeline first.")

    updates = self.workspace.pipelines.list_updates(self.current_pipeline_id).updates
    if not updates:
        raise Exception(f"No updates found for pipeline ID: {self.current_pipeline_id}. Check the DLT pipeline status in the UI.")

    # Get the most recent update
    latest_update = updates[0]
    active_update = latest_update.update_id
    print(f"Monitoring pipeline '{self.current_pipeline_id}'. Update ID: '{active_update}'.")

    while True:
        raw_status = self.workspace.pipelines.get_update(self.current_pipeline_id, active_update).update.state.value
        status = raw_status.split(".")[-1]
        print(f"Pipeline Status: {raw_status}.")

        if status == "COMPLETED":
            print("Successful")
            break
        elif status in ["FAILED", "CANCELED"]:
            print("Failed")
            break
        time.sleep(15)


@DBAcademyHelper.add_method
def modify_pipeline(self, pipeline_name, updated_notebooks, use_serverless=True):
    """
    Modifies an existing pipeline with updated notebook configurations.
    """
    import os
    from databricks.sdk.service import pipelines

    existing_pipeline_id = None
    for pipeline in self.workspace.pipelines.list_pipelines():
        if pipeline.name == pipeline_name:
            existing_pipeline_id = pipeline.pipeline_id
            break

    if not existing_pipeline_id:
        raise ValueError(f"Pipeline with name '{pipeline_name}' not found.")

    # Get the current notebook folder path
    current_folder_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    main_course_folder_path = "/Workspace" + "/".join(current_folder_path.split("/")[:-1])

    updated_notebook_paths = [
        pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=f"{main_course_folder_path}/{notebook}"))
        for notebook in updated_notebooks
    ]

    # Retrieve the existing pipeline details
    existing_pipeline = self.workspace.pipelines.get(existing_pipeline_id)

    # Update the pipeline
    self.workspace.pipelines.update(
        pipeline_id=existing_pipeline_id,
        name=existing_pipeline.spec.name,  # Required pipeline name
        catalog=existing_pipeline.spec.catalog,  # Preserve existing catalog
        target=existing_pipeline.spec.target,    # Preserve existing target
        configuration=existing_pipeline.spec.configuration,
        continuous=existing_pipeline.spec.continuous,
        development=existing_pipeline.spec.development,
        libraries=updated_notebook_paths,
        serverless=use_serverless
    )
    print(f"Pipeline '{pipeline_name}' has been successfully updated.")
    # Store the pipeline ID
    self.current_pipeline_id = existing_pipeline_id

    # Trigger a full refresh
    self.full_refresh_pipeline(existing_pipeline_id)


@DBAcademyHelper.add_method
def full_refresh_pipeline(self, existing_pipeline_id):
    """
    Triggers a full refresh for an existing pipeline.
    """
    if not existing_pipeline_id:
        raise ValueError("Pipeline ID is required for triggering a full refresh.")

    try:
        print(f"Triggering a full refresh for pipeline '{existing_pipeline_id}'.")
        self.workspace.pipelines.start_update(pipeline_id=existing_pipeline_id)
    except Exception as e:
        raise ValueError(f"Failed to trigger a full refresh for pipeline '{existing_pipeline_id}': {str(e)}")

# COMMAND ----------

@DBAcademyHelper.add_method
def delete_all_files(self):
  '''
    Utility method to delete all files from the folders in the stream-source volume to start over. Will loop over and delete any json file it finds in the specified folders within stream-source:
    Delete all files within every sub folder in the user's dbacademy.ops.username volume.
  '''
  ## List of all folders in users stream source path volume
  folder_path_in_volume = dbutils.fs.ls(f'{self.paths.stream_source}')

  ## Get all sub folders
  list_of_sub_folders = []
  for sub_folder in folder_path_in_volume:
      sub_folder_path = sub_folder.path.split(':')[1]
      list_of_sub_folders.append(sub_folder_path)
    

  for folder in list_of_sub_folders:
    files_in_folder = dbutils.fs.ls(folder)
    for file in files_in_folder:
      file_to_delete = file.path.split(':')[1]
      print(f'Deleting file: {file_to_delete}')
      dbutils.fs.rm(file_to_delete)

  print(f'All files deleted in every sub folder within: {self.paths.stream_source}.')

# COMMAND ----------

@DBAcademyHelper.add_method
def print_pipeline_config(self, language):
    "Provided by DBAcademy, this function renders the configuration of the pipeline as HTML"

    config = self.get_pipeline_config(language)

    ## Create a list of tuples that indicate what notebooks the user needs to reference
    list_of_notebook_tuples = []
    for i, path in enumerate(config.notebooks):
        notebook = (f'Notebook #{i+1} Path', path)
        list_of_notebook_tuples.append(notebook)

    ## Use the display_config_values function to display the following values as HTML output.
    ## Will list the Pipeline Name, Source, Default Catalog, Default Schema and notebook paths.
    self.display_config_values([
            ('Pipeline Name',config.pipeline_name),
        ] + list_of_notebook_tuples 
          + [('Default Catalog',self.catalog_name),
             ('Default Schema',self.schema_name), 
             ('Source',config.source)]
    )

# COMMAND ----------

@DBAcademyHelper.add_method
def validate_pipeline_config(self, pipeline_language, num_notebooks=1):
    "Provided by DBAcademy, this function validates the configuration of the pipeline"
    import json
    
    config = self.get_pipeline_config(pipeline_language)

    try:
        pipeline = self.workspace.pipelines.get(
            self.workspace_find(
                'pipelines',
                config.pipeline_name,
                api='list_pipelines'
            ).pipeline_id
        )
    except:
        assert False, f"Could not find a pipeline named {config.pipeline_name}. Please name your pipeline using the information provided in the print_pipeline_config output."

    assert pipeline is not None, "Could not find a pipeline named {config.pipeline_name}"
    assert pipeline.spec.catalog == DA.catalog_name, f"Default Catalog not set to {DA.catalog_name}"
    assert pipeline.spec.target == DA.schema_name, f"Default Schema not set to {DA.schema_name}"

    libraries = [l.notebook.path for l in pipeline.spec.libraries]
    
    def test_notebooks():
        if libraries is None: return False
        if len(libraries) != num_notebooks: return False
        for library in libraries:
            if library not in config.notebooks: return False
        return True
    
    assert test_notebooks(), "Notebooks are not properly configured"
    assert len(pipeline.spec.configuration) == 1, "Expected exactly one configuration parameter."
    assert pipeline.spec.configuration.get("source") == config.source, f"Expected the configuration parameter {config.source}"
    assert pipeline.spec.channel == "CURRENT", "Excpected the channel to be set to Current."
    assert pipeline.spec.continuous == False, "Expected the Pipeline mode to be Triggered."

    print('Pipeline validation complete. No errors found.')

# COMMAND ----------

from databricks.sdk import WorkspaceClient

def get_pipeline_url(workspace_url, pipeline_name):
    """
    Constructs the Delta Live Tables pipeline URL for a given pipeline name.

    Parameters:
    - workspace_url (str): The base URL of the Databricks workspace.
    - pipeline_name (str): The name of the pipeline.

    Returns:
    - str: The URL for the pipeline's Delta Live Tables tab.
    """
    # Initialize Databricks Workspace Client
    client = WorkspaceClient()
    
    # Search for the pipeline by name
    pipelines = client.pipelines.list_pipelines()
    pipeline_id = None
    
    for pipeline in pipelines:
        if pipeline.name == pipeline_name:
            pipeline_id = pipeline.pipeline_id
            break
    
    if not pipeline_id:
        raise ValueError(f"Pipeline with name '{pipeline_name}' not found.")
    
    # Construct the pipeline URL
    pipeline_url = f"https://{workspace_url}/pipelines/{pipeline_id}"
    return pipeline_url

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

import numpy as np
np.set_printoptions(precision=2)

import logging
logging.getLogger("tensorflow").setLevel(logging.ERROR)

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# The location that the DLT databases should be written to
setattr(DA, 'paths.storage_location', f'{DA.paths.working_dir}/storage_location')
DA.dlt_data_factory = DataFactory()

# COMMAND ----------

DA.dlt_data_factory.load()
