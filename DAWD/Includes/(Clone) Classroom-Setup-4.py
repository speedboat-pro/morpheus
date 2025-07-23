# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# The DataFactory is just a pattern to demonstrate a fake stream is more of a function
# streaming workloads than it is of a pipeline - this pipeline happens to stream data.
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
def print_pipeline_job_info(self):
    """
    Returns the name of the job for the user to use. Unique schema name +: Data Transformation Pipeline.
    """
    unique_name = self.unique_name(sep="-")
    pipeline_name = f"{unique_name}"
    
    pipeline_name += ": Data Transformation Pipeline"
   
    pipeline_name = pipeline_name.replace('-','_')
    print(f"{pipeline_name}")
    return pipeline_name

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

@DBAcademyHelper.add_method
def get_pipeline_config(self, language):
    """
    Returns the configuration to be used by the student in configuring the pipeline.
    """
    base_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    base_path = "/".join(base_path.split("/")[:-1])
    
    pipeline_name = self.print_pipeline_job_info()

    if language is None: language = dbutils.widgets.getArgument("pipeline-language", None)
    assert language in ["SQL", "Python"], f"A valid language must be specified, found {language}"
    
    return PipelineConfig(pipeline_name, self.paths.stream_source, [
        f"{base_path}/notebooks/DLT Pipeline"
    ])

# COMMAND ----------

@DBAcademyHelper.add_method
def generate_pipeline_name(self):
    return DA.schema_name.replace('-','_') + ": workflow"

# COMMAND ----------

@DBAcademyHelper.add_method
def generate_pipeline(self,
                      pipeline_name, 
                      notebooks_folder, 
                      pipeline_notebooks,
                      use_schema,
                      use_configuration=None,
                      use_serverless=True,
                      use_continuous=False):
    """
    Generates a Databricks pipeline based on the specified configuration parameters.

    Parameters:
    - pipeline_name (str): The name of the pipeline to be created.
    - notebooks_folder (str): The folder containing the notebooks for the pipeline.
    - pipeline_notebooks (list of str): List of notebook paths to include in the pipeline.
    - use_schema (str): Schema name for the pipeline target.
    - use_configuration (dict or None): Optional configuration dictionary for custom settings.
    - use_serverless (bool): Use serverless environment for the pipeline (default=True).
    - use_continuous (bool): Enable continuous execution for the pipeline (default=False).

    Returns:
    - None: The pipeline ID is stored in `self.current_pipeline_id` for reference.
    """

    import os
    from databricks.sdk.service import pipelines

    # Set pipeline name and target catalog
    pipeline_name = f"{pipeline_name}" 
    use_catalog = f"{self.catalog_name}"

    # Check for duplicate pipeline names
    for pipeline in self.workspace.pipelines.list_pipelines():
        if pipeline.name == pipeline_name:
            raise ValueError(
                f"A pipeline with the name '{pipeline_name}' already exists. "
                f"Please delete the existing pipeline manually before proceeding."
            )

    # Get the current notebook folder path
    current_folder_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    main_course_folder_path = "/Workspace" + "/".join(current_folder_path.split("/")[:-1]) 
    # Create paths for the specified notebooks
    notebooks_paths = []
    for i, notebook in enumerate(pipeline_notebooks):
        notebook_path = f"{main_course_folder_path}/{notebook}"
        if not os.path.exists(notebook_path):
            raise FileNotFoundError(f"The specified notebook path '{notebook_path}' does not exist.")
        notebooks_paths.append(notebook_path)

    # Create the pipeline in development mode
    pipeline_info = self.workspace.pipelines.create(
        allow_duplicate_names=True,
        name=pipeline_name,
        catalog=use_catalog,
        target=use_schema,
        serverless=use_serverless,
        continuous=use_continuous,
        development=True,  # Ensure the pipeline is created in development mode
        configuration=use_configuration,
        libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=path)) for path in notebooks_paths]
    )

    # Store the pipeline ID
    self.current_pipeline_id = pipeline_info.pipeline_id

    # Success message
    print(f"Successfully created the DLT pipeline '{pipeline_name}' in development mode.")

    # Display configuration details
    self.display_config_values([
        ('Pipeline Name', pipeline_name),
        ('Using Catalog', self.catalog_name),
        ('Using Schema', use_schema),
        ('Compute Type', 'Serverless' if use_serverless else 'Standard'),
    ] + [(f"Notebook #{i+1}", path) for i, path in enumerate(notebooks_paths)])


@DBAcademyHelper.add_method
def start_pipeline(self):
    """
    Starts the pipeline execution using the pipeline ID generated during creation.
    """
    if not self.current_pipeline_id:
        raise ValueError("Pipeline ID is not set. Please create a pipeline first.")
    
    print(f"Starting the pipeline '{self.current_pipeline_id}' in development mode. Navigate to Delta Live Tables to view its progress.")
    self.workspace.pipelines.start_update(self.current_pipeline_id)

# COMMAND ----------

class PipelineConfig():
    def __init__(self, pipeline_name, source, notebooks):
        self.pipeline_name = pipeline_name # The name of the pipeline
        self.source = source               # Custom Property
        self.notebooks = notebooks         # This list of notebooks for this pipeline
    
    def __repr__(self):
        content = f"Name:      {self.pipeline_name}\nSource:    {self.source}\n"""
        content += f"Notebooks: {self.notebooks.pop(0)}"
        for notebook in self.notebooks: content += f"\n           {notebook}"
        return content

# COMMAND ----------

@DBAcademyHelper.add_method
def monitor_pipeline_status(self):
    """
    Monitors the status of the Delta Live Tables pipeline.
    Waits for terminal states and stops immediately when reached.
    Prints 'Successful' for COMPLETED and 'Failed' for FAILED or CANCELED.
    """
    import time

    if not self.current_pipeline_id:
        raise ValueError("Pipeline ID is not set. Please create and start a pipeline first.")

    # Retrieve updates for the pipeline
    updates = self.workspace.pipelines.list_updates(self.current_pipeline_id).updates

    if not updates:
        raise Exception(f"No updates found for pipeline ID: {self.current_pipeline_id}. Please check the DLT pipeline status in the UI.")

    # Get the most recent update
    latest_update = updates[0]
    active_update = latest_update.update_id
    raw_status = latest_update.state.value

    print(f"Monitoring pipeline '{self.current_pipeline_id}'. Latest update ID: '{active_update}'.")

    # Monitor the pipeline status until it reaches a terminal state
    while True:
        raw_status = self.workspace.pipelines.get_update(self.current_pipeline_id, active_update).update.state.value
        status = raw_status.split(".")[-1]  # Extract the actual status value (e.g., COMPLETED)

        # Print the status
        print(f"Pipeline Status: {raw_status}.")

        # Check for terminal states and stop monitoring
        if status == "COMPLETED":
            print("Successful")
            break
        elif status in ["FAILED", "CANCELED"]:
            print("Failed")
            break

        # Wait for the next status check
        time.sleep(15)

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

# @DBAcademyHelper.add_method
# def create_pipeline(self, language):
#     "Provided by DBAcademy, this function creates the prescribed pipeline"
    
#     config = self.get_pipeline_config(language)

#     # Delete the existing pipeline if it exists
#     try:
#         self.workspace.pipelines.delete(
#             self.workspace_find(
#                 'pipelines',
#                 config.pipeline_name,
#                 api='list_pipelines'
#             ).pipeline_id
#         )
#     except NotFound:
#         pass

#     policy = self.get_dlt_policy()
#     if policy is None: cluster = [{"num_workers": 1}]
#     else:              cluster = [{"num_workers": 1, "policy_id": self.get_dlt_policy().get("policy_id")}]
    
#     # Create the new pipeline
#     self.pipeline_id = self.workspace.pipelines.create(
#         name=config.pipeline_name, 
#         development=True,
#         catalog=self.catalog_name,
#         target=self.schema_name,
#         notebooks=config.notebooks,
#         configuration = {
#             "source": config.source
#         },
#         clusters=cluster
#     ).pipeline_id

#     print(f"Created the pipeline \"{config.pipeline_name}\" ({self.pipeline_id})")

# COMMAND ----------

# @DBAcademyHelper.add_method
# def start_pipeline(self):
#     "Starts the pipeline and then blocks until it has completed, failed or was canceled"

#     import time

#     # Start the pipeline
#     update_id = self.workspace.pipelines.start_update(self.pipeline_id).update_id

#     # Get the status and block until it is done
#     state = self.workspace.pipelines.get_update(self.pipeline_id, update_id).update.state.value

#     duration = 15

#     while state not in ["COMPLETED", "FAILED", "CANCELED"]:
#         print(f"Current state is {state}, sleeping {duration} seconds.")    
#         time.sleep(duration)
#         state = self.workspace.pipelines.get_update(self.pipeline_id, update_id).update.state.value
    
#     print(f"The final state is {state}.")    
#     assert state == "COMPLETED", f"Expected the state to be COMPLETED, found {state}"

# COMMAND ----------

from databricks.sdk.service import pipelines
import os

def modify_pipeline(DA, pipeline_name, updated_config):
    """
    Modifies an existing DLT pipeline with the provided configuration.
    
    Parameters:
    - DA: The DBAcademyHelper instance for accessing workspace details.
    - pipeline_name (str): The name of the pipeline to be modified.
    - updated_config (dict): A dictionary with updated pipeline configurations.

    Returns:
    - None: Updates the existing pipeline with new settings.
    """
    # Search for the pipeline by name
    existing_pipeline_id = None
    for pipeline in DA.workspace.pipelines.list_pipelines():
        if pipeline.name == pipeline_name:
            existing_pipeline_id = pipeline.pipeline_id
            break

    if not existing_pipeline_id:
        raise ValueError(f"Pipeline with name '{pipeline_name}' not found. Cannot modify a non-existent pipeline.")

    # Retrieve the existing pipeline details
    existing_pipeline = DA.workspace.pipelines.get(existing_pipeline_id)
    
    # Construct the paths for the updated notebooks
    if "notebooks" in updated_config:
        # Get the current notebook folder path
        current_folder_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
        main_course_folder_path = "/Workspace" + "/".join(current_folder_path.split("/")[:-1])

        # Create paths for the specified notebooks
        updated_notebook_paths = []
        for notebook in updated_config["notebooks"]:
            notebook_path = f"{main_course_folder_path}/{notebook}"
            if not os.path.exists(notebook_path):
                raise FileNotFoundError(f"The specified notebook path '{notebook_path}' does not exist.")
            updated_notebook_paths.append(
                pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook_path))
            )
    else:
        updated_notebook_paths = existing_pipeline.spec.libraries

    # Update specific configurations
    updated_config = {
        "name": pipeline_name,
        "catalog": updated_config.get("catalog", existing_pipeline.spec.catalog),
        "target": updated_config.get("target", existing_pipeline.spec.target),
        "configuration": updated_config.get("configuration", existing_pipeline.spec.configuration),
        "continuous": updated_config.get("continuous", existing_pipeline.spec.continuous),
        "development": updated_config.get("development", existing_pipeline.spec.development),
        "libraries": updated_notebook_paths
    }

    # Update the pipeline
    DA.workspace.pipelines.update(existing_pipeline_id, **updated_config)
    print(f"Pipeline '{pipeline_name}' has been successfully updated.")

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
