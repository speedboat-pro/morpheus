# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

# The DataFactory is just a pattern to demonstrate a fake stream is more of a function
# streaming workloads than it is of a pipeline - this pipeline happens to stream data.
class DataFactory:
    def __init__(self):
        spark.sql(f'CREATE VOLUME IF NOT EXISTS `{DA.catalog_name}`.`{DA.schema_name}`.`stream-source`')
        # Bind the stream-source to DA because we will use it again later.
        DA.paths.stream_source = f"/Volumes/{DA.catalog_name}/{DA.schema_name}/stream-source"
        
        self.source_dir = f"{DA.paths.datasets.retail}/retail-pipeline"
        self.target_dir = DA.paths.stream_source
        
        # Determine the smaller count between the two datasets to ensure consistency
        orders_count = len(dbutils.fs.ls(f"{self.source_dir}/orders/stream_json"))
        customers_count = len(dbutils.fs.ls(f"{self.source_dir}/customers/stream_json"))
        self.max_batch = min(orders_count, customers_count)
        
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
            self.copy_file("orders")
            self.copy_file("customers")
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
    for i, path in enumerate(config.notebook):
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
def validate_pipeline_config(self, pipeline_language):
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

    assert pipeline is not None, f"Could not find a pipeline named {config.pipeline_name}"
    assert pipeline.spec.catalog == DA.catalog_name, f"Default Catalog not set to {DA.catalog_name}"

    libraries = [l.notebook.path for l in pipeline.spec.libraries]
    
    def test_notebooks():
        if libraries is None: return False
        if len(libraries) != 1: return False
        return libraries[0] == config.notebook[0]
    
    assert test_notebooks(), "Notebooks are not properly configured"
    assert len(pipeline.spec.configuration) == 1, "Expected exactly one configuration parameter."
    assert pipeline.spec.configuration.get("source") == config.source, f"Expected the configuration parameter {config.source}"
    assert pipeline.spec.channel == "CURRENT", "Expected the channel to be set to Current."
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
        f"{base_path}/Pipelines/Customer Order Pipeline"
    ])

# COMMAND ----------

@DBAcademyHelper.add_method
def generate_pipeline_name(self):
    return DA.schema_name.replace('-','_') + ": Data Transformation Pipeline"

# COMMAND ----------

@DBAcademyHelper.add_method
def generate_pipeline(self,
                      pipeline_name, 
                      notebooks_folder, 
                      use_schema,
                      use_configuration = None,
                      use_serverless = True,
                      use_continuous = False):
    """
    Generates a Databricks pipeline based on the specified configuration parameters.

    This method creates a pipeline that can execute a series of notebooks in a serverless environment, 
    and allows the option to use continuous runs if needed. It relies on Databricks SDK to interact with Databricks services.

    By default will use the dbacademy catalog, within the user's specific schema.

    Parameters:
    - pipeline_name (str): The name of the pipeline to be created.
    - notebooks_folder (str): The folder within Databricks where the notebooks are stored. This should be the folder name one level above where the Classroom-Setup-Common notebooks lives.
    - use_configuration (dict or None): Optional configuration dictionary that can be used to customize the pipeline's settings. 
        - Default is None.
    - use_serverless (bool): Flag indicating whether to use the serverless environment for the pipeline. 
        - Defaults to True.
    - use_continuous (bool): Flag indicating whether to set up continuous execution for the pipeline. 
        - Defaults to False.

    Returns:
    - pipeline (object): A Databricks pipeline object created using the specified parameters.
    - Stores the pipeline_id in the self.current_pipeline_id attribute.

    Raises:
    - Raises an error if the pipeline name already exists.

    Example usage:
            DA.generate_pipeline(
                pipeline_name=f"DEV",            ## creates a pipeline catalogname_DEV
                use_schema = 'default',          ## uses schema within user's catalog
                notebooks_folder='Pipeline 01', 
                pipeline_notebooks=[            ## Uses Pipeline 01/bronze/dev/ingest_subset
                    'bronze/dev/ingest_subset',
                    'silver/quarantine'
                    ]
                )
    
    Notes:
    - The method imports the necessary Databricks SDK service for pipelines.
    - The 'use_catalog' and 'use_schema' attributes are assumed to be part of the class, and are used to define catalog and schema name using the customer DA object attributes.
    """
    import os
    from databricks.sdk.service import pipelines
    
    ## Set pipeline name and target catalog
    pipeline_name = f"{pipeline_name}" 
    use_catalog = f"{self.catalog_name}"
    
    ## Check for duplicate name. Return error if name already found.
    for pipeline in self.workspace.pipelines.list_pipelines():
      if pipeline.name == pipeline_name:
        assert_false = False
        assert assert_false, f'You already have pipeline named {pipeline_name}. Please go to the Delta Live Tables page and manually delete the pipeline. Then rerun this program to create the pipeline.'

    ## Get path of includes folder
    current_folder_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

    ## Go back one folder to the main course folder then navigate to the folder specified by the notebooks_folder variable
    main_course_folder_path = "/Workspace" + "/".join(current_folder_path.split("/")[:-1]) + '/' + notebooks_folder

    ## Create paths for each notebook specified in method argument notebooks(list of notebooks to use)
    notebooks_paths = []
    for i, notebook in enumerate(pipeline_notebooks):
        current_notebook_path = (f'Notebook #{i + 1}', main_course_folder_path + '/' + notebook)
    # Attempt to list the contents of the path. If the path does not exist return an error.
    if not os.path.exists(notebook_path):
        assert_false = False
        assert assert_false, f'The notebook path you specified does not exist {notebook_path}. Please specify a correct path in the generate_pipeline() method using the notebooks_folder argument. Read the method documentation for more information.'

    ## Create pipeline
    pipeline_info = self.workspace.pipelines.create(
        allow_duplicate_names=True,
        name=pipeline_name,
        catalog=use_catalog,
        target=use_schema,
        serverless=use_serverless,
        continuous=use_continuous,
        configuration=use_configuration,
        libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook_path))]
    )

    ## Store pipeline ID
    self.current_pipeline_id = pipeline_info.pipeline_id 

    ## Success message
    print(f"Created the DLT pipeline {pipeline_name} using the settings from below:\n")

    ## Use the display_config_values function to display the following values as HTML output.
    ## Will list the Job Name and notebook paths.
    self.display_config_values([
            ('DLT Pipeline Name', pipeline_name),
            ('Using Catalog', self.catalog_name),
            ('Using Schema', use_schema),
            ('Compute', 'Serverless' if use_serverless else 'Error in setting Compute'),
            ('Notebook Path', notebook_path)
        ])
    

@DBAcademyHelper.add_method
def start_pipeline(self):
    '''
    Starts the pipeline using the attribute set from the generate_pipeline() method.
    '''
    print('Started the pipeline run. Navigate to Delta Live Tables to view the pipeline.')
    self.workspace.pipelines.start_update(self.current_pipeline_id)


# # Example METHOD
# DA.generate_pipeline(
#     pipeline_name=f"DEV1", 
#     use_schema = 'default',
#     notebooks_folder='Pipeline 01'
#     )

# COMMAND ----------

class PipelineConfig():
    def __init__(self, pipeline_name, source, notebook):
        self.pipeline_name = pipeline_name # The name of the pipeline
        self.source = source               # Custom Property
        self.notebook = notebook           # The notebook for this pipeline
    
    def __repr__(self):
        content = f"Name:      {self.pipeline_name}\nSource:    {self.source}\n"
        content += f"Notebook:  {self.notebook}"
        return content

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
