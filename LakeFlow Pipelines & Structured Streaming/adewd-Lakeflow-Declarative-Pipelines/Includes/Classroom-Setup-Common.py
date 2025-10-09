# Databricks notebook source
# MAGIC %run ../../../Includes/_common

# COMMAND ----------

from pyspark.sql.types import StructType
DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

class StreamFactory:
    """
    Incrementally loads data from a source dataset to a target directory.

    Attributes:
        source_dir: source path for datasets
        target_dir: landing path for streams
        max_batch: total number of batches before exhausting stream
        batch: counter used to track current batch number

    Methods:
        load(continuous=False)
        load_batch(target, batch, end): dataset-specific function provided at instantiation
    """
    def __init__(self, source_dir, target_dir, load_batch, max_batch):
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.load_batch = load_batch
        self.max_batch = max_batch
        self.batch = 1     
        
    def load(self, continuous=False):
        
        if self.batch > self.max_batch:
            print("Data source exhausted", end="...")
            total = 0                
        elif continuous == True:
            print(f"Loading all batches to the stream", end="...")
            total = self.load_batch(self.source_dir, self.target_dir, self.batch, self.max_batch)
            self.batch = self.max_batch + 1
        else:
            print(f"Loading batch #{self.batch} to the stream", end="...")
            total = self.load_batch(self.source_dir, self.target_dir, self.batch, self.batch)
            self.batch = self.batch + 1
            
        print(f"Loaded {total:,} records")

None

# COMMAND ----------

@DBAcademyHelper.add_method
def load_user_reg_batch(self,datasets_dir, target_dir, batch_start, batch_end):

    source_dataset = f"{datasets_dir}/user-reg"
    target_path = f"{target_dir}/user_reg"

    df = (spark.read
          .format("json")
          .schema("device_id long, mac_address string, registration_timestamp double, user_id long")
          .load(source_dataset)
          .withColumn("date", F.col("registration_timestamp").cast("timestamp").cast("date"))
          .withColumn("batch", F.when(F.col("date") < "2019-12-01", F.lit(1)).otherwise(F.dayofmonth(F.col("date"))+1))
          .filter(f"batch >= {batch_start}")
          .filter(f"batch <= {batch_end}")          
          .drop("date", "batch")
          .cache())

    df.write.mode("append").format("json").save(target_path)
    return df.count()        

@DBAcademyHelper.add_method
def load_cdc_batch(self,datasets_dir, target_dir, batch_start, batch_end):
    
    source_dataset = f"{datasets_dir}/pii/raw"
    target_path = f"{target_dir}/cdc"

    df = (spark.read
      .load(source_dataset)
      .filter(f"batch >= {batch_start}")
      .filter(f"batch <= {batch_end}")
    )   
    df.write.mode("append").format("json").save(target_path)
    return df.count()

@DBAcademyHelper.add_method
def load_daily_batch(datasets_dir, target_dir, batch_start, batch_end):
    
    source_path = f"{datasets_dir}/bronze"
    target_path = f"{target_dir}/daily"


    df = (spark.read
      .load(source_path)
      .withColumn("day", 
        F.when(F.col("date") <= '2019-12-01', 1)
        .otherwise(F.dayofmonth("date")))
      .filter(F.col("day") >= batch_start)
      .filter(F.col("day") <= batch_end)
      .drop("date", "week_part", "day")  
    )
    df.write.mode("append").format("json").save(target_path)
    return df.count()

None

# COMMAND ----------

@DBAcademyHelper.add_method
def clone_table(self,table_name, source_path =None, source_name=None):
    """
    Clones a Delta table using a shallow clone.

    Args:
        table_name (str): Name of the target table to create.
        source_path (str): Path to the source Delta table.
        source_name (str, optional): Specific name of the source table within the source path. Defaults to the target table name.

    Returns:
        None
    """
    from time import perf_counter

    start_time = perf_counter()

    # Use the table name as the source name if not provided
    if source_name is None:
        source_name = table_name

    if source_path is None:
        source_path = DA.paths.datasets.gym

    table_exists = spark.catalog.tableExists(f"dbacademy.{DA.schema_name}.date_lookup_clone")
    
    if table_exists == False:
        print(f"Cloning table '{table_name}' from '/Volumes/dbacademy_gym/v01/{source_name}'...")

        # Execute the shallow clone SQL command
        spark.sql(f"""
            CREATE OR REPLACE TABLE {table_name}
            CLONE delta.`{source_path}/{source_name}`
        """)

        elapsed_time = perf_counter() - start_time
        print(f"Table '{table_name}' cloned successfully in {elapsed_time:.2f} seconds.")
    else:
        print(f"Table 'dbacademy.{DA.schema_name}.{table_name}' already exists.")

# COMMAND ----------

@DBAcademyHelper.add_method
def generate_pipeline_name(self):
    return DA.schema_name.replace('-','_') + "_pipeline"

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

class DeclarativePipelineCreator:
    """
    A class to create a Lakeflow Declarative DLT pipeline using the Databricks REST API.

    Attributes:
    -----------
    pipeline_name : str
        Name of the pipeline to be created.
    root_path_folder_name : str
        The folder containing the pipeline code relative to current working directory.
    source_folder_names : list
        List of subfolders inside the root path containing source notebooks or scripts.
    catalog_name : str
        The catalog where the pipeline tables will be stored.
    schema_name : str
        The schema (aka database) under the catalog.
    serverless : bool
        Whether to use serverless compute.
    configuration : dict
        Optional key-value configurations passed to the pipeline.
    continuous : bool
        If True, enables continuous mode (streaming).
    photon : bool
        Whether to use Photon execution engine.
    channel : str
        The DLT release channel to use (e.g., "PREVIEW", "CURRENT").
    development : bool
        Whether to run the pipeline in development mode.
    pipeline_type : str
        Type of pipeline (e.g., 'WORKSPACE').
    delete_pipeine_if_exists : bool
        Delete the pipeline if it exists if True. Otherwise return an error (False).
    """

    def __init__(self,
                 pipeline_name: str,
                 root_path_folder_name: str,
                 catalog_name: str,
                 schema_name: str,
                 source_folder_names: list = None,
                 serverless: bool = True,
                 configuration: dict = None,
                 continuous: bool = False,
                 photon: bool = True,
                 channel: str = 'CURRENT',
                 development: bool = True,
                 pipeline_type: str = 'WORKSPACE',
                 delete_pipeine_if_exists = False):
        
        # Assign all input arguments to instance attributes
        self.pipeline_name = pipeline_name
        self.root_path_folder_name = root_path_folder_name
        self.source_folder_names = source_folder_names or []
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.serverless = serverless
        self.configuration = configuration or {}
        self.continuous = continuous
        self.photon = photon
        self.channel = channel
        self.development = development
        self.pipeline_type = pipeline_type
        self.delete_pipeine_if_exists = delete_pipeine_if_exists

        # Instantiate the WorkspaceClient to communicate with Databricks REST API
        self.workspace = WorkspaceClient()
        self.pipeline_body = {}

    def _check_pipeline_exists(self):
        """
        Checks if a pipeline with the same name already exists. If specified, will delete the existing pipeline.
        Raises:
            ValueError if the pipeline already exists if delete_pipeine_if_exists is set to False. Otherwise deletes pipeline.
        """
        ## Get a list of pipeline names
        list_of_pipelines = self.workspace.pipelines.list_pipelines()

        ## Check to see if the pipeline name already exists. Depending on the value of delete_pipeine_if_exists, either raise an error or delete the pipeline.
        for pipeline in list_of_pipelines:
            if pipeline.name == self.pipeline_name:
                if self.delete_pipeine_if_exists == True:
                    print(f'Pipeline with that name already exists. Deleting the pipeline {self.pipeline_name} and then recreating it.\n')
                    self.workspace.pipelines.delete(pipeline.pipeline_id)
                else:
                    raise ValueError(
                        f"Lakeflow Declarative Pipeline name '{self.pipeline_name}' already exists. "
                        "Please delete the pipeline using the UI and rerun to recreate."
                    )

    def _build_pipeline_body(self):
        """
        Constructs the body of the pipeline creation request based on class attributes.
        """
        # Get current working directory
        cwd = os.getcwd()

        # Create full path to root folder
        root_path = os.path.join('/', cwd, self.root_path_folder_name)

        # Convert source folder names into glob pattern paths for the DLT pipeline
        # source_paths = [os.path.join(root_path, folder, '**') for folder in self.source_folder_names]
        source_paths = [os.path.join(root_path, folder) for folder in self.source_folder_names]
        libraries = [{'glob': {'include': path}} for path in source_paths]

        # Build dictionary to be sent in the API request
        self.pipeline_body = {
            'name': self.pipeline_name,
            'pipeline_type': self.pipeline_type,
            'root_path': root_path,
            'libraries': libraries,
            'catalog': self.catalog_name,
            'schema': self.schema_name,
            'serverless': self.serverless,
            'configuration': self.configuration,
            'continuous': self.continuous,
            'photon': self.photon,
            'channel': self.channel,
            'development': self.development
        }

    def get_pipeline_id(self):
        """
        Returns the ID of the created pipeline.
        """
        if not hasattr(self, 'response'):
            raise RuntimeError("Pipeline has not been created yet. Call create_pipeline() first.")

        return self.response.get("pipeline_id")


    def create_pipeline(self):
        """
        Creates the pipeline on Databricks using the defined attributes.
        
        Returns:
            dict: The response from the Databricks API after creating the pipeline.
        """
        # Check for name conflicts
        self._check_pipeline_exists()

        # Build the body of the API request and creates self.pipeline_body variable
        self._build_pipeline_body()

        # Display information to user
        print(f"Creating the Lakeflow Declarative Pipeline '{self.pipeline_name}'.\n")
        print(f"Using root folder path: {self.pipeline_body['root_path']}\n")
        print(f"Using source folder path(s): {self.pipeline_body['libraries']}\n")

        # Make the API call
        self.response = self.workspace.api_client.do('POST', '/api/2.0/pipelines', body=self.pipeline_body)

        # Notify of completion
        print(f"Lakeflow Declarative Pipeline Creation '{self.pipeline_name}' Complete!")

        return self.response


    def start_pipeline(self):
        '''
        Starts the pipeline using the attribute set from the generate_pipeline() method.
        '''
        print('Started the pipeline run. Navigate to Jobs and Pipelines to view the pipeline.')
        self.workspace.pipelines.start_update(self.get_pipeline_id())

# COMMAND ----------

def delete_source_files(source_files: str):
    """
    Deletes all files in the specified source volume.

    This function iterates through all the files in the given volume,
    deletes them, and prints the name of each file being deleted.

    Parameters:
    - source_files : str
        The path to the volume containing the files to delete. 
        Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
            Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

    Returns:
    - None. This function does not return any value. It performs file deletion and prints all files that it deletes. If no files are found it prints in the output.

    Example:
    - delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
    """

    import os

    print(f"\nSearching for files in the volume: '{source_files}'.")
    if os.path.exists(source_files):
        list_of_files = sorted(os.listdir(source_files))
    else:
        list_of_files = None

    if not list_of_files:  # Checks if the list is empty.
        print(f"No files found in the volume: '{source_files}'.\n")
    else:
        print(f"Deleting all files in the volume: '{source_files}'.\n")
        for file in list_of_files:
            file_to_delete = source_files + file
            dbutils.fs.rm(file_to_delete)

# COMMAND ----------

# @DBAcademyHelper.add_method
# def generate_pipeline(self,
#                       pipeline_name, 
#                       notebooks_folder, 
#                       pipeline_notebooks,
#                       use_schema,
#                       use_configuration = None,
#                       use_serverless = True,
#                       use_continuous = False):
#     """
#     Generates a Databricks pipeline based on the specified configuration parameters.

#     This method creates a pipeline that can execute a series of notebooks in a serverless environment, 
#     and allows the option to use continuous runs if needed. It relies on Databricks SDK to interact with Databricks services.

#     By default will use the dbacademy catalog, within the user's specific schema.

#     Parameters:
#     - pipeline_name (str): The name of the pipeline to be created.
#     - notebooks_folder (str): The folder within Databricks where the notebooks are stored. This should be the folder name one level above where the Classroom-Setup-Common notebooks lives.
#     - pipeline_notebooks (list of str): List of notebook paths that should be included in the pipeline. Use path from after the notebooks_folder.
#     - use_configuration (dict or None): Optional configuration dictionary that can be used to customize the pipeline's settings. 
#         - Default is None.
#     - use_serverless (bool): Flag indicating whether to use the serverless environment for the pipeline. 
#         - Defaults to True.
#     - use_continuous (bool): Flag indicating whether to set up continuous execution for the pipeline. 
#         - Defaults to False.

#     Returns:
#     - pipeline (object): A Databricks pipeline object created using the specified parameters.
#     - Stores the pipeline_id in the self.current_pipeline_id attribute.

#     Raises:
#     - Raises an error if the pipeline name already exists.

#     Example usage:
#             DA.generate_pipeline(
#                 pipeline_name=f"DEV",            ## creates a pipeline catalogname_DEV
#                 use_schema = 'default',          ## uses schema within user's catalog
#                 notebooks_folder='Pipeline 01', 
#                 pipeline_notebooks=[            ## Uses Pipeline 01/bronze/dev/ingest_subset
#                     'bronze/dev/ingest_subset',
#                     'silver/quarantine'
#                     ]
#                 )
    
#     Notes:
#     - The method imports the necessary Databricks SDK service for pipelines.
#     - The 'use_catalog' and 'use_schema' attributes are assumed to be part of the class, and are used to define catalog and schema name using the customer DA object attributes.
#     """
#     import os
#     from databricks.sdk.service import pipelines
    
#     ## Set pipeline name and target catalog
#     pipeline_name = f"{pipeline_name}" 
#     use_catalog = f"{self.catalog_name}"
    
#     ## Check for duplicate name. Return error if name already found.
#     for pipeline in self.workspace.pipelines.list_pipelines():
#       if pipeline.name == pipeline_name:
#         assert_false = False
#         assert assert_false, f'You already have pipeline named {pipeline_name}. Please go to the Pipelines under Data Engineering section, and manually delete the pipeline. Then rerun this program to create the pipeline.'

#     ## Get path of includes folder
#     current_folder_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)

#     ## Go back one folder to the main course folder then navigate to the folder specified by the notebooks_folder variable
#     main_course_folder_path = "/Workspace" + "/".join(current_folder_path.split("/")[:-1]) + '/' + notebooks_folder

#     ## Create paths for each notebook specified in method argument notebooks(list of notebooks to use)
#     notebooks_paths = []
#     for i, notebook in enumerate(pipeline_notebooks):
#         current_notebook_path = (f'Notebook #{i + 1}', main_course_folder_path + '/' + notebook)
        
#         # Attempt to list the contents of the path. If the path does not exist return an error.
#         if os.path.exists(current_notebook_path[1]):
#             pass
#         else:
#             assert_false = False
#             assert assert_false, f'The notebook path you specified does not exists {current_notebook_path[1]}. Please specify a correct path in the generate_pipeline() method using the notebooks_folder and pipeline_notebooks arguments. Read the method documentation for more information.'
        
#         notebooks_paths.append(current_notebook_path)


#     ## Create pipeline
#     pipeline_info = self.workspace.pipelines.create(
#         allow_duplicate_names=True,
#         name=pipeline_name,
#         catalog=use_catalog,
#         target=use_schema,
#         serverless=use_serverless,
#         continuous=use_continuous,
#         configuration=use_configuration,
#         libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook)) for i, notebook in notebooks_paths]
#     )

#     ## Store pipeline ID
#     self.current_pipeline_id = pipeline_info.pipeline_id 

#     ## Success message
#     print(f"Created the DLT pipeline {pipeline_name} using the settings from below:\n")

#     ## Use the display_config_values function to display the following values as HTML output.
#     ## Will list the Job Name and notebook paths.
#     self.display_config_values([
#             ('DLT Pipeline Name', pipeline_name),
#             ('Using Catalog', self.catalog_name),
#             ('Using Schema', use_schema),
#             ('Compute', 'Serverless' if use_serverless else 'Error in setting Compute')
#         ] + notebooks_paths)
    

# @DBAcademyHelper.add_method
# def start_pipeline(self):
#     '''
#     Starts the pipeline using the attribute set from the generate_pipeline() method.
#     '''
#     print('Started the pipeline run. Navigate to Jobs and Pipelines to view the pipeline.')
#     self.workspace.pipelines.start_update(self.current_pipeline_id)


# # # Example METHOD
# # DA.generate_pipeline(
# #     pipeline_name=f"DEV1", 
# #     use_schema = 'default',
# #     notebooks_folder='Pipeline 01', 
# #     pipeline_notebooks=[
# #         'bronze/dev/ingest_subset',
# #         'silver/quarantine'
# #         ]
# #     )

# COMMAND ----------

@DBAcademyHelper.add_method
def delete_all_pipelines(self):
    """
    Deletes all Delta Live Tables (DLT) pipelines in the workspace.

    Parameters:
    - None

    Returns:
    - None
    """
    from databricks.sdk.service import pipelines

    # List all pipelines
    all_pipelines = self.workspace.pipelines.list_pipelines()

    # Delete each pipeline
    for pipeline in all_pipelines:
        self.workspace.pipelines.delete(pipeline_id=pipeline.pipeline_id)
