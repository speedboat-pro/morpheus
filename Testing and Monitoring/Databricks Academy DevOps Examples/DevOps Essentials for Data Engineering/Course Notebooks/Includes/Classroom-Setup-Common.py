# Databricks notebook source
# MAGIC %run ../../../Includes/_common

# COMMAND ----------

@DBAcademyHelper.add_method
def create_DA_keys(self): 
    '''
    Create the DA references to the dev, prod and stage catalogs for the user.
    '''
    print('Set DA dynamic references to the dev, stage and prod catalogs.\n')
    setattr(DA, f'catalog', f'{self.catalog_name}')
    setattr(DA, f'catalog_dev', f'{self.catalog_name}_1_dev')
    setattr(DA, f'catalog_stage', f'{self.catalog_name}_2_stage')
    setattr(DA, f'catalog_prod', f'{self.catalog_name}_3_prod')

# COMMAND ----------

@DBAcademyHelper.add_method
def create_volumes(self, in_catalog: str, in_schema: str, vol_names: list):
    """
    Creates the listed volumes in the specified schema within the user's catalog.
    
    This function will check if the volumes already exists. If they don't exist the volumes will be created and a note returned.
    
    Args:
    -------
        - in_catalog (str): The catalog to create the volume in.
        - in_schema (str): The schema to create the volumes in.
        - vol_names (list): A list of strings representing the volumes to create. If one volume, create a list of a single volume.
    
    Returns:
    -------
        Note in the log on the action(s) performed.
  
    Example:
    -------
        create_volumes(in_catalog=DA.catalog_dev_1, in_schema='default', vol_names=['health'])
    """
    ## Store current volumes in a list
    get_current_volumes = spark.sql(f'SHOW VOLUMES IN {in_catalog}.{in_schema}')
    current_volumes = (get_current_volumes
                       .select('volume_name')
                       .toPandas()['volume_name']
                       .tolist())

    ## Check to see if the volume(s) are created. If not, test each volume and create.
    if set(vol_names).issubset(current_volumes):
        print(f'Volume check. Volume {vol_names} already exist in {in_catalog}.{in_schema}. No action taken')

    for vol in vol_names:
        if vol not in current_volumes:
            spark.sql(f'CREATE VOLUME IF NOT EXISTS {in_catalog}.{in_schema}.{vol}')
            print(f'Created the volume: {in_catalog}.{in_schema}.{vol}.')

# COMMAND ----------

@DBAcademyHelper.add_method
def create_spark_data_frame_from_cdc(self, cdc_csv_file_path):
    '''
    Create the DataFrame used to create the CSV files for the course.
    '''
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import rand, when, lit, to_date, monotonically_increasing_id, col, coalesce
    from pyspark.sql.types import StringType
    import uuid

    ##
    ## Generate a column with a unique id (using it as a 'fake' PII column)
    ##

    # Define a UDF to generate deterministic UUID based on the row index
    def generate_deterministic_uuid(index):
        # Use UUID5 or UUID3 based on a namespace and the row index as the name
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(index)))  # You can use uuid3 as well

    # Register UDF
    generate_deterministic_uuid_udf = udf(generate_deterministic_uuid, StringType())

    ##
    ## Create spark dataframe with required columns and values to save CSV files.
    ##
    sdf = (spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cdc_csv_file_path)
        .repartition(1)
        .withColumn("ID", monotonically_increasing_id())
        .select("ID",
                generate_deterministic_uuid_udf(monotonically_increasing_id()).alias('PII'),
                when(col("ID") < 15121, "2025-01-01")
                    .when(col("ID") < 40351, "2025-01-02")
                    .otherwise("2025-01-03")
                    .alias("date"),
                when(col("HighChol") < .2, 0)  # when value is less than .2
                    .when((col("HighChol").cast('float') >= .2) & (col("HighChol") < 1.03), 1) # when value is between .2 and 1.03 
                    .otherwise(2)              # else when value is greater than or equal to 0.9
                    .alias("HighCholest"),
                "HighBP", 
                "BMI",
                "Age",
                "Education",
                "income"        
        )
    )

    return sdf

# COMMAND ----------

@DBAcademyHelper.add_method
def delete_source_files(self, source_files):
        """
        Deletes all files in the specified source volume.

        This function iterates through all the files in the given volume,
        deletes them, and prints the name of each file being deleted.

        Parameters:
        ----------
        source_files : str, optional
            The path to the volume containing the files to delete. 
            Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
                Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

        Returns:
        -------
        None
            This function does not return any value. It performs file deletion as a side effect and prints all files that it deletes.

        Example:
        --------
        delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
        """
        
        import os
        
        print(f'\nSearching for files in {source_files} volume to delete prior to creating files...')
        if os.path.exists(source_files):
            list_of_files = sorted(os.listdir(source_files))
        else:
            list_of_files = None

        if not list_of_files:  # Checks if the list is empty.
            print(f"No files found in {source_files}.\n")
        else:
            for file in list_of_files:
                file_to_delete = source_files + '/' + file
                print(f'Deleting file: {file_to_delete}')
                dbutils.fs.rm(file_to_delete)

# COMMAND ----------

import os

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
                 channel: str = 'PREVIEW',
                 development: bool = True,
                 pipeline_type: str = 'WORKSPACE'):

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

        # Instantiate the WorkspaceClient to communicate with Databricks REST API
        self.workspace = WorkspaceClient()
        self.pipeline_body = {}

    def _check_pipeline_exists(self):
        """
        Checks if a pipeline with the same name already exists.
        Raises:
            ValueError if the pipeline already exists.
        """
        for pipeline in self.workspace.pipelines.list_pipelines():
            if pipeline.name == self.pipeline_name:
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

        # Source folder is not in current working directory, so go up two level (Only for this course)
        main_course_folder = os.path.dirname(os.path.dirname(cwd))

        # Create full path to root folder
        root_path_folder = os.path.join('/', main_course_folder, self.root_path_folder_name)

        # Convert source folder names into glob pattern paths for the DLT pipeline
        source_paths = [os.path.join(main_course_folder, folder) for folder in self.source_folder_names]
        libraries = [{'glob': {'include': path}} for path in source_paths]

        # Build dictionary to be sent in the API request
        self.pipeline_body = {
            'name': self.pipeline_name,
            'pipeline_type': self.pipeline_type,
            'root_path': root_path_folder,
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
        print(f"Creating the Lakeflow Declarative Pipeline '{self.pipeline_name}'...")
        print(f"Root folder path: {self.pipeline_body['root_path']}")
        print(f"Source folder path(s): {self.pipeline_body['libraries']}")

        # Make the API call
        self.response = self.workspace.api_client.do('POST', '/api/2.0/pipelines', body=self.pipeline_body)

        # Notify of completion
        print(f"\nLakeflow Declarative Pipeline Creation '{self.pipeline_name}' Complete!")

        return self.response

    def get_pipeline_id(self):
        """
        Returns the ID of the created pipeline.
        """
        if not hasattr(self, 'response'):
            raise RuntimeError("Pipeline has not been created yet. Call create_pipeline() first.")

        return self.response.get("pipeline_id")

    def start_pipeline(self):
        '''
        Starts the pipeline using the attribute set from the generate_pipeline() method.
        '''
        print('Started the pipeline run. Navigate to Jobs and Pipelines to view the pipeline.')
        self.workspace.pipelines.start_update(self.get_pipeline_id())

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()
