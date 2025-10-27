# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

@DBAcademyHelper.add_method
def create_catalogs(self, catalog_suffix: list):
    '''
    Tests to see if catalogs are created for the course. 

    If the catalogs do not exist in the environment it will creates the catalogs based on the user's catalog name + given 
    suffix and assigns them as attributes to the DA object.

    Args:
    -------
        catalog_suffix (list): A list of strings representing suffixes to append 
                                to the base user catalog name to form the full catalog name.

    Returns:
    -------
        Log information:
            - If catalog(s) do not exist, prints information on the catalogs it created.
            - If catalog(s) exist, prints information that catalogs exist.

    Example:
    -------
        # Example usage
        catalog_suffixes = ['dev_1', 'prod_1', 'test_1']
        create_catalogs(catalog_suffixes)

        This will create the following catalogs:
        - catalog_dev_1
        - catalog_prod_1
        - catalog_test_1

        And the following attributes will be added to the DA object:
        - DA.catalog_dev_1 = 'catalog_dev_1'
        - DA.catalog_prod_1 = 'catalog_prod_1'
        - DA.catalog_test_1 = 'catalog_test_1'
    '''

    ## Current catalogs
    list_of_curr_catalogs = [catalog.name for catalog in spark.catalog.listCatalogs()]

    ## Create catalog for the user for the course.
    catalog_start = f'{DA.catalog_name}'
    create_catalogs = [catalog_start + suffix for suffix in catalog_suffix]

    ## Check if the required user catalogs exist in the workspace. If not create catalogs.
    if set(create_catalogs).issubset(set(list_of_curr_catalogs)):
        print('Required catalogs check. Ready for takeoff.\n')
    else:
        ## Create catalog if not already exist and print response
        for catalog_name in create_catalogs:
            if catalog_name not in list_of_curr_catalogs:
                create_catalog = spark.sql(f'CREATE CATALOG IF NOT EXISTS {catalog_name}')
                print(f'Created catalog: {catalog_name}.')

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
def create_course_csv_files(self,
                            dataframe, 
                            prod_volume_write_path: str, 
                            stage_volume_write_path: str,
                            dev_volume_write_path: str,
                            main_volume_write_path: str,
                            data_filter_conditions: list, 
                            data_name_append: str, 
                            del_files_first: bool = True):
    '''
    This method will first delete all files (by default) in the specified prod, stage and dev volume paths.

    Then it will create the CSV files for the course in the following volumes:
        - user user_catalog/volume/health/ 1 csv file with 15% of the rows
        - user dev/volume/health/ 1 csv file with 15% of the rows
        - user stage volume/volume/health/ 1 csv file with 40% of the rows
        - user prod volume/volume/health/ 3 csv files all files
    '''
    from pyspark.sql.functions import rand, when, lit, to_date, monotonically_increasing_id, col, coalesce

    print('\n----Creating the CSV files in the necessary volumes for the course----\n')

    ##
    ## Delete the files in the volume prior to creating the CSV files.
    ##
    if del_files_first == True:
        print('---DELETE FILES IN THE HEALTH VOLUME WITHIN THE MAIN, DEV, STAGE, PROD CATALOGS----')
        DA.delete_source_files(source_files = prod_volume_write_path)
        DA.delete_source_files(source_files = stage_volume_write_path)
        DA.delete_source_files(source_files = dev_volume_write_path)
        DA.delete_source_files(source_files = main_volume_write_path)


    ## Utility method to delete files that begin with an underscore when creating files in a volume from a spark data frame.
    def util_del_files_with_underscore_in_volume(self, volume_path: str):
        """
        Deletes files in the specified volume (directory) whose names start with an underscore ('_').

        This method lists all files in the given volume directory, filters out the ones that start 
        with an underscore, and deletes them using the `dbutils.fs.rm()` function cleaning up a 
        volume when creating files from a spark data frame.

        Parameters:
        ----------
            volume_path (str): The path to the volume (directory) where the files are located.

        Returns:
        -------
            Output information in the log.
        
        Example:
        --------
        >>> del_files_with_underscore_in_volume("/Volumes/inquisitive_layer_stage_2/default/health")
        """ 
        
        ## List files in specified volume
        files = dbutils.fs.ls(volume_path)

        # Filter files that start with an underscore '_'
        files_to_remove = [file.path for file in files if file.name.startswith('_')]
        
        ## If no files found, pass. Otherwise delete files.
        if len(files_to_remove) == 0:
            pass
        else: 
            print(f'Deleting files with underscores in: {volume_path}:')
            for file_path in files_to_remove:
                dbutils.fs.rm(file_path)


    ##
    ## Utility method to renameCSV files based on filter conditions in the PROD catalog volume
    ##
    def util_rename_csv_file(self, volume_path: str, file_prefix: str, file_suffix: str):
        """
        Renames CSV files in a specified volume path that start with 'part-' by appending 
        a date filter condition and a custom name suffix.

        `<date_filter_condition>_<data_name_append>.csv`. The renaming is performed using the `os.rename` method.

        Parameters:
        ----------
        volume_path (str): The directory path where the CSV files are located.
        date_filter_condition (str): A date or condition to be added at the beginning of the new file name.
        data_name_append (str): A suffix to append to the new file name before the `.csv` extension.

         Returns:
        -------
            Output information in the log.

        Example:
        --------
        >>> rename_csv_file('/Volumes/inquisitive_layer_stage_2/default/health', '2025-01-01', 'sales_data')
        """
        import os

        output_files = os.listdir(volume_path)

        for file in output_files:
            if file.startswith('part-'):
                file_to_rename = f'{volume_path}/{file}'
                new_csv_file_name = f'{file_prefix}_{file_suffix}.csv'
                rename_file_to = f'{volume_path}/{new_csv_file_name}'  

                print(f'Renaming CSV file: {file_to_rename}')
                print(f'New CSV file name: {rename_file_to}')
                os.rename(file_to_rename, rename_file_to)
                print(f'Created CSV file: {new_csv_file_name}')
        print('\n')


    ###
    ### Create the prod csv files based on filter
    ###
    def create_prod_csv_files(self):
        """
        Creates and writes CSV files for each filter condition in the provided list of filter conditions. All arguments
        are obtained from the main outer method.

        This function takes a DataFrame, filters it by each date condition in the `data_filter_conditions` list, 
        writes each filtered DataFrame as a single CSV file to the specified directory (`prod_volume_write_path`).
        Each CSV file is written with headers and appended in case the directory already contains data.

        Parameters (obtained from outer/main method):
        ----------
            - data_filter_conditions (list): A list of date filter conditions to filter the DataFrame by.
            - dataframe (DataFrame): The Spark DataFrame to be filtered and written to CSV.
            - prod_volume_write_path (str): The destination path where the CSV files will be written.

        Returns:
        -------
            Prints output in the log.

        Example:
        >>> create_prod_csv_files()
        """
        print('-------------------------------------------------------')
        print(f'--- Creating CSV files in PROD: {prod_volume_write_path}')
        print('-------------------------------------------------------')
        for filter in data_filter_conditions:
            (dataframe
             .filter(col("date") == filter)
             .repartition(1)   ## One CSV file per date
             .write
             .option("header", "true")
             .mode('append')
             .csv(prod_volume_write_path)
            )

            ## Del files with underscores after writing the csv files
            util_del_files_with_underscore_in_volume(self, prod_volume_write_path)

            ## Rename file with the filter condition + data_name_append
            util_rename_csv_file(self, prod_volume_write_path, file_prefix=filter, file_suffix=data_name_append)
            

        print(f'Finished creating CSV files in PROD: {prod_volume_write_path} \n')



    
    ###
    ### Create the dev csv file
    ###
    def create_dev_csv_files(self, spec_volume_path):
        """
        Creates and writes a CSV file that is a subset of the orginal with the dev and main catalogs volume.

        Parameters (obtained from outer/main method):
        ----------
            - dataframe (DataFrame): The Spark DataFrame to be filtered and written to CSV.

        Returns:
        -------
            Prints output in the log.

        Example:
        >>> create_dev_csv_files(dataframe)
        """
        print('-------------------------------------------------------')
        print(f'--- Creating CSV files in DEV: {spec_volume_path}')
        print('-------------------------------------------------------')
        (dataframe
         .filter(col("ID") <= 7499)
         .repartition(1)   ## One CSV file per date
         .withColumn("PII", lit("********"))
         .write
         .option("header", "true")
         .mode('append')
         .csv(spec_volume_path)
        )

        ## Del files with underscores after writing the csv files
        util_del_files_with_underscore_in_volume(self, spec_volume_path)

        ## Rename file with the filter condition + data_name_append
        ## Rename file with the filter condition + data_name_append
        util_rename_csv_file(self, spec_volume_path, file_prefix="dev", file_suffix=data_name_append)

        print(f'Finished creating CSV file in DEV: {spec_volume_path} \n')



    def create_stage_csv_files(self):
        """
        Creates and writes a CSV file that is a subset of the orginal with the stage catalog

        Parameters (obtained from outer/main method):
        ----------
            - dataframe (DataFrame): The Spark DataFrame to be filtered and written to CSV.

        Returns:
        -------
            Prints output in the log.

        Example:
        >>> create_dev_csv_files(dataframe)
        """
        print('-------------------------------------------------------')
        print(f'--- Creating CSV files in STAGE: {stage_volume_write_path}')
        print('-------------------------------------------------------')
        (dataframe
         .filter(col("ID") <= 34999)
         .repartition(1)   ## One CSV file per date
         .write
         .option("header", "true")
         .mode('append')
         .csv(stage_volume_write_path)
        )

        ## Del files with underscores after writing the csv files
        util_del_files_with_underscore_in_volume(self, stage_volume_write_path)

        ## Rename file with the filter condition + data_name_append
        ## Rename file with the filter condition + data_name_append
        util_rename_csv_file(self, stage_volume_write_path, file_prefix="stage", file_suffix=data_name_append)

        print(f'Finished creating CSV file in STAGE: {stage_volume_write_path} \n')



    ## CREATE PROD CSV FILES FOR PROD DEV STAGE
    create_dev_csv_files(self, spec_volume_path = dev_volume_write_path)
    create_dev_csv_files(self, spec_volume_path = main_volume_write_path)
    create_stage_csv_files(self)
    create_prod_csv_files(self)

# COMMAND ----------

## Setup course environment
list_of_catalog_suffixes = ['_1_dev', '_2_stage', '_3_prod']

## Create user specific catalogs
DA.create_catalogs(list_of_catalog_suffixes)

## Create the DA keys for the user's catalogs
DA.create_DA_keys()

## Create the volumes in each catalog
DA.create_volumes(in_catalog=DA.catalog_name, in_schema='default', vol_names=['health'])
DA.create_volumes(in_catalog=DA.catalog_dev, in_schema='default', vol_names=['health'])
DA.create_volumes(in_catalog=DA.catalog_stage, in_schema='default', vol_names=['health'])
DA.create_volumes(in_catalog=DA.catalog_prod, in_schema='default', vol_names=['health'])


##
## Create the CSV files
##

## Create the Spark dataframe to use
sdf = DA.create_spark_data_frame_from_cdc("/Volumes/dbacademy_cdc_diabetes/v01/cdc-diabetes/diabetes_binary_5050_raw.csv")


## Create the CSV files for the course.
DA.create_course_csv_files(dataframe=sdf, 
                           prod_volume_write_path = f'/Volumes/{DA.catalog_prod}/default/health', 
                           stage_volume_write_path = f'/Volumes/{DA.catalog_stage}/default/health',
                           dev_volume_write_path = f'/Volumes/{DA.catalog_dev}/default/health',
                           main_volume_write_path = f'/Volumes/{DA.catalog_name}/default/health',
                           data_filter_conditions = ["2025-01-01", "2025-01-02","2025-01-03"], 
                           data_name_append = "health", 
                           del_files_first = True)


## Setup Complete
print('\n\n\n------------------------------------------------------------------------------')
print('COURSE SETUP COMPLETE!')
print('------------------------------------------------------------------------------')

## Display the course catalog and schema name for the user.
DA.display_config_values(
  [
    ('Main catalog reference: DA.catalog_name', DA.catalog_name)
   ]
)
