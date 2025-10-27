# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

## Create the DA keys for the user's catalogs
DA.create_DA_keys()

## Display the course catalog and schema name for the user.
DA.display_config_values(
  [
    ('DEV catalog reference: DA.catalog_dev', DA.catalog_dev),
    ('STAGE catalog reference: DA.catalog_stage', DA.catalog_stage),
    ('PROD catalog reference: DA.catalog_prod', DA.catalog_prod)
   ]
)

# COMMAND ----------

def obtain_pipeline_id_or_create_if_not_exists():
    '''
    Checks to see if the required DLT pipeline is created from the previous demo.

    If the job exists it returns the pipeline id.

    If the job does not exist it creates the DLT pipeline and returns the pipeline id.
    '''
    from databricks.sdk.service import pipelines
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    try:
        # New way to declare the pipeline
        pipeline = DeclarativePipelineCreator(
                            pipeline_name=f"sdk_health_etl_{DA.catalog_dev}", 
                            catalog_name = DA.catalog_name,
                            schema_name = 'default',
                            root_path_folder_name='src',
                            source_folder_names=[
                                'src/dlt_pipelines/**', 
                                'tests/integration_test/**'],
                            configuration = {
                                'target': 'dev',
                                'raw_data_path':f'/Volumes/{DA.catalog_name}/default/health'
                            })

        # print(f'----- Pipeline sdk_health_etl_{DA.catalog_dev} not found for the demonstration -----')

        pipeline.create_pipeline()
        pipeline.start_pipeline()
        return pipeline.pipeline_id
    except:
        for pipeline in w.pipelines.list_pipelines():
            if pipeline.name == f"sdk_health_etl_{DA.catalog_dev}":
                print('Pipeline found and pipeline id has been stored')
                return pipeline.pipeline_id

# COMMAND ----------

def create_demo_5_job(my_pipeline_id, job_name):
    import os
    from databricks.sdk.service import jobs
    from databricks.sdk import WorkspaceClient

    email_me = [DA.username] # Use DA object to get email 
    target_catalog = DA.catalog_name

    ## Creating an instance of the WorkspaceClient class from the Databricks SDK
    w = WorkspaceClient()

    ## Error if the job name already exists
    for job in w.jobs.list():
        if job.settings.name == job_name:
            test_job_name = False
            assert test_job_name, f'You already have a job with the same name. Please manually delete the job {job_name}'


    ## Store the path of the main course folder 2 folders back
    current_path = os.path.dirname(os.path.dirname(os.getcwd()))


    ##
    ## Create individual tasks
    ##
    ## Unit tests task
    unit_tests_notebook_path = f'{current_path}/Run Unit Tests'
    task_unit_tests = jobs.Task(task_key="Unit_Tests",
                                description="Execute unit tests for project.",
                                notebook_task=jobs.NotebookTask(notebook_path=unit_tests_notebook_path),
                                timeout_seconds=0)


    ## DLT Pipeline
    task_dlt = jobs.Task(task_key="Health_ETL",
                        description="Delta Live Tables",
                        pipeline_task=jobs.PipelineTask(pipeline_id=my_pipeline_id, full_refresh=True),
                        depends_on = [
                                    jobs.TaskDependency(task_key="Unit_Tests")
                                ],
                        timeout_seconds=0)



    ## Data visualization task
    visualization_notebook_path = f'{current_path}/src/Final Visualization'
    task_visualization = jobs.Task(task_key="Visualization",
                                description="Final visualization for project.",
                                notebook_task=jobs.NotebookTask(
                                    notebook_path=visualization_notebook_path,
                                    base_parameters = {'catalog_name': DA.catalog_name} # Use DA object to get catalog name
                                    ),
                                depends_on = [
                                    jobs.TaskDependency(task_key="Health_ETL")
                                ],
                                timeout_seconds=0)



    ##
    ## Create Entire Job Using the Tasks Above
    ##
    created_job = w.jobs.create(
            name=job_name,
            description='Final Workflow SDK',
            tasks=[
                task_unit_tests,
                task_visualization,
                task_dlt
                ],
            parameters = [
                    jobs.JobParameterDefinition(name='target', default='dev'),
                    jobs.JobParameterDefinition(name='catalog_name', default=target_catalog)
                ]
            )
    
    print('Creating Workflow using the Databricks SDK for the demonstration.')
