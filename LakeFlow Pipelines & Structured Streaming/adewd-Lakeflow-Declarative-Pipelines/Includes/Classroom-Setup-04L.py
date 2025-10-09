# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])
# setattr(DA, 'paths.storage_location', f'{DA.paths.working_dir}/storage_location')

# COMMAND ----------

DA.paths.datasets.gym = '/Volumes/dbacademy_gym/v01'
DA.paths.stream_source = DA.paths.working_dir + "/stream"
DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"
DA.lookup_db = DA.schema_name

# COMMAND ----------

## Delete all files in the volume to start empty.
delete_source_files(source_files=f'{DA.paths.stream_source}/daily/')

# COMMAND ----------

DA.delete_all_pipelines()

# COMMAND ----------

DA.clone_table(f"{DA.lookup_db}.date_lookup_clone", source_name="date-lookup")

# COMMAND ----------

DA.daily_stream = StreamFactory(
    source_dir=DA.paths.datasets.gym, 
    target_dir=DA.paths.stream_source,
    load_batch=load_daily_batch,
    max_batch=16
)
DA.daily_stream.load()
