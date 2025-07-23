# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

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

#################################################################################
## Delete all raw files to start the pipeline over
#################################################################################   
## Step 1. First delete the DLT pipeline you created
## Step 2. Uncomment and run the method below to delete all files
#################################################################################

# DA.dlt_data_factory.delete_all_files()

# COMMAND ----------

DA.dlt_data_factory.load()
