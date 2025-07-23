# Databricks notebook source
# MAGIC %run ./_common

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
DA.paths.datasets.retail_sales = f'{DA.paths.datasets.retail}/source_files/sales.csv'

# COMMAND ----------

# Define the destination directory and file path
destination_dir = f"/Volumes/{DA.catalog_name}/{DA.schema_name}/myfiles"
destination_path = f"{destination_dir}/customers.csv"
datasets_customers = f'{DA.paths.datasets.retail}/source_files/customers.csv'
# Create the myfiles directory if it doesn't exist
spark.sql(f'CREATE VOLUME IF NOT EXISTS `{DA.catalog_name}`.`{DA.schema_name}`.`myfiles`')

# Read the dataset
datasets_df = spark.read.csv(datasets_customers, header=True, inferSchema=True)

# Save the dataset as a CSV file in the myfiles directory
datasets_df.write.mode("overwrite").option("header", "true").csv(destination_dir)

# Move the generated CSV files to the desired filename
files_in_dir = dbutils.fs.ls(destination_dir)
csv_files = [file.path for file in files_in_dir if file.path.endswith(".csv")]

if csv_files:
    dbutils.fs.mv(csv_files[0], destination_path)

# COMMAND ----------

# Define the destination directory for cloud files
cloud_dir = f"{DA.paths.working_dir}/retail_sales"
cloud_file_path = f"{cloud_dir}/retail_sales.csv"
datasets_retail = DA.paths.datasets.retail_sales
# Ensure the destination directory exists
dbutils.fs.mkdirs(cloud_dir)

# Read the dataset
datasets_df = spark.read.csv(datasets_retail, header=True, inferSchema=True)

# Save the dataset as a CSV file in the cloud directory
datasets_df.write.mode("overwrite").option("header", "true").csv(cloud_dir)

# Move the generated CSV files to the desired filename
files_in_dir = dbutils.fs.ls(cloud_dir)
csv_files = [file.path for file in files_in_dir if file.path.endswith(".csv")]

if csv_files:
    dbutils.fs.mv(csv_files[0], cloud_file_path)
