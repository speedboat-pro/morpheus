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
