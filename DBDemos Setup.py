# Databricks notebook source
# MAGIC %pip install dbdemos

# COMMAND ----------

import dbdemos

# COMMAND ----------

dbdemos.list_demos()

# COMMAND ----------

dbdemos.install('lakehouse-monitoring', catalog='main', schema='dbdemos_lhm', use_current_cluster=True)


# COMMAND ----------

dbdemos.install('uc-04-system-tables',use_current_cluster=True)

# COMMAND ----------


