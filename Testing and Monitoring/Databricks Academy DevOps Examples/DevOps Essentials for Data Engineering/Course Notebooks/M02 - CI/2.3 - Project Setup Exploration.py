# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.3 - Project Setup Exploration
# MAGIC
# MAGIC Explore the isolated catalogs within the project.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.
# MAGIC
# MAGIC #### The notebook "2.1 - Modularizing PySpark Code - Required" sets up the catalogs for this course. If you have not run this notebook, the catalogs will not be available.

# COMMAND ----------

# DBTITLE 1,Setup
# MAGIC %run ../Includes/Classroom-Setup-2.3

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore Your Environment
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Let's quickly explore the course folder structure and files for this course. We will explore each of these folders in depth throughout the course:
# MAGIC
# MAGIC     a. In the left navigation bar, make sure to select the folder icon.
# MAGIC
# MAGIC     b. Navigate back to the main course folder **DevOps Essentials for Data Engineering**. It contains the **Course Notebooks**, **src**, and **tests** folders (two folders back from this notebook).
# MAGIC
# MAGIC     c. Your main course folder includes the following:
# MAGIC
# MAGIC     - **Course Notebooks** folder: This folder contains the notebooks and files to follow along with during class demonstrations and labs.
# MAGIC
# MAGIC     - **src** folder: Contains the source production notebooks and Python files for the project.
# MAGIC
# MAGIC     - **tests** folder: Contains the unit and integration tests for our project.
# MAGIC     
# MAGIC     - A variety of files that will be used in the course.
# MAGIC
# MAGIC     d. Navigate back to **Course Notebooks** -> **M02 - CI**. We will dive into each of these folders and files throughout the course project.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Your environment has been configured with the following catalogs and files:
# MAGIC
# MAGIC     - Catalog: **unique_catalog_name_1_dev**
# MAGIC       - Schema: **default**
# MAGIC         - Volume: **health**
# MAGIC           - *dev_health.csv* : Small subset of prod data, anonymized *PII*, 7,500 rows
# MAGIC
# MAGIC     - Catalog: **unique_catalog_name_2_stage**
# MAGIC       - Schema: **default**
# MAGIC         - Volume: **health**
# MAGIC           - *stage_health.csv* : Subset of prod data, 35,000 rows
# MAGIC
# MAGIC     - Catalog: **unique_catalog_name_3_prod**
# MAGIC       - Schema: **default**
# MAGIC         - Volume: **health**
# MAGIC           - *2025-01-01_health.csv*
# MAGIC           - *2025-01-02_health.csv*
# MAGIC           - *2025-01-03_health.csv*
# MAGIC           - CSV files are added to this cloud storage location daily.Manually view your catalogs for this course.
# MAGIC
# MAGIC #### Complete the following to explore your catalogs:
# MAGIC
# MAGIC   a. In the navigation bar on the left, select the catalog icon.
# MAGIC
# MAGIC   b. Confirm your environment has created three new catalogs for you with the specified volume and files from above:
# MAGIC
# MAGIC   - **unique_name_1_dev**
# MAGIC
# MAGIC   - **unique_name_2_stage**
# MAGIC
# MAGIC   - **unique_name_3_prod**
# MAGIC
# MAGIC
# MAGIC **NOTE:** If you do not have the catalogs specified from above, you will need to run the **M02 - CI/2.1 - Modularizing PySpark Code - REQUIRED** notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Throughout the course, the following Python variables will be used to dynamically reference your course catalogs:
# MAGIC     - `DA.catalog_dev`
# MAGIC     - `DA.catalog_stage`
# MAGIC     - `DA.catalog_prod`
# MAGIC
# MAGIC     Run the code below and confirm the variables refer to your catalog names.
# MAGIC

# COMMAND ----------

print(f'DA.catalog_dev: {DA.catalog_dev}')
print(f'DA.catalog_stage: {DA.catalog_stage}')
print(f'DA.catalog_prod: {DA.catalog_prod}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Explore the Dev Data

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Complete the following to manually view the development CSV file in the volume within the dev catalog:
# MAGIC
# MAGIC     a. Expand the catalog **unique_name_1_dev**.
# MAGIC
# MAGIC     b. Expand the **default** schema. 
# MAGIC
# MAGIC     c. Confirm that a volume named **health** was created. 
# MAGIC
# MAGIC     d. Expand the **health** volume and confirm the *dev_health.csv* file is available.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the following cell to view the raw dev CSV file and see the number of rows. 
# MAGIC
# MAGIC     Notice that the *dev_health.csv* file:
# MAGIC     - Contains columns delimited by a comma with the **PII** column completely masked for simplicity.
# MAGIC     
# MAGIC     - Contains 7500 rows (7501 - 1 for the header column).
# MAGIC
# MAGIC **NOTE:** The *dev_health.csv* file is a small sample of the production data that we will use for development and testing.

# COMMAND ----------

spark.sql(f'''
SELECT *
FROM text.`/Volumes/{DA.catalog_dev}/default/health/dev_health.csv`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
