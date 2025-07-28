# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Deploying a DAB to Multiple Environments
# MAGIC
# MAGIC In this demonstration, we will deploy a Databricks project to both a development and production environment using DABs and modify the configuration for each environment.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Explore and modify variables within Databricks Asset Bundles.
# MAGIC - Leverage the **includes** top-level mapping to define paths that contain configuration files for inclusion in the bundle.
# MAGIC - Deploy projects across multiple environments using Databricks Asset Bundles.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC     - In the drop-down, select **More**.
# MAGIC
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course.
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically reference the information needed to run the course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-03

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMPORTANT LAB INFORMATION
# MAGIC
# MAGIC Recall that your credentials are stored in a file when running [0 - REQUIRED - Course Setup and Authentication]($../0 - REQUIRED - Course Setup and Authentication).
# MAGIC
# MAGIC If you end your lab or your lab session times out, your environment will be reset.
# MAGIC
# MAGIC If you encounter an error regarding unavailable catalogs or if your Databricks CLI is not authenticated, you will need to rerun the [0 - REQUIRED - Course Setup and Authentication]($../0 - REQUIRED - Course Setup and Authentication) notebook to recreate the catalogs and your Databricks CLI credentials.
# MAGIC
# MAGIC **Use classic compute to use the CLI through a notebook.**

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the Databricks CLI command below to confirm the Databricks CLI is authenticated.
# MAGIC </br>
# MAGIC ##### DATABRICKS CLI ERROR TROUBLESHOOTING:
# MAGIC   - If you encounter an Databricks CLI authentication error, it means you haven't created the PAT token specified in notebook **0 - REQUIRED - Course Setup and Authentication**. You will need to set up Databricks CLI authentication as shown in that notebook.
# MAGIC
# MAGIC   - If you encounter the error below, it means your `databricks.yml` file is invalid due to a modification. Even for non-DAB CLI commands, the `databricks.yml` file is still required, as it may contain important authentication details, such as the host and profile, which are utilized by the CLI commands.
# MAGIC
# MAGIC ![CLI Invalid YAML](../Includes/images/databricks_cli_error_invalid_yaml.png)

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks catalogs list

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the `databricks -v` command to view the version of the Databricks CLI. Confirm that the cell returns version **v0.240.0**.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Development and Production Data

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Preview the development data in your **username_1_dev** catalog. Note the following:
# MAGIC    - It contains 7,500 rows (excluding the header column).
# MAGIC    - The PII data is masked.
# MAGIC
# MAGIC **NOTE:** In this scenario, the sample data in the development environment is a subset of production data used for testing. We will testing later.

# COMMAND ----------

spark.sql(f'''
SELECT *
FROM text.`/Volumes/{DA.catalog_dev}/default/health`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Preview the production data in your **username_3_prod** catalog. Note the following:
# MAGIC    - It contains 70,695 rows.
# MAGIC    - The PII data is available. <username>
# MAGIC
# MAGIC **NOTE:** In our scenario, a CSV file will be added to the **health** volume in the prod catalog daily. If you inspect the health volume in the production catalog, you will find 3 days already populated.

# COMMAND ----------

spark.sql(f'''
SELECT count(*) AS Total
FROM text.`/Volumes/{DA.catalog_prod}/default/health`
''').display()

# COMMAND ----------

spark.sql(f'''
SELECT *
FROM text.`/Volumes/{DA.catalog_prod}/default/health`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Deploy a DAB to Multiple Environments (Development and Production)
# MAGIC
# MAGIC In this example, we will be using the same job from demonstration 01. Here are the desired configurations of each environment (catalog):
# MAGIC
# MAGIC #### Development target configuration requirements:
# MAGIC - Use the value **username_1_dev** for the development catalog to read and write to.
# MAGIC - Run the job using the **small lab cluster** since the development data is small and static.
# MAGIC - Make the development environment the **default** environment.
# MAGIC
# MAGIC #### Production target configuration requirements:
# MAGIC - Use the value **username_3_prod** for the production catalog to access the production data.
# MAGIC - Run the job using serverless compute since the data will continually grow, letting Databricks Serverless adjust to the compute needs.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Open the **./resources/demo_03_job.job.yml** file in a new tab and explore the job configuration.
# MAGIC
# MAGIC    a. The job key name is **demo03_job**.
# MAGIC
# MAGIC    b. The job name is **${bundle.target}\_demo3_dab_${workspace.current_user.userName}**.
# MAGIC
# MAGIC    c. The job is using the **../src/create_bronze_table.py** and **../src/create_silver_table.py** notebooks. 
# MAGIC    
# MAGIC    d. In the YAML file, scroll down and notice that the parameters for this job are using variables:
# MAGIC    ```YAML
# MAGIC       parameters:
# MAGIC       - name: display_target
# MAGIC         default: ${bundle.target}
# MAGIC       - name: catalog_name
# MAGIC         default: ${var.target_catalog}
# MAGIC     ```
# MAGIC
# MAGIC    e. No cluster is being specified, so this job by default will run on Serverless compute.
# MAGIC    
# MAGIC    f. Leave this tab open.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the follow code to obtain your lab user name. You will need this for the next section.

# COMMAND ----------

print(DA.catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. In your other tab, navigate to the **./databricks.yml** file located in the main demonstration folder and explore the bundle. Notice the following:
# MAGIC
# MAGIC    a. This bundle is named **demo03_bundle**.
# MAGIC
# MAGIC    b. This bundle contains an **include** top-level mapping, which specifies the path to the **./resources/demo_03_job.job.yml** file. This YAML file will define the job for the **resources** mapping as you saw in the previous step. 
# MAGIC    
# MAGIC    **NOTE:** As your project grows, it's best practice to modularize the resources of the DAB.
# MAGIC
# MAGIC    c. This DAB also contains a **variable** top-level mapping. Let's modify and view the defined variables:
# MAGIC
# MAGIC       - The `my_lab_user_name` variable uses the substitute variable `${workspace.current_user.short_name}`. This will obtain your username for the lab and will be used to propagate the correct value to the remaining variables dynamically for each catalog.
# MAGIC
# MAGIC       - The `catalog_dev` variable uses the `my_lab_user_name` variable and appends **_1_dev** to your user name to reference your development catalog.
# MAGIC
# MAGIC       - The `catalog_prod` variable uses the `my_lab_user_name` variable and appends **_3_prod** to your user name to reference your production catalog.
# MAGIC
# MAGIC       - The `target_catalog` variable by default references your default catalog. Remember, this variable is being used in your job parameter specified in your **./resources/demo_03_job.job.yml** configuration:
# MAGIC
# MAGIC       </br>
# MAGIC     
# MAGIC     ```YAML
# MAGIC       parameters:
# MAGIC       - name: display_target
# MAGIC         default: ${bundle.target}
# MAGIC       - name: catalog_name
# MAGIC         default: ${var.target_catalog}
# MAGIC     ```
# MAGIC    </br>
# MAGIC    
# MAGIC       - The `raw_data_path` variable references the **health** volume using the `target_catalog` value, which by default references the **health** volume in your default catalog. 
# MAGIC
# MAGIC       - **TO DO:** The `cluster_id` variable will use a lookup variable to obtain the cluster ID for the specific lab user. By default, in the Databricks Academy lab, your cluster name is your user name. So, here get your username from the cell above and paste the cluster name next to cluster. This will look up the cluster id from the cluster name. 
# MAGIC
# MAGIC    d. Leave this tab open.

# COMMAND ----------

# MAGIC %md
# MAGIC 4. In the **databricks.yml** file, explore the **targets** first-level mapping. Notice the following:
# MAGIC
# MAGIC    a. When deploying to the **development** target:
# MAGIC    
# MAGIC       - It is set to **development** mode.
# MAGIC       
# MAGIC       - It is the **default** target.
# MAGIC       
# MAGIC       - The root path where the files are placed will end with the target name, **development**.
# MAGIC       
# MAGIC       - In development, we are modifying the compute for each task in the **resources** mapping. Here, we are adding our small lab compute to process the tasks using the lookup variable `my_cluster_id` we defined earlier. We are doing this because, in development, we know our data is small and don't need large compute resources.
# MAGIC
# MAGIC    b. When deploying to the **production** target:
# MAGIC    
# MAGIC       - It is set to **production** mode.
# MAGIC       
# MAGIC       - The `target_catalog` variable is modified from the default value of `${var.catalog_dev}` to `${var.catalog_prod}`. This will read from our production catalog and write to the production catalog when deployed in production.
# MAGIC       
# MAGIC       - The root path where the files are placed will end with the target name, **production**.
# MAGIC
# MAGIC       - The job will run in the **Serverless** mode since are not overriding the compute of the specified job in production. 
# MAGIC
# MAGIC **NOTES:**
# MAGIC - If available, we could specify the **host** and modify which Databricks Workspace to run this in. In our lab, we only have one Workspace, so we are isolating environments by catalogs.
# MAGIC   
# MAGIC - In this example, we kept it simple and modified a few configurations for the **production** target. [You can modify a variety of other configurations](https://docs.databricks.com/aws/en/dev-tools/bundles/settings).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Let's validate the bundle for this demonstration. Confirm that the bundle validated correctly.
# MAGIC
# MAGIC     **NOTE:** If the bundle does not validate, read the error and fix the issue. Common issues include:
# MAGIC
# MAGIC     - Missing file extensions on the notebook paths within the **demo_03_job.job.yml** file.
# MAGIC
# MAGIC     - Failing to modify the variables correctly in the **databricks.yml** file.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle validate --output json

# COMMAND ----------

# MAGIC %md
# MAGIC 6. You can run the `databricks bundle validate --output json` to view details of the bundle configuration in JSON format. This helps you identify all of the configurations within your DAB. You can use these values as variable substitutions.
# MAGIC
# MAGIC
# MAGIC Here are some commonly used substitutions:
# MAGIC
# MAGIC - ${bundle.name}
# MAGIC
# MAGIC - ${bundle.target}  # Use this substitution instead of ${bundle.environment}
# MAGIC
# MAGIC - ${workspace.host}
# MAGIC
# MAGIC - ${workspace.current_user.short_name}
# MAGIC
# MAGIC - ${workspace.current_user.userName}
# MAGIC
# MAGIC - ${workspace.file_path}
# MAGIC
# MAGIC - ${workspace.root_path}
# MAGIC
# MAGIC - ${resources.jobs.<job-name>.id}
# MAGIC
# MAGIC - ${resources.models.<model-name>.name}
# MAGIC
# MAGIC - ${resources.pipelines.<pipeline-name>.name}
# MAGIC
# MAGIC
# MAGIC For example, in the JSON configuration below, notice that 
# MAGIC - `bundle.target` is *development* in the JSON. Recall we use the `${bundle.target}` variable within our YAML file.
# MAGIC - in the JSON output look for "workspace", then "current_user", then "short_name". Notice this value will return your user name without the vocareum extension.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle validate --output json

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Deploy to the Development Environment

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Let's delete the tables **health_bronze_demo03** and **health_silver_demo03** if they exist in our development catalog to verify that our bundle is creating the tables correctly. Run the code and confirm that the tables are not in your **_1_dev** catalog. The output from the following cell will display any other table that exists inside the **default** schema except for `health_bronze_demo03` and `health_silver_demo03`. 
# MAGIC

# COMMAND ----------

del_table(DA.catalog_dev, 'default', 'health_bronze_demo03')
del_table(DA.catalog_dev, 'default', 'health_silver_demo03')

spark.sql(f'''SHOW TABLES IN {DA.catalog_dev}.default''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Let's deploy the bundle to the **development** environment using the specific configurations.
# MAGIC
# MAGIC **NOTE:** If you do not specify `-t development`, it will deploy to this environment by default since the configuration `default: True` is used for the development target in the **databricks.yml** file. However, it's better to be explicit.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle deploy -t development

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle summary

# COMMAND ----------

# MAGIC %md
# MAGIC 3. When the cell above completes (in about a minute), view the deployed job named `[dev username] development_demo3_dab_<username>`
# MAGIC
# MAGIC In the job, check the following:
# MAGIC
# MAGIC - Select the job tasks and confirm that each task is using our lab compute cluster specified in the configuration.
# MAGIC
# MAGIC - Find the **Job parameters** section in the right details pane. Note the following values:
# MAGIC
# MAGIC     **Job parameters**
# MAGIC     - **catalog_name** - `<username>_1_dev`
# MAGIC     - **display_target** - `development`
# MAGIC
# MAGIC Recall that we deployed the job in development mode, and it’s using the default variables we created in the **databricks.yml** file to read from and write to our development catalog.
# MAGIC
# MAGIC Note: When running a job from the command line, you will need to pass the job key from the job YAML file. For example, in our scenario, we have the following in our job YAML file:
# MAGIC
# MAGIC
# MAGIC ```YAML
# MAGIC   resources:
# MAGIC     jobs:
# MAGIC       demo03_job:
# MAGIC         name: ${bundle.target}_demo3_dab_${workspace.current_user.userName}
# MAGIC         ...
# MAGIC ```
# MAGIC
# MAGIC So, we will run `databricks bundle run -t development demo03_job` and _not_ `databricks bundle run -t development development_demo3_dab_<username>` or any variant.

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Run the job in the development environment.
# MAGIC
# MAGIC     **NOTE:** While the job is running, let's take a moment to address any specific questions.
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle run -t development demo03_job

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the following cell to view the available tables in the development catalog after the job completes (in about 2 minutes).
# MAGIC
# MAGIC     Notice that the two new tables, **health_bronze_demo03** and **health_silver_demo03**, were created.

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {DA.catalog_dev}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Count the number of rows in the **health_bronze_demo03** table in the **user_name_1_dev** catalog. Notice that it contains 7,500 rows, as we are using the development data.
# MAGIC
# MAGIC     This confirms that our job correctly read from and wrote to the development catalog.

# COMMAND ----------

spark.sql(f'''
    SELECT count(*) 
    FROM {DA.catalog_dev}.default.health_bronze_demo03''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Deploy to the Production Environment
# MAGIC Now that we've confirmed the job ran in the development environment let's easily deploy the job to run in our production environment!
# MAGIC
# MAGIC **NOTE:** Typically when running in production you will want to run the job using a service principal. For more information, check out the [Set a bundle run identity](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as).
# MAGIC
# MAGIC For demonstration purposes, we are simply running the production job as the user.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Before we deploy to production let's check (and delete if necessary) the tables in the `<username>_3_prod` catalog. Notice that the tables `health_bronze_demo03` and `health_silver_demo03` are not present in the production catalog.

# COMMAND ----------

del_table(DA.catalog_prod, 'default', 'health_bronze_demo03')
del_table(DA.catalog_prod, 'default', 'health_silver_demo03')

spark.sql(f'SHOW TABLES IN {DA.catalog_prod}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Let's view the **production** configurations. Note the following:
# MAGIC
# MAGIC ```YAML
# MAGIC   production:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       # host: https://dbc-d9be2316-40bd.cloud.databricks.com/
# MAGIC       root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
# MAGIC
# MAGIC     ## Change variable values when in the production environment to use the production catalog username_3_prod
# MAGIC     variables:
# MAGIC         target_catalog: ${var.catalog_prod}
# MAGIC ```
# MAGIC
# MAGIC   - Here, we are modifying the variable `target_catalog` to reference our variable `catalog_prod` that references our production catalog. This overrides the default job parameter in the **./resources/demo_03_job.job.yml** file.
# MAGIC
# MAGIC   - We are not adding any overrides regarding the cluster to use for our job. Since we do not provide any overrides, it will use the default set in the **./resources/demo_03_job.job.yml** file, which is using Serverless compute.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Let's deploy the bundle to the **production** environment using the specified configurations.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle deploy -t production

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle summary -t production

# COMMAND ----------

# MAGIC %md
# MAGIC 4. When the cell above completes (in about a minute), view the deployed job named `production_demo3_dab_<username>`.
# MAGIC
# MAGIC In the job, check the following:
# MAGIC
# MAGIC - Select the job tasks and confirm that each task is using Serverless compute.
# MAGIC
# MAGIC - Find the **Job parameters** section in the right details pane. Note the following values:
# MAGIC
# MAGIC     **Job parameters**
# MAGIC     - **catalog_name** - `<username>_3_prod`
# MAGIC     - **display_target** - `production`
# MAGIC
# MAGIC Recall that we deployed the job in production mode, and it’s using the configuration we specified in the **databricks.yml** file to read from and write to our production catalog, and to use Serverless compute.

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the production job using the Databricks CLI.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle run -t production demo03_job

# COMMAND ----------

# MAGIC %md
# MAGIC 6. While the job is running, let's view where the Databricks assets were bundled.
# MAGIC
# MAGIC     a. In the main navigation bar, right-click on **Workspace** and select *Open in a New Tab*.
# MAGIC
# MAGIC     b. Navigate to **Workspace > Users > your user name**.
# MAGIC
# MAGIC     c. Open the **.bundle** folder. Here, you should see the names of the bundles you have deployed (**demo01_bundle** and **demo03_bundle**).
# MAGIC
# MAGIC     d. Open the deployed **demo03_bundle** (the bundle name we specified in the **databricks.yml** file for this demonstration).
# MAGIC
# MAGIC     e. Here, we can see that we deployed to the **development** and **production** targets. 
# MAGIC
# MAGIC     f. Select the **production** folder. You will see the **artifacts**, **files**, and **state** folders.
# MAGIC
# MAGIC     g. Select the **files** folder.
# MAGIC
# MAGIC     h. Notice that all of the files we deployed have been added to this location for the production mode deployment within the Workspace.
# MAGIC
# MAGIC     i. Close this tab.

# COMMAND ----------

# MAGIC %md
# MAGIC 7. By now, the **production** job should be completed. Navigate to the job and confirm it executed successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Run the following code to view the tables in your `<username>_3_prod` catalog. Notice that the production job created the production tables `health_bronze_demo03` and `health_silver_demo03`.

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {DA.catalog_prod}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Count the number of rows in the `health_bronze_demo03` table in the `<username>_3_prod` catalog. Notice that it contains over 70,692 rows because it's read from the production data and writing to the production catalog.

# COMMAND ----------

spark.sql(f'''
          SELECT count(*) 
          FROM {DA.catalog_prod}.default.health_bronze_demo03'''
          ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Destroy the Bundles
# MAGIC Lastly since we are finished with this bundle, let's delete it using the `databricks bundle destroy` command.
# MAGIC
# MAGIC
# MAGIC   By default, you are prompted to confirm permanent deletion of the previously-deployed jobs, pipelines, and artifacts. To skip these prompts and perform automatic permanent deletion, add the `--auto-approve` option to the bundle destroy command.
# MAGIC
# MAGIC   **Warning:** Destroying a bundle permanently deletes a bundle’s previously-deployed jobs, pipelines, and artifacts. This action cannot be undone. For more information view [Destroy the bundle](https://docs.databricks.com/en/dev-tools/bundles/work-tasks.html#step-6-destroy-the-bundle)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Delete the bundles!

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle destroy --auto-approve
# MAGIC databricks bundle destroy -t production --auto-approve

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC
