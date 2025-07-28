# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Deploying a Simple Databricks Asset Bundle (DABs)
# MAGIC
# MAGIC
# MAGIC In this demonstration, we will create a simple job, examine its YAML configuration, and then learn how to validate, deploy, and run the job using DABs (Databricks Asset Bundles) to the development environment.
# MAGIC
# MAGIC ### Objectives
# MAGIC - Explain the purpose of the databricks.yml configuration file in the context of DABs.
# MAGIC - Identify key components in the YAML configuration of a DAB.
# MAGIC - Validate, deploy, and execute a job using a DAB.

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

# MAGIC %run ../Includes/Classroom-Setup-01

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
# MAGIC ## B. Create a Simple Job

# COMMAND ----------

# MAGIC %md
# MAGIC 1. During development, it's easier to manually create the job you want to automatically deploy with Databricks Asset Bundles in order to get the necessary YAML configuration for deployment.
# MAGIC
# MAGIC     Run the cell below and confirm that the job was created.
# MAGIC
# MAGIC     **NOTE:** To save time, we will use the Databricks Academy `DAJobConfig` class, which was created using the Databricks SDK to automatically create our job for this demonstration. In a typical development cycle, you would create the job manually.

# COMMAND ----------

job_tasks = [
    {
        'task_name': 'create_bronze_table',
        'notebook_path': '/01 - Deploying a Simple DAB/src/create_bronze_table',
        'depends_on': None
    },
    {
        'task_name': 'create_silver_table',
        'notebook_path': '/01 - Deploying a Simple DAB/src/create_silver_table',
        'depends_on': [{'task_key': 'create_bronze_table'}]
    }
]

myjob = DAJobConfig(job_name=f'demo1_simple_dab_ui_{DA.catalog_name}',
                    job_tasks=job_tasks,
                    job_parameters=[
                      {'name':'display_target', 'default':'development'},
                      {'name':'catalog_name', 'default':DA.catalog_dev}
                    ])

# COMMAND ----------

print(myjob)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete the following steps to explore the YAML configuration of the job:
# MAGIC
# MAGIC    a. In the left main navigation bar, right-click on **Jobs and Pipelines** and select *Open in a new tab*.
# MAGIC
# MAGIC    b. Locate your deployed job named **demo1_simple_dab_ui_username**.
# MAGIC
# MAGIC    c. Select your job.
# MAGIC
# MAGIC    **NOTE:** During development it would be beneficial to run and confirm the job works. For the purpose of this demonstration the job has been tested and validated.
# MAGIC
# MAGIC    d. In the right **Job details** pane, scroll to the bottom and find **Job parameters**. Notice that two parameters have been set for this job:
# MAGIC    
# MAGIC     - **catalog_name** - references your **username_1_dev** catalog.
# MAGIC     
# MAGIC     - **display_target** - specifies the environment where the job is running. In this example, we are using **development**.
# MAGIC
# MAGIC    e. In the top navigation bar, select **Tasks**. Notice that this job has two tasks:
# MAGIC
# MAGIC     - **TASK 1**: **create_bronze_table** reads from the development CSV file in the **username_1_dev** catalog (specified using job parameter **catalog_name**) and creates a table named **health_bronze_demo_1**.
# MAGIC     
# MAGIC     - **TASK 2**: **create_silver_table** reads from the bronze table in the **username_1_dev** catalog (specified using job parameter **catalog_name**) and creates a table named **health_silver_demo_1**. Task 2 depends on Task 1 to complete successfully.
# MAGIC     
# MAGIC     - Both tasks use Serverless compute.
# MAGIC
# MAGIC    f. Leave the job page open and move to the next task.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Complete the following steps to view the YAML configuration for the job:
# MAGIC
# MAGIC    a. Go back to your job.
# MAGIC
# MAGIC    b. At the top right of the job, select the kebab menu (three vertical dots icon near the **Run now** button).
# MAGIC
# MAGIC    c. Select **Edit as YAML**.
# MAGIC
# MAGIC    d. Notice that you can now view the YAML configuration for the job. This is a great way to easily get the necessary values for the YAML deployment for your Databricks Asset Bundles (DABs).
# MAGIC
# MAGIC    e. Copy the configuration.
# MAGIC    
# MAGIC    f. In the top right, select **Close editor**.
# MAGIC
# MAGIC    g. Leave the tab with your job open. We use this copied YAML configuration in a later section.
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Example YAML Configuration (yours will differ slightly)**
# MAGIC ```
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     demo1_simple_dab_ui_labuser123:
# MAGIC       name: demo1_simple_dab_ui_labuser123
# MAGIC       tasks:
# MAGIC         - task_key: create_bronze_table
# MAGIC           notebook_task:
# MAGIC             notebook_path: /Workspace/Shared/databricks-asset-bundles-source/Source/Automated
# MAGIC               Deployment with Databricks Asset Bundles/01 - Simple
# MAGIC               DAB/src/create_bronze_table
# MAGIC             source: WORKSPACE
# MAGIC         - task_key: create_silver_table
# MAGIC           depends_on:
# MAGIC             - task_key: create_bronze_table
# MAGIC           notebook_task:
# MAGIC             notebook_path: /Workspace/Shared/databricks-asset-bundles-source/Source/Automated
# MAGIC               Deployment with Databricks Asset Bundles/01 - Simple
# MAGIC               DAB/src/create_silver_table
# MAGIC             source: WORKSPACE
# MAGIC       parameters:
# MAGIC         - name: display_target
# MAGIC           default: development
# MAGIC         - name: catalog_name
# MAGIC           default: labuser123_1_dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Deploying your Job Using Databricks Asset Bundles (DABs)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the `databricks -v` command to view the version of the Databricks CLI. Confirm that the cell returns version **v0.240.0**.
# MAGIC <br>
# MAGIC
# MAGIC ##### DATABRICKS CLI ERROR TROUBLESHOOTING:
# MAGIC   - If you encounter an Databricks CLI authentication error, it means you haven't created the PAT token specified in notebook **0 - REQUIRED - Course Setup and Authentication**. You will need to set up Databricks CLI authentication as shown in that notebook.
# MAGIC
# MAGIC   - If you encounter the error below, it means your `databricks.yml` file is invalid due to a modification. Even for non-DAB CLI commands, the `databricks.yml` file is still required, as it may contain important authentication details, such as the host and profile, which are utilized by the CLI commands.
# MAGIC
# MAGIC ![CLI Invalid YAML](../Includes/images/databricks_cli_error_invalid_yaml.png)

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Use the `pwd` command to view the current working directory. It should display that you are in the folder **01 - Deploying a Simple DAB**. The CLI is using the current directory of this notebook.

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Use the `ls` command to view the available files in the current directory. Confirm that you see the **databricks.yml** file.
# MAGIC
# MAGIC **NOTE:** A bundle configuration file must be in YAML format and must contain at least the top-level `bundle` mapping. That is, in the `databricks.yml` file you will find the first mapping `bundle`. We will discuss other mappings shortly. In addition, each bundle must contain exactly one bundle configuration file named **databricks.yml**.

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Now that we have confirmed we are in the working directory of the **databricks.yml** file, let's open the bundle configuration file in a new tab and explore the bundle configuration.
# MAGIC
# MAGIC    a. In the left navigation, select the folder icon and confirm you are in the **01 - Deploying a Simple DAB** folder. Right-click on the **databricks.yml** file and select *Open in a new tab*.
# MAGIC
# MAGIC    b. In the **databricks.yml** file, a bundle configuration must contain only one top-level **bundle** mapping that associates the bundle’s contents with Databricks settings. This is a very simple bundle example.
# MAGIC    
# MAGIC    **NOTE:** For a list of all [bundle mappings](https://docs.databricks.com/en/dev-tools/bundles/settings.html#mappings) view the documentation. We will explore more of these mappings later in the course.
# MAGIC
# MAGIC    c. The **bundle** mapping is required and must include a bundle **name**. 
# MAGIC
# MAGIC    d. The **resources** mapping (notice this is blank in the YAML) specifies information about the Databricks resources used by the bundle. This bundle configuration defines a job resource. We will add our specific job in the next section and review the configuration.
# MAGIC
# MAGIC    e. The **targets** mapping specifies one or more target environments in which to run Databricks workflow. Each target is a unique collection of artifacts, Databricks workspace settings, and Databricks job or pipeline details. In this example, we have one target named **development** and it uses a simple configuration.
# MAGIC
# MAGIC     - The **mode:** *development* mapping defines this target as development mode. Development mode implements a variety of behaviors. One behavior is that it prepends all resources that are not deployed as files or notebooks with the prefix **[dev ${workspace.current_user.short_name}]** and tags each deployed job and pipeline with a dev Databricks tag. For more behaviors, visit the [Development mode](https://docs.databricks.com/en/dev-tools/bundles/deployment-modes.html#development-mode) documentation.
# MAGIC
# MAGIC     - The **default:** *true* mapping specifies that this is the default target environment if multiple targets are available. Setting the default to the **development** run helps avoid accidentally deploying to a production environment.
# MAGIC
# MAGIC     - In the **workspace** mapping the following are specified:
# MAGIC       - **host** specifies the Workspace to run this in. By default it will use the current workspace. We will leave this commented out.
# MAGIC       - **root_path** specifies where the files will be deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC 5. After examining the bundle configuration in the **databricks.yml** file, let's go back to our job and copy the YAML configuration (if necessary). Then paste the YAML configuration in the **resources** mapping with your specific job YAML configuration (under the RESOURCES comment).

# COMMAND ----------

# MAGIC %md
# MAGIC 6. After pasting your specific job configuration to your **databricks.yml** file, let's modify some of the paths to make them relative paths, add the notebook extensions, and give it an easy job key name.
# MAGIC
# MAGIC    a. Under **resources**, then **jobs** you will see a key named **demo1_simple_dab_ui_username**. Replace that key with **demo01_simple_dab**.
# MAGIC
# MAGIC    ```
# MAGIC    resources:
# MAGIC       jobs:
# MAGIC         demo1_simple_dab_ui_labuser1234:    ## <--------MODIFY THIS VALUE HERE TO demo01_simple_dab
# MAGIC           name: demo1_simple_dab_ui_labuser1234
# MAGIC    ```
# MAGIC
# MAGIC    b. For the **task_key: create_bronze_table** modify the **notebook_path** to `./src/create_bronze_table.py`.
# MAGIC
# MAGIC    c. For the **task_key: create_silver_table** modify the **notebook_path** to ` ./src/create_silver_table.py`.
# MAGIC
# MAGIC    d. Close the **databricks.yml** file.
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC **Example of the resource mapping configuration (yours will have a different name)**
# MAGIC ```
# MAGIC ...
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     demo01_simple_dab:
# MAGIC       name: l1_simple_dab_labuser1234
# MAGIC       tasks:
# MAGIC         - task_key: create_bronze_table
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/create_bronze_table.py
# MAGIC             source: WORKSPACE
# MAGIC         - task_key: create_silver_table
# MAGIC           depends_on:
# MAGIC             - task_key: create_bronze_table
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/create_silver_table.py
# MAGIC             source: WORKSPACE
# MAGIC       parameters:
# MAGIC         - name: display_target
# MAGIC           default: development
# MAGIC         - name: catalog_name
# MAGIC           default: labuser1234_1_dev
# MAGIC ...
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### NOTE - PLEASE READ!
# MAGIC
# MAGIC Starting December 20, 2024, the [default format for new notebooks is now IPYNB (Jupyter) format](https://docs.databricks.com/en/release-notes/product/2024/december.html#the-default-format-for-new-notebooks-is-now-ipynb-jupyter-format). This may cause issues when referencing notebooks with DABs, as you must specify the file extension.
# MAGIC
# MAGIC For the purpose of this course, all notebooks will be in either **.py** or **.sql** format. However, to confirm the file extension of a notebook, complete the following steps:
# MAGIC
# MAGIC - In the top navigation bar, below the notebook name, select **File**.
# MAGIC
# MAGIC - Scroll down and find the **Notebook format** option, then select it.
# MAGIC
# MAGIC - Here, you should see the notebook format listed as **Source (.scala, .py, .sql, .r)**.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 7. Let's validate our **databricks.yml** bundle configuration file using the Databricks CLI. Run the cell and confirm the validation of the bundle was successful.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle validate

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Let's deploy the bundle using the Databricks CLI!
# MAGIC
# MAGIC     Run the command below to deploy the bundle. The command `databricks bundle deploy -t development` specifies to deploy the bundle to the development environment. By default if we did not specify the target environment it would use the default target we specified earlier.
# MAGIC
# MAGIC     **NOTE:** This will take about a minute to complete.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle deploy -t development

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Let's view where the Databricks assets were deployed.
# MAGIC
# MAGIC     a. In the main navigation bar, right-click on **Workspace** and select *Open in a New Tab*.
# MAGIC
# MAGIC     b. Navigate to **Workspace > Users > your user name**.
# MAGIC
# MAGIC     c. Open the **.bundle** folder.
# MAGIC
# MAGIC     d. Open the deployed bundle **demo01_bundle** (the bundle name we specified in **databricks.yml**).
# MAGIC
# MAGIC     e. Here, we can see that we deployed the **development** target. Within the **development** folder, there will be a variety of folders and files.
# MAGIC
# MAGIC     **NOTE:** Since you are in the workspace, the bundle deployed as a "Source-linked deployment" (from the note in the cell above) when deploying to development. This means the files weren't moved into the target directory specified in the **databricks.yml** file. You can confirm this by checking the folder `.bundle/demo01_bundle/development/files` to verify that it's empty. If deploying from outside the workspace, the files would have been copied into that location. However, if you were to deploy to production while _in_ a Databricks Workspace, you would see that `.bundle/demo01_bundle/development/files` is non-empty and contains the relevant files. 
# MAGIC
# MAGIC
# MAGIC     f. Close the Workspace tab.

# COMMAND ----------

# MAGIC %md
# MAGIC 10. Complete the following steps to explore the job we deployed with a DAB.
# MAGIC
# MAGIC     a. In the left main navigation bar, right-click on **Jobs & Pipelines** and select *Open in a new tab*.
# MAGIC
# MAGIC     b. Find your deployed job named **[dev username] username demo01_simple_dab**.
# MAGIC
# MAGIC       - By default, development mode prepends all resources that are not deployed as files or notebooks with the prefix `[dev ${workspace.current_user.short_name}]` and tags each deployed job and pipeline with a dev Databricks tag.
# MAGIC
# MAGIC       - For other [development mode](https://docs.databricks.com/en/dev-tools/bundles/deployment-modes.html#development-mode) behaviors, view the documentation.
# MAGIC
# MAGIC     c. Select the job.
# MAGIC
# MAGIC     d. Notice the note at the top of the job: **Connected to Databricks Asset Bundles**. Select the link **Learn more**.
# MAGIC
# MAGIC     e. In the right navigation pane, scroll down to **Job parameters**. Notice that the parameters **catalog_name** and **display_target** are specified using the development specifications.
# MAGIC
# MAGIC     f. Leave the job tab open.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 11. Let's run the job! Run the cell below to run the job from the **databricks.yml** file using the CLI command. Run the cell and confirm the job runs successfully. 
# MAGIC
# MAGIC       - `databricks bundle run -t development demo01_simple_dab` specifies to run this job in the development environment. 
# MAGIC
# MAGIC       - This job key can be found under the **resources** mapping in the **databricks.yml** file.
# MAGIC
# MAGIC     **NOTE:** This will take about a 1-2 minutes to complete. If you get an error you must likely did not change the key within the **resources** mapping. View the example below.
# MAGIC
# MAGIC **Example (your actual job name will differ)**:
# MAGIC ```
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     demo01_simple_dab:    # <--- The job key name here. Your job key should be named: demo01_simple_dab
# MAGIC       name: l1_simple_dab_labuser1234
# MAGIC ```

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle summary

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle run -t development demo1_simple_dab

# COMMAND ----------

# MAGIC %md
# MAGIC 12. After the job was successfully run, navigate back to the job tab. Notice that the cell above automatically ran the specified job using our development catalog that we specified within the job parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC 13. Run the cell below and confirm the tables **health_bronze_demo_01** and **health_silver_demo_01** were created successfully in your dev catalog.

# COMMAND ----------

tables = spark.sql(f'''
SHOW TABLES IN {DA.catalog_dev}.default
''')

tables.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 14. Let's make a change to our bundle configuration in the **databricks.yml** file. 
# MAGIC
# MAGIC     a. Right click on the **databricks.yml** file and select *Open in a new tab*.
# MAGIC
# MAGIC     b. In the **resources** mapping modify the default value of the job parameter **display_target** to *development_new_parameter_value*.
# MAGIC
# MAGIC     c. Run the cell below to validate and deploy the new bundle. Wait until the cell completes (about 1 minute).
# MAGIC
# MAGIC **NOTE:** If you make a change to your configuration file you will have to redeploy the bundle. After you modify the **databricks.yml** file wait about 30 seconds for the auto save to save the file before redeploying.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle validate
# MAGIC databricks bundle deploy -t development

# COMMAND ----------

# MAGIC %md
# MAGIC 15. After the deployment completes, view the new deployed job by navigating back to your job and view the **Job parameters** (if the page is already open, refresh the page). 
# MAGIC
# MAGIC     Notice that the default value for the **display_target** parameter has been updated based on the change we made in the **databricks.yml** file.

# COMMAND ----------

# MAGIC %md
# MAGIC 16. Lastly since we are finished with this bundle, let's delete it using the `databricks bundle destroy` command.
# MAGIC
# MAGIC     By default, you are prompted to confirm permanent deletion of the previously-deployed jobs, pipelines, and artifacts. To skip these prompts and perform automatic permanent deletion, add the `--auto-approve` option to the bundle destroy command.
# MAGIC
# MAGIC   **Warning:** Destroying a bundle permanently deletes a bundle’s previously-deployed jobs, pipelines, and artifacts. This action cannot be undone. For more information view [Destroy the bundle](https://docs.databricks.com/en/dev-tools/bundles/work-tasks.html#step-6-destroy-the-bundle)

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle destroy --auto-approve

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC This was a quick introduction into bundle modification, validation, and deployment within Databricks. This demonstration was simple and meant as an introductory lesson into how you can perform automatic updates to your workflow using simple bash commands and configuring a YAML file. 
# MAGIC
# MAGIC How can we use Databricks Asset Bundles to easily deploy to multiple environments? Can we dynamically change values using variables?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC
