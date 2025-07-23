# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo - Databricks Workspace Walkthrough
# MAGIC
# MAGIC In this demo, we will navigate through the Databricks DI Platform Workspace, understanding its layout and features. The Workspace acts as a central hub for organizing and accessing various assets, such as notebooks and files. By the end of this demo, you'll be familiar with navigating the Workspace efficiently and using its functionalities.
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC - Understand the Databricks Workspace interface.
# MAGIC - Learn how to navigate through different sections of the Workspace.
# MAGIC - Explore the organization of assets within the Workspace.
# MAGIC - Perform common actions available in the Workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Overview of the UI
# MAGIC
# MAGIC When you first land on the Databricks DI Platform, you'll be greeted by a landing page. The left-side navigation bar provides easy access to various services and capabilities, such as the Workspace, Data, Clusters, and more. In the top-right corner, you'll find settings, notifications, and other account-related options.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Workspace
# MAGIC
# MAGIC The Workspace is where you'll manage your assets and perform various tasks. Here's how to navigate and perform common actions within the Workspace:
# MAGIC
# MAGIC * **Create a Folder**:
# MAGIC    - Click on the **Workspace** button in the left navigation bar.
# MAGIC    - Select the folder where you want to create a new folder.
# MAGIC    - Right-click and choose **Create > Folder**.
# MAGIC    - Provide a name for the folder and click **Create**.
# MAGIC    - Alternatively, click on the **Create** button at the top-right corner.
# MAGIC    - Select **Folder** option name.
# MAGIC
# MAGIC * **Import a File**:
# MAGIC    - Navigate to the desired folder.
# MAGIC    - Right-click and choose **Import**.
# MAGIC    - Select the file you want to import and click **Import**.
# MAGIC    - Alternatively, navigate to desired folder and click on **( ⋮ )** kebab icon at top-right corner.
# MAGIC    - Select the **Import** option from the dropdown.
# MAGIC
# MAGIC * **Export a File**:
# MAGIC    - Right-click on a file in the Workspace.
# MAGIC    - Choose **Export** and select the desired export format.
# MAGIC    - Alternatively, navigate to folder in which file is present
# MAGIC    - Click on the click on **( ⋮ )** kebab icon and select export
# MAGIC    - Select the desired file type
# MAGIC
# MAGIC * **Navigating Folders**:
# MAGIC    - Double-click on a folder to enter it.
# MAGIC    - Use the breadcrumb trail at the top to navigate back.
# MAGIC    - Alternatively, click on the workspace on left sidebar to move at top of folder structure
# MAGIC
# MAGIC * **Create a notebook**:
# MAGIC    - Navigate to the desired folder
# MAGIC    - Right-click and select **Create > Notebook**
# MAGIC    - Alternatively, click on the **Create** button inside the desired folder
# MAGIC    - Select **Notebook** option to create new notebook
# MAGIC
# MAGIC * **Rename Folder**:
# MAGIC    - Navigate over your folder name
# MAGIC    - Right-click on a folder and select **Rename**
# MAGIC    - Enter the new name and select **Ok**
# MAGIC    - Alternatively, navigate to desired folder 
# MAGIC    - Click on kebab icon **( ⋮ )** on folder name want to rename
# MAGIC    - Rename the folder and click on **ok** button
# MAGIC  
# MAGIC * **Share Folder**:
# MAGIC    - Hover over the folder you want to share
# MAGIC    - Click on the kebab icon **( ⋮ )** at right corner
# MAGIC    - Click on the Share(permissions) option from the dropdown
# MAGIC    - Select the username from the search bar you want to share
# MAGIC    - Provide edit, view, manage, run permissions you want to give to user
# MAGIC    - Click on **Add** button

# COMMAND ----------

# MAGIC %md
# MAGIC ## User Settings
# MAGIC
# MAGIC 1. Go to the top right, click on the user icon, and select **Settings**.
# MAGIC
# MAGIC 2. In **Settings**, you will see **User Settings** with five different options:
# MAGIC    - **Profile**: view your display name, group memberships, and change password (if permitted by your identity management system)
# MAGIC    - **Preferences**: configure language and UI theme
# MAGIC    - **Developer**: configure the notebook development environment, and create API tokens (if permitted by the workspace configuration)
# MAGIC    - **Linked Accounts**: configure access to secured git repositories
# MAGIC    - **Notifications**: configure notifications for various events related to workspace resources

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Notebooks
# MAGIC
# MAGIC Let's now explore the power of Databricks notebooks:
# MAGIC
# MAGIC * Attach a Notebook to a Cluster:
# MAGIC    - Click on **Workspace** in the left navigation bar.
# MAGIC    - Select the desired folder or create a new one.
# MAGIC    - Right-click and choose **Create > Notebook**.
# MAGIC    - Name your notebook and select the previously created cluster from the dropdown.
# MAGIC    - Click **Confirm**.
# MAGIC
# MAGIC * Creating a Cell
# MAGIC    - Navigate to the bottom of existing cell
# MAGIC    - Click on **+Code** to prepare a new cell for code in the default notebook language, or **+Text** to prepare a new cell for Markdown content.    
# MAGIC
# MAGIC * Running a Cell:
# MAGIC    - Cells in a notebook can be executed using the **▶** button at the top-left corner of the cell or by pressing **Shift + Enter**.
# MAGIC    - Try running a cell with simple Python code to see the output.
# MAGIC
# MAGIC * Run all cells:
# MAGIC    - Click on the **Run all** button at the top of the page to run all cells in sequence
# MAGIC
# MAGIC * Choose language for code cells:
# MAGIC    - Navigate to the language switcher cell at the top-right of cell
# MAGIC    - Select the desired language for your cell
# MAGIC    - Alternatively, type **%py** or **%sql** at the top of the cell
# MAGIC
# MAGIC ### Left sidebar actions
# MAGIC    - Click on the **Table of contents** icon between the left sidebar and the topmost cell to access notebook contents (relies on Markdown elements to define the hierarchy)
# MAGIC    - Click on the **Workspace** icon to access **folder** structure of the workspace
# MAGIC    - Click on the **Catalog** icon to access the metastore (the structures containing your data objects)
# MAGIC    - Navigate to **Assistant** to activate an assistant that can provide code suggestions and error diagnostics.
# MAGIC
# MAGIC ### Right sidebar actions 
# MAGIC    - Click on the message icon to add **Comments** on existing code
# MAGIC    - Use the **MLflow experiments** icon to create a workspace experiment.
# MAGIC    - Access code versioning history through the **Version history** icon (this is an internal versioning system that provides a basic yet convenient set of features; though it is not intended to replace a full-fledged versioning system like git).
# MAGIC    - Get a list of variables used in a notebook by navigating to **Variable explorer** icon
# MAGIC    - Discover the specifications for this notebook by navigating to the **Environment** icon.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unity Catalog and Catalog Explorer
# MAGIC
# MAGIC Managing permission is essential for controlling who can access and perform actions on your data and resources. Unity catalog allows data asset owners to manage permissions using the Catalog Explorer UI or using SQL commands.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Granting Table Permissions to Users with the UI
# MAGIC
# MAGIC 1. Navigate to **Catalog** in the sidebar and search for catalog the catalog we made earlier (see **Create a sample table** within this notebook). 
# MAGIC 2. Navigate to the **Schema** within the catalog.
# MAGIC 3. Click the table we created earlier in **Catalog Explorer** to open the table details page, and go to the **Permissions** tab. 
# MAGIC 4. Click **Grant**. 
# MAGIC     - Select the users and groups you want to give permission to. 
# MAGIC     - Select the privileges you want to grant. For example, assign `SELECT` (read) privilege. 
# MAGIC     - Under **Privilege presents** you can grant broad privileges as a Data Reader or Data Editor: 
# MAGIC       - **Data Reader**: can read from any object in the catalog. 
# MAGIC       - **Data Editor**: can read and modify any object in the catalog, as well as create new objects. 
# MAGIC 5. Click **Grant** once you have made your selections. 
# MAGIC
# MAGIC Inside **Catalog Explorer**, we can also see options for creating and uploading other assets such as **volumes** and **models**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) Understanding Compute
# MAGIC
# MAGIC
# MAGIC 1. On the left sidebar, you'll see **Compute**. Click on it.
# MAGIC
# MAGIC 2. You will be taken to a screen where, at the top, you will see six different tabs:
# MAGIC    - **All-Purpose Compute**
# MAGIC    - **Job Compute**
# MAGIC    - **SQL Warehouses**
# MAGIC    - **Vector Search**
# MAGIC    - **Pools**
# MAGIC    - **Apps**
# MAGIC
# MAGIC 3. Inside **All-Purpose Compute**, click on **Create with Personal Compute** at the top right.
# MAGIC
# MAGIC 4. You will be taken to a screen where you can view how to create a new compute instance. For example, you will be able to see compute policy:
# MAGIC    - **Personal Compute**.
# MAGIC
# MAGIC 5. You will see **Access Mode**, which contains:
# MAGIC    - **Single User Shared**
# MAGIC    - **No Isolation Shared**
# MAGIC
# MAGIC 6. Additionally, you will see options for:
# MAGIC    - **Performance**
# MAGIC    - **Node Type**
# MAGIC    - **Instance Profile**
# MAGIC    - **Tags**
# MAGIC    - Other advanced options.
# MAGIC
# MAGIC 7. In the **Summary** section, you will see a couple of tags, such as:
# MAGIC    - **Unity Catalog** being enabled for this compute
# MAGIC    - The **Runtime** details for the compute.
# MAGIC
# MAGIC 8. Since we are not creating a new compute, go ahead and click **Cancel**.
# MAGIC
# MAGIC
# MAGIC #### Understanding Compute and Runtimes in Databricks
# MAGIC
# MAGIC Databricks offers various types of compute for different tasks, including:
# MAGIC
# MAGIC 1. **Serverless Compute for a Notebook**
# MAGIC 2. **Serverless Compute for Jobs**
# MAGIC 3. **All-Purpose Compute**
# MAGIC 4. **Jobs Compute**
# MAGIC 5. **Instance Pools**
# MAGIC 6. **Serverless SQL Warehouses**
# MAGIC 7. **Classic SQL Warehouses**
# MAGIC
# MAGIC We also have the choice of using a CPU or a GPU for our compute type. 
# MAGIC
# MAGIC
# MAGIC ### Photon
# MAGIC
# MAGIC In addition to these compute options, Databricks provides **Photon**, a high-performance native vectorized query engine. Photon runs SQL workloads and DataFrame API calls faster, reducing the total cost per workload.
# MAGIC
# MAGIC ### Serverless Compute
# MAGIC
# MAGIC **Serverless compute** enhances productivity, efficiency, and reliability by automatically managing infrastructure needs for your workloads.
# MAGIC
# MAGIC In addition to the different types of compute, there are also various **Databricks Runtimes**. Each Databricks runtime version includes updates that improve usability, performance, and security. All of this is managed infrastructure provided by Databricks.
# MAGIC
# MAGIC The **Databricks Runtime** on your compute adds many features, such as **Delta Lake** and pre-installed **Java**, **Scala**, **Python**, and **R** libraries
# MAGIC
# MAGIC ### Databricks Machine Learning Runtime
# MAGIC
# MAGIC The **Databricks Machine Learning Runtime** provides scalable clusters that include:
# MAGIC - Popular machine learning frameworks
# MAGIC - Built-in **AutoML**
# MAGIC - Optimizations for performance enhancements in data science and machine learning tasks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this demo, we explored the Databricks DI Platform Workspace and its features. You've learned how to navigate the interface, manage folders, import and export files, and work with Git repositories. The Workspace provides a central location for organizing and accessing your assets, making your work more organized and efficient.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
