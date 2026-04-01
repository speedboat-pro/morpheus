# Databricks notebook source
# MAGIC %md
# MAGIC # Data Analytics and Warehousing with Databricks
# MAGIC # Lab Environment
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo - Setting up a Catalog and Schema
# MAGIC ---
# MAGIC
# MAGIC Let's get started by running your first query. The query will retrieve your user name as listed in this Databricks workspace. We'll use that user name to set up a unique catalog name where you can store, update, and manage data in this shared workspace.
# MAGIC
# MAGIC There are a few ways to start a new query in Databricks SQL. The **New** button in the sidebar menu provides a shortcut for creating many new data objects, including queries. Complete the following steps to create a new query:
# MAGIC
# MAGIC 1.  Click **New** > **Query** in the sidebar menu
# MAGIC
# MAGIC   This is the Query Editor in Databricks SQL. Note the following features of the Query Editor:
# MAGIC
# MAGIC   *   Schema Browser
# MAGIC   *   Tabbed Interface
# MAGIC   *   Results View
# MAGIC
# MAGIC   To run a query, we need to make sure we have a warehouse selected.
# MAGIC
# MAGIC 2.  Click the drop-down in the upper-right corner to the left of the button labeled **Save**
# MAGIC
# MAGIC     The SQL warehouse is pre-created for you and should already be selected.
# MAGIC
# MAGIC     This drop-down lists the SQL Warehouses you have access to. It is possible you have access to only one warehouse. If this is the case, select this warehouse. If there are multiple warehouses in the list, select the one that fits your organization's requirements.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get your user name
# MAGIC ---
# MAGIC
# MAGIC The query shown in this section returns your user name as a result. Complete the following steps:
# MAGIC
# MAGIC 1.  Paste the code in the text box below into the query editor, and click **Run** in the query editor:
# MAGIC
# MAGIC ```
# MAGIC SELECT current_user();
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC The results display your user name. Copy your user name without the `@vocareum.com` suffix. For example, my user name is `labuser6429341@vocareum.com`, so I would copy `labuser6429341`.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create your catalog and schema
# MAGIC ---
# MAGIC
# MAGIC Now, create a catalog and schema that you can use throughout this course. A unique catalog name will effectively isolate your data so that you can make updates and changes without affecting other in your shared workspace. Within that catalog, we will also create a schema to hold the tables you will be working with.
# MAGIC
# MAGIC Let's use your unique username and concatenate it with the word Catalog. The following query creates a catalog and schema that you will use throughout the course.
# MAGIC
# MAGIC Complete the following steps:
# MAGIC
# MAGIC 1. Create a new query editor tab and paste in the following query. Create a unique catalog name by joining your username from the previous step with the string `_Catalog`.
# MAGIC
# MAGIC For example, if my username is `labuser6429341`, I would replace <catalog-name> with `labuser6429341_Catalog`.
# MAGIC
# MAGIC ```
# MAGIC CREATE CATALOG IF NOT EXISTS <catalog-name>;
# MAGIC USE CATALOG <catalog-name>;
# MAGIC CREATE SCHEMA IF NOT EXISTS dawd_v2;
# MAGIC ```
# MAGIC
# MAGIC Note: If a catalog with the same name already exists, you will not receive a notification about it, so please be sure to use your unique username for the catalog name used in this training.
# MAGIC
# MAGIC Now that you've created a catalog and schema use the UI to set the default catalog and schema values for the queries you run in the SQL editor.
# MAGIC
# MAGIC Note: When you add data in Databricks, you can access it using its three-level namespace, which is `<catalogname>.<schema-name>.<table-name>`. When you set the default catalog and schema names in the SQL editor UI, you can use the table name alone when writing your queries.
# MAGIC
# MAGIC 2. Above the query input window, to the right of the **Run** button, there are two drop-downs. Click the current selected catalog and select the catalog name you just created.
# MAGIC 3. Select the schema name `dawd_v2` from the second drop-down.
# MAGIC
# MAGIC With these values selected, all queries will run against the `dawd_v2` schema in your catalog.
# MAGIC
# MAGIC ---
# MAGIC ## Demo - Data Importing
# MAGIC ---
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lesson objectives
# MAGIC ---
# MAGIC
# MAGIC At the end of this lesson, you will be able to:
# MAGIC
# MAGIC * Identify small-file upload as a secure solution for importing small text files like lookup tables and quick data integrations.
# MAGIC * Use small-file upload in Databricks SQL to upload small text files securely as Delta tables.
# MAGIC * Add metadata to a table using Catalog Explorer.
# MAGIC * Import from object storage using Databricks SQL.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Uploading a data file
# MAGIC ---
# MAGIC
# MAGIC In this lesson, we will use two methods of ingesting data into Databricks SQL. First, we will upload a data file directly.
# MAGIC
# MAGIC There are many cases when it is beneficial to upload a data file directly to Databricks. When this is the case, Databricks provides Small File Upload, which allows you to upload a subset of file types, such as CSV, TSV, and JSON. Currently, we are using CSV for quick analysis or for uploading simple tables, like lookup tables. In this part of the demo, I'm going to show you how to use this feature. To begin, we need a data file we can upload.
# MAGIC
# MAGIC 1. Click [here](https://files.training.databricks.com/courses/data-analysis-with-databricks-sql/data.csv) to download a .csv file.
# MAGIC
# MAGIC 2. Click **New** > **Add or upload data** in the sidebar menu.
# MAGIC 3. Click **Create or modify table**.
# MAGIC
# MAGIC In this simple interface, you can drop files directly or browse for them on your local computer's filesystem. Note that you can only upload ten files at a time and that the total file size cannot exceed 2 GB.
# MAGIC
# MAGIC 4. Drag and drop the data.csv file (the one you downloaded in step 1) onto the page (or click **browse** and select the file)
# MAGIC
# MAGIC Note the features of the page that appear:
# MAGIC
# MAGIC * The top area where we can select the location of the table.
# MAGIC * **Advanced attributes** where the delimiter, escape character, and other attributes can be selected
# MAGIC * The bottom area where a preview of the data is displayed, and we can make changes to columns.
# MAGIC
# MAGIC Complete the following:
# MAGIC
# MAGIC 5. Select your catalog name and the schema **dawd_v2**.
# MAGIC 6. The table name defaults to the name of the file (minus the extension). Change the name of the table (if needed) to **data**.
# MAGIC 7. Click **Create table** at the bottom of the page.
# MAGIC
# MAGIC It may take a few seconds for the table to be created. After it is created, Catalog Explorer opens and shows the **data** table in the **dawd_v2** schema.
# MAGIC
# MAGIC ---
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Catalog Explorer
# MAGIC ---
# MAGIC
# MAGIC You can reach the Catalog Explorer anytime by clicking **Catalog** in the sidebar menu.
# MAGIC
# MAGIC Note the features of the Catalog Explorer:
# MAGIC
# MAGIC * You can view any catalog, schema, and table that you have access to view
# MAGIC
# MAGIC 1. Click your catalog name
# MAGIC 2. Select the schema `dawd_v2`
# MAGIC
# MAGIC You can view information about this schema, including the type (Schema), the owner (your username), and the list of tables in the schema.
# MAGIC
# MAGIC 3. Click the table we just created, **data**.
# MAGIC
# MAGIC This is the table details view, which includes more information about the table and a preview of the data.
# MAGIC
# MAGIC The following details are listed on the right side of the screen, under **About this table:**
# MAGIC
# MAGIC * You are the listed owner of this table.
# MAGIC * The data source format is Delta.
# MAGIC * The time and date of the table's last update.
# MAGIC * Popularity, as measured by the number of queries that have accessed the table in the past 30 days.
# MAGIC * The table size.
# MAGIC * No tags have been added to this table. Tags can be used to track usage.
# MAGIC * Because no comment was included in the CSV upload, an AI-generated suggested comment describes the data in this table. Click **Accept** to keep this table description.
# MAGIC
# MAGIC A tabbed UI near the top of the screen allows you to investigate specific details for this table.
# MAGIC
# MAGIC * The **Overview** tab shows the column names, data types, and comments for each column in the table.
# MAGIC * The **Sample Data** tab shows a selection of rows from the table.
# MAGIC * The **Details** tab shows additional information like table type and storage location.
# MAGIC * The **Permissions** tab shows privileges granted on the table.
# MAGIC * The **History** tab shows actions that have been taken on the table. This feature is available because this is a Delta table. We will talk more about this feature later in the course.
# MAGIC
# MAGIC Because you created the table, you are its owner and are automatically granted all privileges. You can also grant and revoke privileges for others. You can make changes to privileges using SQL or the Catalog Explorer UI. The following instructions explain how to grant privileges on the table.
# MAGIC
# MAGIC 4.  Click **Permissions**.
# MAGIC
# MAGIC 5.  Click **Grant**.
# MAGIC 6.  Start typing <b>account users</b> into the <b>Principals</b> text field. Select <b>All account users</b> when it appears.
# MAGIC 7.  Click the checkbox next to `SELECT` and then click **Grant**.
# MAGIC
# MAGIC This action gives all users registered to your Databricks account read access to this table. For users to access the data, you might also need to grant `USE` permissions on the catalog and schema for this table. 
# MAGIC
# MAGIC Privileges are hierarchical. Granting privilege on a catalog can cascade to schemas and tables contained in the catalog, but granting privilege on a table requires that you also set permissions for the data objects containing it.
# MAGIC
# MAGIC You can also use this view to revoke access to data objects.
# MAGIC
# MAGIC Complete the following steps to revoke access:
# MAGIC
# MAGIC 8. Select the checkbox next to **All account users**. Then click **Revoke**.
# MAGIC 9. Click **Revoke** in the warning that appears.
# MAGIC
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create new tables for the course
# MAGIC ---
# MAGIC
# MAGIC The data.csv file you uploaded contains data for three different tables we will use in this course. Let's run a query to create three individual tables.
# MAGIC
# MAGIC Complete the following steps:
# MAGIC
# MAGIC 1. Start a new query by clicking **New** > **Query** in the sidebar menu
# MAGIC 2. Make sure your catalog and schema are selected.
# MAGIC 3. Copy and paste the following query into the editor and click **Run**.
# MAGIC
# MAGIC ```
# MAGIC CREATE
# MAGIC OR REPLACE TABLE customers AS
# MAGIC SELECT
# MAGIC   customer_id2 AS customer_id,
# MAGIC   tax_id,
# MAGIC   tax_code,
# MAGIC   customer_name3 AS customer_name,
# MAGIC   state,
# MAGIC   city,
# MAGIC   postcode,
# MAGIC   street,
# MAGIC   number,
# MAGIC   unit,
# MAGIC   region,
# MAGIC   district,
# MAGIC   lon,
# MAGIC   lat,
# MAGIC   ship_to_address,
# MAGIC   valid_from,
# MAGIC   valid_to,
# MAGIC   units_purchased,
# MAGIC   loyalty_segment
# MAGIC FROM
# MAGIC   data
# MAGIC WHERE
# MAGIC   table_name1 IS NULL
# MAGIC   AND table_name IS NULL;
# MAGIC
# MAGIC CREATE
# MAGIC OR REPLACE TABLE sales AS
# MAGIC SELECT
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   product_name,
# MAGIC   order_date,
# MAGIC   product_category,
# MAGIC   product,
# MAGIC   total_price
# MAGIC FROM
# MAGIC   data
# MAGIC WHERE
# MAGIC   table_name IS NOT NULL;
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC 4. You can use the editor to automatically format your SQL queries if necessary. Click the <b> ( ⋮ ) </b> kebab menu, then select <b>Format Query</b>.
# MAGIC
# MAGIC Note: This query include two SQL statements separated by a semicolon. Running this command creates two tables. This action only needs to be performed once. You can delete these query statements after the tables have been created.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo - A Quick Query and Visualization
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lesson objectives
# MAGIC ---
# MAGIC
# MAGIC At the end of this lesson, you will be able to:
# MAGIC
# MAGIC *   Connect Databricks SQL queries to an existing Databricks SQL warehouse
# MAGIC *   Describe how to complete a basic query, visualization, and dashboard workflow using Databricks SQL
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Landing page
# MAGIC ---
# MAGIC
# MAGIC Let's review the landing page. The features on the landing page will help you navigate Databricks as you progress through the course.
# MAGIC
# MAGIC The landing page is the main page for Databricks. Note the following features of the landing page:
# MAGIC
# MAGIC *   Shortcuts
# MAGIC *   Recents and Favorites
# MAGIC *   Documentation Links
# MAGIC *   Links to Release Notes
# MAGIC *   Links to Blog Posts
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sidebar menu
# MAGIC ---
# MAGIC
# MAGIC On the left side of the page is the sidebar menu.
# MAGIC
# MAGIC 1.  Roll your mouse over the sidebar menu
# MAGIC
# MAGIC By default, the sidebar is expanded for all users. You can change this behavior by clicking the <b>Menu icon</b> in the upper left corner. This will cause the sidebar to be collapsed until you hover over it. To return the sidebar to a <b>held-open</b> state, simply click the <b>Menu icon</b> again.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run a query on the `customers` table
# MAGIC ---
# MAGIC
# MAGIC In this part of the lesson, follow the instructions below to run a query, make a visualization from that query, and create a simple dashboard with that visualization.
# MAGIC
# MAGIC 1. If your query tab contains the create table code that you used in a previous step, delete it.
# MAGIC 2. Make sure your catalog and the schema, `dawd_v2`, are selected in the drop-downs above the query editor
# MAGIC 2.  Paste the query below into the query editor, and click **Run**:
# MAGIC
# MAGIC     ```
# MAGIC     SELECT * FROM customers;
# MAGIC     ```
# MAGIC
# MAGIC You just ran a query on the `customers` table. This table contains dummy data for fake customers of a retail organization.
# MAGIC
# MAGIC Note the following features of the results window:
# MAGIC
# MAGIC *   Number of Results Received
# MAGIC *   Refreshed Time
# MAGIC *   **+** button for adding visualizations, filters, and parameters
# MAGIC
# MAGIC When you add visualizations to your queries, they appear in the results panel.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add a visualization
# MAGIC ---
# MAGIC
# MAGIC In the results panel, you can see data presented as a table.
# MAGIC
# MAGIC You are going to create a simple visualization, but please note that we are going to cover visualizations in much more detail later in the course. Complete the following steps:
# MAGIC
# MAGIC 1. Click the **+** button in the results section of the query editor, and then select **Visualization**
# MAGIC 2. Make sure that **Bar** is selected under **Visualization Type**.
# MAGIC 3. Use the **X column** drop-down to select **state**.
# MAGIC 4. Click **Add column** under **Y column**, and select __*__.
# MAGIC 5. Leave **Count** selected.
# MAGIC 6. In the upper-left corner, click **Bar 1** and change the name to **Customer Count by State**.
# MAGIC 7. Click **Save** in the lower-right corner
# MAGIC
# MAGIC The visualization is added to the query. Note that you did not have to perform any grouping or aggregation in the query itself, yet the visualization displays a count, grouped by state.
# MAGIC
# MAGIC Save the query:
# MAGIC
# MAGIC 8.  Click the **Save** button above the SQL editor, and save the query as **All Customers**. By default, the **Save** dialog prompts you to save workspace objects, like queries, to the home folder associated with your user name. Save it there for now, and next we'll talk about workspace organization.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Workspace organization
# MAGIC ---
# MAGIC
# MAGIC Keeping your work organized will help you find and share your work. Objects in a folder inherit all permissions settings of that folder. That means you can seamlessly share your work with colleagues by organizing by object type or project.  You'll learn more about setting permission levels later in the course. In this section, we’ll set up some folders in your workspace to keep track of your work.
# MAGIC
# MAGIC 1.  Select **Workspace** from the sidebar menu.
# MAGIC 2.  With **Home** selected, you can review the items created earlier in this demonstration.
# MAGIC 3.  Select the **Create** button in the upper-right corner.
# MAGIC 4.  From the drop-down menu, select **Folder**.
# MAGIC 5.  Name the folder **Queries**.
# MAGIC 6.  Click **Create**.
# MAGIC
# MAGIC   This opens the newly created folder. Returning to **Home**, you can drag and drop any saved queries to this folder.
# MAGIC
# MAGIC 7.  Repeat the steps above to create folders for “Dashboards” and “Alerts.” We'll create those later in the lesson.
# MAGIC
# MAGIC Going forward, as you save dashboards, queries, and alerts, you will be able to select the respective folder for the item in the save dialogue box.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo - Delta Lake in Databricks SQL
# MAGIC ---
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lesson Objectives
# MAGIC ---
# MAGIC
# MAGIC At the end of this lesson, you will be able to:
# MAGIC
# MAGIC *   Describe Delta Lake as a tool for managing data files.
# MAGIC *   Describe the benefits of Delta Lake in the Lakehouse.
# MAGIC *   Identify common differences between Delta Lake and popular enterprise data warehouses.
# MAGIC *   Summarize best practices for creating and managing schemas (databases), tables, and views on Databricks.
# MAGIC *   Use Databricks to create, use, and drop schemas (databases), tables, and views.
# MAGIC *   Optimize commonly used Delta tables for storage using built-in techniques.
# MAGIC *   Identify ANSI SQL as the default standard for SQL in Databricks.
# MAGIC *   Identify Delta- and Unity Catalog-specific SQL commands as the preferred data management and governance methods of the Lakehouse.
# MAGIC *   Identify common differences between common source environment SQL and Databricks Lakehouse SQL.
# MAGIC *   Describe the basics of the Databricks SQL execution environment.
# MAGIC *   Apply Spark- and Databricks-specific built-in functions to scale intermediate and advanced data manipulations.
# MAGIC *   Review query history and performance to identify and improve slow-running queries.
# MAGIC *   Access and clean silver-level data.
# MAGIC *   Describe the purpose of SQL user-defined functions (UDFs).
# MAGIC *   Create and apply SQL functions within the context of the medallion architecture.
# MAGIC *   Use Databricks SQL to ingest data
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake
# MAGIC ---
# MAGIC
# MAGIC Delta Lake is the optimized storage layer that provides the foundation for storing data and tables in the Databricks Data Intelligence Platform. Delta Lake is open source software that extends Parquet data files with a file-based transaction log for ACID transactions and scalable metadata handling. Delta Lake allows you to easily use a single copy of data for both batch and streaming operations, providing incremental processing at scale.
# MAGIC
# MAGIC Here are the benefits of using Delta Lake in the Databricks Lakehouse:
# MAGIC
# MAGIC ![Delta Lake features](https://s3.us-west-2.amazonaws.com/files.training.databricks.com/courses/data-analysis-with-databricks-sql/Delta+Lake+High+Level+Overview_crop.png)
# MAGIC
# MAGIC Here are a handful of differences between using Delta Lake in the Lakehouse and other data warehouse solutions:
# MAGIC
# MAGIC *   ACID transactions for data stored in data lakes
# MAGIC *   A single source of truth
# MAGIC *   Time travel
# MAGIC *   Schema evolution/enforcement
# MAGIC *   Open architecture
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create schema with one table
# MAGIC ---
# MAGIC Note: To create a schema (database), as discussed in this part of the lesson, you must have privileges to create schemas from your Databricks administrator.
# MAGIC
# MAGIC I want to show you how to create a new schema (database), and we will add one table to the schema.
# MAGIC
# MAGIC 1. Click SQL Editor in the sidebar menu to go to the SQL editor.
# MAGIC 2. Click the plus sign to open a new tab.
# MAGIC 3. Make sure your catalog and schema are selected.
# MAGIC 4. Paste the following series of SQL statements into the query editor. Then click **Run**.
# MAGIC
# MAGIC   ```
# MAGIC   DROP SCHEMA IF EXISTS temporary_schema CASCADE;
# MAGIC   CREATE SCHEMA IF NOT EXISTS temporary_schema;
# MAGIC   CREATE
# MAGIC   OR REPLACE TABLE temporary_schema.simple_table (width INT, length INT, height INT);
# MAGIC   INSERT INTO
# MAGIC     temporary_schema.simple_table
# MAGIC   VALUES
# MAGIC     (3, 2, 1);
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     temporary_schema.simple_table;
# MAGIC   ```
# MAGIC
# MAGIC The code runs five statements. The first drops the schema just in case we run this more than once. The second creates the schema. The schema will be created in the catalog chosen in the drop-down to the right of the Run button.
# MAGIC
# MAGIC In the third statement, we override the chosen schema in the drop-down above the query editor and USE the schema we just created.
# MAGIC
# MAGIC In the last three statements, we create a simple table, fill it with one row of data, and `SELECT` from it.
# MAGIC
# MAGIC This code modifies the database. After you run this code, you can delete it and keep working in the same query tab.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### View the table's metadata
# MAGIC ---
# MAGIC
# MAGIC 5.  Run the code below to see information about the table we just created:
# MAGIC
# MAGIC   ```
# MAGIC   DESCRIBE EXTENDED temporary_schema.simple_table;
# MAGIC   ```
# MAGIC
# MAGIC   Note that the table is a managed table, and the underlying data is stored in the metastore's default location. When we drop this table, our data will be deleted. Note also that this is a Delta table.
# MAGIC
# MAGIC   This query is used to demonstrate how to get additional information about the tables you're working with. There's no need to save this query. You can delete it and continue working in the same query tab.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC #### DROP the Table
# MAGIC ---
# MAGIC
# MAGIC 6.  Run the following query statement:
# MAGIC
# MAGIC   ```
# MAGIC   DROP TABLE IF EXISTS temporary_schema.simple_table;
# MAGIC   ```
# MAGIC
# MAGIC   You do not need to save this query. You can delete it and keep working in the same query tab.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Views
# MAGIC ---
# MAGIC
# MAGIC Views can be created from other views, tables, or data files. In the code below, we create a view of our sales table.
# MAGIC
# MAGIC 1.  Run the code below:
# MAGIC
# MAGIC   ```
# MAGIC   CREATE
# MAGIC   OR REPLACE VIEW high_sales AS
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     sales
# MAGIC   WHERE
# MAGIC     total_price > 10000;
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC     high_sales;
# MAGIC
# MAGIC   ```
# MAGIC
# MAGIC The view gives us all the sales that totaled more than 10,000. If new rows are added to the sales table, the view will add these rows the next time it's run.
# MAGIC
# MAGIC This query modifies the database by adding a view. There's no need to save this query. You can delete it and keep working in the same query tab.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Travel
# MAGIC ---
# MAGIC
# MAGIC #### SELECT on Delta tables
# MAGIC
# MAGIC In the next few queries, we are going to look at commands that are specific to using `SELECT` on Delta tables.
# MAGIC
# MAGIC Delta tables keep a log of changes that we can view by running the command below.
# MAGIC
# MAGIC 1. Run the code below:
# MAGIC
# MAGIC   ```
# MAGIC   DESCRIBE HISTORY customers;
# MAGIC   ```
# MAGIC
# MAGIC   After running DESCRIBE HISTORY, we can see that we are on version number 0 and we can see a timestamp of when this change was made.
# MAGIC
# MAGIC   This query is used to demonstrate the kind of information you get when using `DESCRIBE HISTORY`. There's no need to save this query. You can delete it and keep working in the same query tab.
# MAGIC
# MAGIC
# MAGIC #### SELECT on Delta Tables -- Updating the Table
# MAGIC
# MAGIC We are going to make two separate changes to the table. Then, we'll run a `DESCRIBE` command to see table details.
# MAGIC
# MAGIC 2. Run the SQL statements:
# MAGIC
# MAGIC   ```
# MAGIC   UPDATE customers SET loyalty_segment = 10 WHERE loyalty_segment = 0;
# MAGIC   UPDATE customers SET loyalty_segment = 0 WHERE loyalty_segment = 10;
# MAGIC   DESCRIBE HISTORY customers;
# MAGIC   ```
# MAGIC
# MAGIC   The code uses two UPDATE statements to make two changes to the table. We also reran our DESCRIBE HISTORY command and noted that the updates are noted in the log with new timestamps. All changes to a Delta table are logged in this way. After you run this query, you can delete the text and keep working in the same query tab.
# MAGIC
# MAGIC
# MAGIC #### SELECT on Delta Tables -- VERSION AS OF
# MAGIC
# MAGIC We can now use a special predicate for use with Delta tables: VERSION AS OF
# MAGIC
# MAGIC 3. Run the code below:
# MAGIC   ```
# MAGIC   SELECT loyalty_segment FROM customers VERSION AS OF 1;
# MAGIC   ```
# MAGIC   By using `VERSION AS OF`, we can `SELECT` from specific table versions. This feature of Delta tables is called "Time Travel," and it is very powerful.
# MAGIC
# MAGIC   We can also use `TIMESTAMP AS OF` to `SELECT` based on a table's state on a specific date and time, and you can find more information on this in the documentation.
# MAGIC
# MAGIC   Our select on the table returns the results from version 2 of the table, which had changed loyalty segments that were equal to 0 to 10. We see this reflected in the results set.
# MAGIC
# MAGIC
# MAGIC #### Restore a table to a past state
# MAGIC
# MAGIC If we want to restore a table to a previous version or timestamp, we can use the `RESTORE` command.
# MAGIC
# MAGIC 4. Run the code below:
# MAGIC
# MAGIC   ```
# MAGIC   RESTORE TABLE customers TO VERSION AS OF 1;
# MAGIC   DESCRIBE HISTORY customers;
# MAGIC   ```
# MAGIC
# MAGIC   After running the code, we can see the restore added to the log, and if we ran a `SELECT` on the table, it would contain the state that it had in version 1.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo - Data Visualizations and Dashboards
# MAGIC ---
# MAGIC
# MAGIC In this lesson, you'll visualize the data in your tables and build a dashboard that you can share.
# MAGIC
# MAGIC ---
# MAGIC ### Lesson Objective
# MAGIC ---
# MAGIC
# MAGIC At the end of this lesson, you will be able to:
# MAGIC
# MAGIC *   Identify dashboards as an intelligent, in-platform visualization tool for the Lakehouse.
# MAGIC *   Create basic, schema-specific visualizations using Databricks SQL.
# MAGIC *   Compute and display summary statistics using data visualizations.
# MAGIC *   Interpret summary statistics within data visualizations for business reporting purposes.
# MAGIC *   Describe how to create customized visualizations.
# MAGIC *   Explore the available visualization types.
# MAGIC *   Create customized data visualizations to aid in data storytelling.
# MAGIC
# MAGIC ---
# MAGIC ### The Counter
# MAGIC ---
# MAGIC
# MAGIC The Counter visualization displays a single number by default, but it can also be configured to display a goal number. In this example, we are going to configure a sum of completed sales, along with a **Sales Goal**. The query calculates a sum of total sales and also provides a hard-coded sales goal column.
# MAGIC
# MAGIC Complete the following:
# MAGIC
# MAGIC 1. Open a new query tab, make sure your catalog and schema are selected, and run the query below:
# MAGIC
# MAGIC   ```
# MAGIC   SELECT
# MAGIC     sum(total_price) AS Total_Sales,
# MAGIC     3000000 AS Sales_Goal
# MAGIC   FROM
# MAGIC     sales;
# MAGIC   ```
# MAGIC
# MAGIC 2. Save the query as **Count Total Sales**. Make sure the query is saved to the queries folder you set up previously.
# MAGIC 3. Click the **+** button in the results panel. Then click **Visualization** to add a new visualization for this query.
# MAGIC 4. Select **Counter** as the visualization type.
# MAGIC 5. For **Label** type **Total sales**.
# MAGIC 6. For the **Value Column** make sure the column **Total_Sales** is selected.
# MAGIC 7. For **Target Column** select **Sales goal**.
# MAGIC
# MAGIC   Note that we can configure the counter to count rows for us if we do not aggregate our data in the query itself.
# MAGIC
# MAGIC 8. Click the **Format** tab.
# MAGIC 9. Optional: Change the decimal character and thousands separator.
# MAGIC 10. **Total Sales** is a dollar figure, so add **$** to **Formatting String Prefix**.
# MAGIC 11. Turn the switch, **Format Target Value**, to on.
# MAGIC 12. Click **Save** in the lower-right corner.
# MAGIC 13. Click the name of the visualization (the name of the tab) and change the name to **Total Sales**.
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC ### The Combo Chart
# MAGIC ---
# MAGIC
# MAGIC Databricks SQL supports a variety of customization options to make charts look beautiful. In this example, we will start with a bar chart and then add a line chart, essentially turning it into a combo chart.
# MAGIC
# MAGIC Complete the following:
# MAGIC
# MAGIC 1.  Open a new query tab, check that your catalog and schema are selected, and run the query below:
# MAGIC
# MAGIC   ```
# MAGIC   SELECT
# MAGIC     customer_name,
# MAGIC     total_price AS Total_Sales,
# MAGIC     date_format(order_date, "MM") AS Month,
# MAGIC     product_category
# MAGIC   FROM
# MAGIC     sales
# MAGIC   WHERE
# MAGIC     order_date >= to_date('2019-08-01')
# MAGIC   AND order_date <= to_date('2019-10-31');
# MAGIC   ```
# MAGIC
# MAGIC 2.  Save the query as **Sales over three months**
# MAGIC
# MAGIC 3. Click the **+** button then **Visualization** in the results panel to add a new visualization for this query.
# MAGIC 4. Select **Bar** as the visualization type.
# MAGIC 5. For **X Column** choose **Month**.
# MAGIC 6. For **Y Columns** click **Add column** and select **Total Sales** and **Sum**.
# MAGIC 7. Click **Add column** again and select **Total Sales** and **Count**.
# MAGIC 8. Click the **Y-Axis** tab. Make the following changes:
# MAGIC   - Change the Scale to **Logarithmic**.
# MAGIC   - Type **Dollars** in the **Name** field (Left Y-Axis).
# MAGIC 9. Click the **Series** tab and type **Total Sales** in the first **Label** field.
# MAGIC 10. Type **Number of Sales** in the second **Label** field and change **Type** to **Line**.
# MAGIC 11. Click **Save** in the lower-right corner.
# MAGIC 12. Click the name of the visualization (the name of the tab) and change the name to **Sales by month**.
# MAGIC 13. Make sure the query is saved.
# MAGIC
# MAGIC ---
# MAGIC ### Add visualizations to a dashboard
# MAGIC ---
# MAGIC
# MAGIC You can create a dashboard based on the queries and visualizations you made in the SQL editor. The first iteration of <DBSQL> dashboards are now called legacy dashboards. The latest version of dashboards features an enhanced visualization library and a streamlined configuration experience so that you can quickly transform data into shareable insights.
# MAGIC
# MAGIC Dashboards have the following components:
# MAGIC
# MAGIC - Data: The **Data** tab allows you to define datasets that you will use in the dashboard. Datasets are bundled with dashboards when sharing, importing, or exporting them using the UI or API.
# MAGIC
# MAGIC - Canvas: The **Canvas** tab allows users to create visualizations and construct their dashboards. Each item on the canvas is called a widget. Widgets have three types: visualizations, text boxes, and filters.
# MAGIC
# MAGIC In this part of the lesson, you will create a new dashboard from your existing visualizations and then add more visualizations to it. Keep working in the SQL editor for now. Follow the instructions to guide you through the steps of creating a new dashboard from the SQL editor. 
# MAGIC
# MAGIC 1. Click the down-caret in the **Sales by month** visualization tab. Then click **Add to dashboard**.
# MAGIC 2. The dialog that appears references your saved query, **Sales over three months**.
# MAGIC
# MAGIC   - Change the name to **Retail organization**
# MAGIC   - Select the **Sales by month** visualization. You do not need to select the table visualization.
# MAGIC   - Click **Create**.
# MAGIC
# MAGIC Note: The **Add to dashboard** button converts the existing visualization and query to a canvas widget associated with a dataset. In this case, your saved query, **Sales over three months**, becomes a dataset for your new dashboard. This opens in a new authoring environment. 
# MAGIC
# MAGIC 3. A new dashboard canvas appears with a widget that shows an error message. The error message prompts you to adjust your dataset. This is expected behavior. Click the **Data** tab to update your dataset. The editor on your dataset tab shows your original query. 
# MAGIC
# MAGIC   Previously, you used UI selectors to set the catalog and schema for your queries.  For datasets in a dashboard, use the full three-level namespace, `<catalog.schema.table>`, to identify your table.
# MAGIC
# MAGIC 4. Delete the table name, `sales`, from the query in the editor.
# MAGIC 5. Without leaving the **Data** tab, click the **Catalog** icon to browse your available schemas and tables. Do not click on the sidebar menu. Make sure to stay within the **Data** tab.
# MAGIC 6. Find your catalog name in the catalog list. You can type to filter if necessary.
# MAGIC 7. Expand the **dawd_v2** catalog to show the tables you created.
# MAGIC 8. To the right of the **sales** table, click **>>** to automatically insert the table name into your query. It should look like: `labuser6429341_Catalog.dawd_v2.sales`. Make sure that the table name has been inserted into the correct place in your query, after the `FROM` clause.
# MAGIC 9. Click **Run**.
# MAGIC 10. Click the **Canvas** tab to go back to your dashboard canvas. The error messages in your visualization has been resolved and the widget now shows the chart you created.
# MAGIC
# MAGIC #### Adjust the visualization
# MAGIC
# MAGIC The converted visualization needs a couple of manual adjustments. When a widget is selected, its visualization configuration panel appears on the right side of the screen.
# MAGIC
# MAGIC Change the following settings in the configuration panel: 
# MAGIC
# MAGIC 11. Click the <b> ( ⋮ ) </b> kebab menu to the right of **Y axis**.
# MAGIC
# MAGIC 12. Click the checkbox to turn on **Enable dual axis**.
# MAGIC
# MAGIC Enabling dual axis allows us to see changes in the data on two different scales. Note that the trend captured in this data is much easier to visually detect than in the previous graph. You can see that the number of sales in August and October was low, but the dollar amounts were high. The opposite is true in September.
# MAGIC
# MAGIC ---
# MAGIC ### Add more datasets
# MAGIC ---
# MAGIC
# MAGIC You can use multiple datasets in a single dashboard. The datasets can be created from SQL queries or selected from a list of available Unity Catalog tables. Let's go ahead and add the tables you already created to this dashboard.
# MAGIC
# MAGIC 1. Click the **Data** tab. Then, click the **Dataset list** icon above the **Catalog** icon.
# MAGIC
# MAGIC 2. Click **Select a table**.
# MAGIC
# MAGIC 3. Use the **Catalog** and **Schema** drop-downs to select the catalog and schema you've been working with in this lesson.
# MAGIC
# MAGIC 4. Click <b>sales</b> to add it as a dataset, and then select the <b>Confirm</b> button. Note that it appears in your dataset list. 
# MAGIC
# MAGIC 5. Repeat these steps to add the **customers** tables.
# MAGIC
# MAGIC Note that each table is added to the list, and the query editor is automatically populated with a `SELECT *` statement for each table. You can modify that SQL query to alter the dataset.
# MAGIC
# MAGIC ---
# MAGIC ### Add charts using Databricks Assistant
# MAGIC ---
# MAGIC
# MAGIC Databricks Assistant works as an AI-based companion pair-programmer to make you more efficient as you create notebooks, queries, visualizations, and files. See <a href="https://docs.databricks.com/en/notebooks/databricks-assistant-faq.html" target="_blank">What is Databricks Assistant</a> for more details.
# MAGIC
# MAGIC When drafting a dashboard, you can provide a natural language prompt to the Databricks Assistant and it autogenerates a chart based on your request. The Databricks Assistant can help you build charts based on any dataset defined in your dashboard's data tab. Let's try it out.
# MAGIC
# MAGIC Complete the following steps:
# MAGIC
# MAGIC 1. Click the **Canvas** tab.
# MAGIC 2. Click the **Visualization** icon in the toolbar and use your mouse to place it on the canvas.
# MAGIC 3. In the text field at the top of your widget, enter the following prompt:
# MAGIC
# MAGIC   ```
# MAGIC   Create a bar chart with product_category on the x-axis and average total_sales on the y-axis.
# MAGIC   ```
# MAGIC
# MAGIC 4. Click **Submit** to generate a response. It may take a moment for the Assistant to provide a visualization.
# MAGIC 5. You should have a bar chart that matches the provided description. Click **Accept**.
# MAGIC
# MAGIC   If the visualization does not match your description or match the kind of visualization you wanted to create, you can reject or regenerate the response. You can also adjust the configuration of the chart.
# MAGIC
# MAGIC
# MAGIC #### Use the configuration panel
# MAGIC
# MAGIC Let's create a stacked bar chart by adding another measure on the y-axis.
# MAGIC
# MAGIC 6. Click the widget you just created to expose the configuration panel. If necessary, choose the dataset to **Sales over three months**.
# MAGIC 7. Click the **+** to the right of where it says, **Y axis**, then click **Total_sales**. A **SUM** transformations is automatically applied.
# MAGIC 8. Click the label **SUM(Total_sales)**. In the **Transform** area, choose **MIN**. Your new chart shows a stacked bar chart.
# MAGIC 9. Click the <b> ( ⋮ ) </b> kebab menu to the right of **Y axis**. A y-axis configuration menu appears. Check the box next to **Enable dual axis**. 
# MAGIC
# MAGIC Note that as you entered each choice of column in the axis settings, a transformation was applied automatically. You can click on the field name to remove the transformation or change to a different transformation.  
# MAGIC
# MAGIC You now have a dual-axis bar chart that shows each value according to its own scale. This visualization shows that, although the **Reagate** category has the highest minimum sales figure, it also has the lowest average total sales.
# MAGIC
# MAGIC ---
# MAGIC ### Create a visualization: Scatterplot
# MAGIC ---
# MAGIC You can also create charts without using the Databricks Assistant. Let's create a scatterplot that can help us understand how sales values vary with different order dates.
# MAGIC
# MAGIC
# MAGIC To make a scatterplot, complete the following steps:
# MAGIC
# MAGIC 1. Click the **Visualization** icon in the toolbar and use your mouse to place it on the canvas.
# MAGIC 2. With the new widget selected, view the configuration panel. Enter the following settings:
# MAGIC   - **Dataset**: sales
# MAGIC   - **Visualization**: Scatter
# MAGIC   - **X axis**: order_date
# MAGIC   - **Y axis**: total_price
# MAGIC 3. Click the field name under **X axis** (order_date) and adjust the settings as follows:
# MAGIC   - Set the **Scale Type** as **Temporal**.
# MAGIC   - Set the **Transform** method as **DAILY**.
# MAGIC 4. Click the field name under **Y axis** (total_price) and, if necessary, adjust the settings as follows:
# MAGIC   - Set the **Scale Type** as **Quantitative**.
# MAGIC   - Set the **Transform** method as **SUM**.
# MAGIC
# MAGIC ---
# MAGIC ### Add a text box
# MAGIC ---
# MAGIC Let's add a name and description to your draft dashboard. When you add a new widget to the canvas, other widgets automatically move to accommodate your placement. You can use your mouse to move and resize widgets. To delete a widget, select it and then press your delete key.
# MAGIC
# MAGIC Complete the following steps to add a text box:
# MAGIC
# MAGIC 1. Click the **Add a text box** icon and drag the widget to the top of your canvas.
# MAGIC 2. Type: `# Retail organization`
# MAGIC     Note: Text boxes use markdown. The `#` character in the included texts indicates that **Retail organization** is a level 1 heading. See <a href="https://www.markdownguide.org/basic-syntax/" target="_blank">this markdown guide</a> for more on basic markdown syntax.
# MAGIC
# MAGIC ---
# MAGIC ### Publish and share your dashboard
# MAGIC ---
# MAGIC
# MAGIC Great work! Your draft dashboard is coming together. Let's publish it so that you can share these insights.
# MAGIC
# MAGIC Published dashboards can be shared with other users in your workspace and with users registered at the account level. That means that users registered to your Databricks account, even if they have not been assigned workspace access or compute resources, can be assigned access to your dashboards.
# MAGIC
# MAGIC When you publish a dashboard, the default setting is to embed credentials. Embedding credentials in your published dashboard allows dashboard viewers to use your credentials to access the data and power the queries that support it. If you choose not to embed credentials, dashboard viewers use their own credentials to access necessary data and compute power. If a viewer does not have access to the default SQL warehouse that powers the dashboard, or if they do not have access to the underlying data, visualizations will not render.
# MAGIC
# MAGIC To publish your dashboard, complete the following steps:
# MAGIC
# MAGIC 1. Click **Publish** in the upper-right corner of your dashboard. Read the setting and notes in the **Publish** dialog.
# MAGIC 2. Click **Publish** in the lower-right corner of the dialog. The **Sharing** dialog opens.
# MAGIC     - You can use the text field at the top of the dialog to search for individual users you might want to share with, or share with a preconfigured group, like **Admins** or **All workspace users**. In this space, you can grant leveled privileges like **Can Manage** or **Can Edit**. We'll revisit these settings later in the class. See <a href="https://docs.databricks.com/en/security/auth-authz/access-control/index.html#lakeview" target="_blank">Dashboard ACLs</a> for details on permissions.
# MAGIC     - The bottom of the **Sharing** dialog controls view access. Use this setting to easily share with all account users.
# MAGIC 3. Under **Sharing settings**, choose **Anyone in my account can view** from the drop-down. Then, close the **Sharing** dialog.
# MAGIC 4. Use the drop-down near the top of the dashboard to switch between **Draft** and **Published** versions of your dashboard.
# MAGIC
# MAGIC Note: When you edit your draft dashboard, viewers of the published dashboards do not see your changes until you republish. The published dashboard includes visualizations that are built on queries that can be refreshed as new data arrives. Dashboards are updated with new data automatically without republishing.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab - Data Visualizations and Dashboards
# MAGIC ---
# MAGIC ### Lesson Objective
# MAGIC ---
# MAGIC
# MAGIC At the end of this lesson, you will be able to:
# MAGIC
# MAGIC *   Create additional customized visualizations in a dashboard.
# MAGIC
# MAGIC ---
# MAGIC ### Create Tables and Load Data for this lesson
# MAGIC ---
# MAGIC
# MAGIC Complete the following steps:
# MAGIC
# MAGIC 1. Click **New** > **Query** to open a new query.
# MAGIC 2. Ensure your catalog and the schema, **dawd_v2**, are selected in the drop-downs above the query editor.
# MAGIC 3. Run the following query:
# MAGIC
# MAGIC   ```
# MAGIC   DROP TABLE IF EXISTS sales_orders;
# MAGIC   CREATE TABLE sales_orders AS
# MAGIC   SELECT
# MAGIC     *
# MAGIC   FROM
# MAGIC   delta.`wasbs://courseware@dbacademy.blob.core.windows.net/data-analysis-with-databricks/v03/retail-org/sales_orders/sales_orders_delta`;
# MAGIC
# MAGIC   ```
# MAGIC
# MAGIC After your new table is created, close the query tab. You do not have to save this query.
# MAGIC
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add new datasets to your dashboard
# MAGIC ---
# MAGIC
# MAGIC Now it's time to prepare more data for your dashboard by adding a table as a dataset.
# MAGIC
# MAGIC 1. Click **Dashboards** in the sidebar menu to get to the dashboards listing page. By default, the page shows a list of dashboards that you own.
# MAGIC 2. Click **Retail organization** to open the dashboard you created in the previous section. The last published version of the dashboard opens.
# MAGIC 3. Use the drop-down near the top of the page to access the draft dashboard.
# MAGIC 4. Click the **Data** tab.
# MAGIC 5. Click **Select a table** to add a new table.
# MAGIC 6. Select your catalog name and the **dawd_v2** schema.
# MAGIC 7. Click <b>sales_orders</b> to add it as a dataset, and then select the <b>Confirm</b> button..
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC ### Create a pie chart
# MAGIC ---
# MAGIC
# MAGIC Let's build a pie chart that shows the proportion of sales coming from each product category.
# MAGIC
# MAGIC Complete the following:
# MAGIC
# MAGIC 1. Click the **Canvas** tab.
# MAGIC 2. Add a visualization widget to the canvas.
# MAGIC 3. Apply the following configuration settings:
# MAGIC   - **Dataset**: sales
# MAGIC   - **Visualization**: Pie
# MAGIC   - **Angle**: product_category. Apply the **COUNT** transformation. 
# MAGIC   - **Color/Group by**: product_category
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC ### Edit dataset queries
# MAGIC ---
# MAGIC
# MAGIC The next two charts require data from two different tables. We can use a `JOIN` in the query editor on the **Data** tab to create a dataset that includes all necessary values. Datasets can be created using common table expressions and joins.
# MAGIC
# MAGIC #### New dataset 1: Sales by loyalty segment
# MAGIC
# MAGIC This query creates a dataset that joins data from the customers and sales tables. It connects a customer's spending amount with their loyalty program status.
# MAGIC
# MAGIC Complete the following steps to create this dataset:
# MAGIC
# MAGIC 1. Click the **Data** tab.
# MAGIC 2. Click **Create from SQL**.
# MAGIC 3. Double-click the title of the new dataset and replace the default name, **Untitled**, with **Sales by loyalty segment**
# MAGIC 4. Paste the following query into the editor:
# MAGIC
# MAGIC   ```
# MAGIC   SELECT
# MAGIC     product_category,
# MAGIC     loyalty_segment,
# MAGIC     total_price
# MAGIC   FROM
# MAGIC     <catalog.schema.table>  -- Use the sales table
# MAGIC     JOIN <catalog.schema.table> -- Use the customers table 
# MAGIC     on sales.customer_id = customers.customer_id  
# MAGIC
# MAGIC   ```
# MAGIC 5. Before you run the query, replace <catalog.schema.table> with the appropriate table name as indicated in the comments.
# MAGIC
# MAGIC 6. Run the query.
# MAGIC
# MAGIC #### New dataset 2: Sales and orders by day
# MAGIC
# MAGIC This query creates a dataset that shows the day of the month a purchase was made, the total sales for that day, and the number of orders in that group. It pulls data from the `sales` and `sales_order` tables.
# MAGIC
# MAGIC Complete the following steps to add the next dataset:
# MAGIC
# MAGIC 1. Click **Create from SQL**.
# MAGIC 2. Double-click the title of the new dataset and replace the default name, **Untitled**, with **Sales and orders by day**
# MAGIC 3. Paste the following query into the editor:
# MAGIC
# MAGIC ```
# MAGIC WITH sales_data AS (
# MAGIC   SELECT
# MAGIC     date_format(order_date, "dd") AS day,
# MAGIC     SUM(total_price) AS total_sales
# MAGIC   FROM <catalog.schema.table>  -- Use sales table
# MAGIC   GROUP BY day
# MAGIC   ),
# MAGIC orders_data AS (
# MAGIC   SELECT
# MAGIC     CASE 
# MAGIC       WHEN sales_orders.order_datetime <> '' 
# MAGIC       THEN DAY(FROM_UNIXTIME(sales_orders.order_datetime))
# MAGIC       ELSE NULL 
# MAGIC     END as day,
# MAGIC     COUNT(order_number) AS total_orders
# MAGIC   FROM <catalog.schema.table> -- Use sales_orders table
# MAGIC   GROUP BY day
# MAGIC )
# MAGIC SELECT
# MAGIC   cast(s.day as INT),
# MAGIC   s.total_sales,
# MAGIC   o.total_orders
# MAGIC FROM sales_data s
# MAGIC JOIN orders_data o ON s.day = o.day
# MAGIC ORDER BY s.day;
# MAGIC
# MAGIC ```
# MAGIC 4. Before you run the query, replace <catalog.schema.table> with the appropriate table name as indicated in the comment.
# MAGIC
# MAGIC 5. Run the query.
# MAGIC
# MAGIC ---
# MAGIC ### Create additional visualizations
# MAGIC ---
# MAGIC Now, let's use those new datasets to create new visualizations.
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC #### Create a Heatmap
# MAGIC ---
# MAGIC
# MAGIC Heatmaps are helpful for understanding patterns around the occurrence of certain events. The heatmap you're about to create shows data broken down by the customer's assigned loyalty segment and product category. The colors in the chart show the total amount spent in each category.
# MAGIC
# MAGIC To create a heat map, complete the following steps:
# MAGIC
# MAGIC 1. Click the **Canvas** tab.
# MAGIC 2. Add a visualization widget to the canvas.
# MAGIC 3. Apply the following configuration settings:
# MAGIC     - **Dataset**: Sales by loyalty segment
# MAGIC     - **Visualization**: Heatmap
# MAGIC     - **X axis**: product_category
# MAGIC     - **Y axis**: loyalty_segment
# MAGIC     - **Color by**: SUM(total_price)
# MAGIC 4. Resize and arrange the widget on your dashboard.
# MAGIC 5. The default size for this widget is too small to show all of the product categories. Hover over the widget's edges and adjust the chart's size. Hover over other areas of the chart until you see a hand icon. Click and drag to move your chart around the canvas.
# MAGIC
# MAGIC Great! For this data, it looks like customers in loyalty segment 3 spend the most overall. For product categories **Reagate** and **Zamaha**, loyalty segments are slower to catch on and don't seem very related to customer spend.
# MAGIC
# MAGIC ---
# MAGIC #### Dual-axis line chart
# MAGIC ---
# MAGIC
# MAGIC Dual-axis line charts, like dual-axis bar charts, can be useful for comparing related quantities on different scales. The chart you're about to create tracks proceeds from sales and the number of orders placed. The data is grouped by the day of the month that the sale occurred. This can be useful for tracking customer spending patterns throughout the month.
# MAGIC
# MAGIC To create your dual-axis line chart, complete the following:
# MAGIC
# MAGIC 1. Add a visualization widget to the canvas.
# MAGIC 2. Apply the following configuration settings:
# MAGIC   - **Dataset**: Sales and orders by day
# MAGIC   - **Visualization**: Line
# MAGIC   - **X axis**: day (Set transformation to **None**)
# MAGIC   - **Y axis**: SUM(total_sales)
# MAGIC   - **Y axis**: SUM(total_orders)
# MAGIC   This creates your basic chart, but you must enable the dual axis to show the quantities on their respective scales.
# MAGIC 3. Click the <b> ( ⋮ ) </b> kebab menu to the right of the Y axis. Then, click the checkbox to turn on **Enable dual axis**.
# MAGIC 4. Resize and arrange the widget on your dashboard.
# MAGIC
# MAGIC ---
# MAGIC ### Republish your dashboard
# MAGIC ---
# MAGIC 1. Click **Publish** to create a shareable copy of your revised dashboard.
# MAGIC 2. Use the switcher to view your published dashboard.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo - Create Interactive Dashboards
# MAGIC ---
# MAGIC
# MAGIC The dashboard you created in the lab is good for reporting, and viewers can use it to stay up-to-date on the most recent retail sales figures. However, the viewer has no controls that allow them to further explore the data. For example, if a user wants to see the data for a specific period, they must contact the dashboard author to request changes.
# MAGIC
# MAGIC You can create user controls that allow the viewer to filter certain data based on a field or a parameter value. Filters are widgets that allow dashboard viewers to narrow down results by filtering on specific fields or setting dataset parameters. 
# MAGIC
# MAGIC ---
# MAGIC ### Lesson Objectives
# MAGIC ---
# MAGIC
# MAGIC At the end of this lesson, you will be able to:
# MAGIC
# MAGIC *   Create and use datasets and dashboards with field filters and parameters to allow stakeholders to customize results and visualizations.
# MAGIC *   Share queries and dashboards with stakeholders with appropriate permissions.
# MAGIC *   Organize queries and dashboards in the Databricks workspace browser.
# MAGIC
# MAGIC ---
# MAGIC ### Using field filters
# MAGIC ---
# MAGIC
# MAGIC Filters can be applied to fields of one or more datasets. Filters on fields allow users to focus on certain values, or ranges of values in the data. The filter applies to all visualizations built on the selected datasets.
# MAGIC
# MAGIC Complete the following:
# MAGIC
# MAGIC 1. Click **Dashboards** in the sidebar menu.
# MAGIC 2. Click **Retail organization** to open the last published copy of your dashboard.
# MAGIC 3. Use the drop-down at the top of your dashboard to get to the draft version.
# MAGIC 4. Click the **Filter** icon in the toolbar near the bottom of the canvas. Place the widget near the top of your dashboard, under your text box.
# MAGIC   When the filter widget is selected, the filter configuration panel appears.
# MAGIC 5. Apply the following settings:
# MAGIC   - **Filter**: Single value
# MAGIC   - **Fields**: 
# MAGIC       - sales.product_category 
# MAGIC       - sales over three months.product_category 
# MAGIC       - sales by loyalty segment.product_category 
# MAGIC 6. Use the checkboxes to turn on **Title**.
# MAGIC 7. Double-click the title on the widget and change it to **Product category**
# MAGIC 8. Use the drop-down in the filter widget to test your filter.
# MAGIC
# MAGIC Note: The filter applies to each selected dataset in the filter configuration panel. All of the datasets you selected share the same range of values for product_category. A dashboard viewer can select from that list when choosing which data to filter on the dashboard.
# MAGIC
# MAGIC You can also use parameters to create interactive dashboards. Parameters allow users customize visualizations by substituting values into dataset queries at runtime. See <a href="https://docs.databricks.com/en/dashboards/parameters.html" target="_blank">What are dashboard parameters?</a> to learn more.
# MAGIC
# MAGIC ---
# MAGIC ### Republish your dashboard
# MAGIC ---
# MAGIC 1. Click **Publish** to republish your dashboard.
# MAGIC
# MAGIC ---
# MAGIC ### Organizing workspace objects
# MAGIC ---
# MAGIC
# MAGIC Queries, alerts, and dashboards can be organized in a folder structure to help you keep track of where your work is located.
# MAGIC
# MAGIC 1.  Click **Workspace** in the sidebar menu.
# MAGIC
# MAGIC     You are taken to your home folder, where you can see the dashboard we created in this lesson and any other workspace objects that you have saved.
# MAGIC
# MAGIC 2.  Click **Create**.
# MAGIC 3.  Review the types of items you can create.
# MAGIC 4.  Select **Folder** from the available options.
# MAGIC 5.  Name the folder something unique, such as **My Great Project** and click **Create** when finished.
# MAGIC 6.  You will be taken to the contents of your new folder, so return to your home folder.
# MAGIC 7.  Drag and drop your dashboard and other objects from this lesson into the new folder.
# MAGIC 8.  Click the folder name to open it, and verify that the dashboard has been moved successfully by checking its presence within the folder.
# MAGIC
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo - Dashboards SQL in Production
# MAGIC ---
# MAGIC
# MAGIC ---
# MAGIC ### Lesson Objectives
# MAGIC ---
# MAGIC
# MAGIC At the end of this lesson, you will be able to:
# MAGIC
# MAGIC * Describe scheduling tools that you can use to update data and reports automatically.
# MAGIC * Set a refresh schedule for a dashboard.
# MAGIC * Set a refresh schedule for a query.
# MAGIC * Explain how to set query alerts.
# MAGIC * Describe workspace object permissions required to complete certain tasks.
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC ### Securely sharing workspace objects
# MAGIC ---
# MAGIC
# MAGIC ---
# MAGIC ### Managing workspace object permissions
# MAGIC ---
# MAGIC
# MAGIC Dashboards, queries, and alerts are all workspace objects. You can grant access to workspace objects that you own and collaborate with colleagues who have invited you to edit objects they own. In this section, we'll review how to share workspace objects and the available access levels.
# MAGIC
# MAGIC As an example, let's share a dashboard with other members of your team:
# MAGIC
# MAGIC 1.  Select **Dashboards** from the sidebar menu.
# MAGIC 2.  Choose the dashboard that you want to share. The dashboard opens.
# MAGIC 3.  Select **Share** from the upper right corner.
# MAGIC
# MAGIC   The **Sharing** dialogue appears. If you do not have permission to share the dashboard, all the options will be grayed out and uneditable by you. As the owner of the dashboard, you have “Can manage” permissions. You can share the query with users and groups who are configured in your workspace. These users and groups can have “Can Manage”, “Can Edit,” “Can Run,” or “Can View” permissions. Those with “Can Edit” permissions can also run and publish the dashboard.
# MAGIC
# MAGIC   Note: When you published your dashboard earlier in this lesson, you were instructed to publish it with embedded credentials. If you change that publish setting to **Don't embed credentials**, viewers are limited to their own permissions which mean that they may not be able to access the SQL warehouse that powers the underlying queries or the data from the underlying datasets.
# MAGIC
# MAGIC 4.  Select the drop-down box that reads **Type to add multiple users or groups.** You will be prompted with suggestions; instead, you can type a user or group name to make your selection. Select **All workspace users** for this example.
# MAGIC 5.  Toggle the option for “Can Manage” to “Can Edit,” “Can Run,” or “Can View.”
# MAGIC 6.  Click “Add” to close the dialogue box.
# MAGIC 7.  Close the Sharing dialogue by clicking the **X** in the upper right corner.
# MAGIC
# MAGIC ---
# MAGIC ### Schedule automatic updates
# MAGIC ---
# MAGIC
# MAGIC We will review how to set automatic updates for the following workspace objects:
# MAGIC
# MAGIC * Dashboards
# MAGIC * Queries
# MAGIC * Alerts
# MAGIC
# MAGIC These updates are configured within Databricks SQL and are independent of any automations in the rest of the Lakehouse, meaning, they can affect data anywhere in the Lakehouse, but they do not use Workflows or Jobs. They use SQL warehouses for compute resources.
# MAGIC
# MAGIC ---
# MAGIC ### Query Refresh Schedule
# MAGIC ---
# MAGIC
# MAGIC You can use scheduled query runs to keep your dashboards updated or to enable routine alerts. Let's set a schedule on one of your saved queries.
# MAGIC
# MAGIC 1. Click **Queries** in the sidebar menu to go to the queries listing page. Your queries landing page opens to a list of your own queries.
# MAGIC 2. Click **Sales over three months**. The query opens in the SQL editor.
# MAGIC 3. Click **Schedule** next to **Save**.
# MAGIC 4. Click **Add Schedule** from the drop-down.
# MAGIC 5. Configure the schedule for every hour at 0 minutes past the hour, and confirm your timezone from the drop-down.
# MAGIC 6. Click **Create** to confirm the schedule for the query.
# MAGIC
# MAGIC     WARNING: If the refresh rate is less than the SQL warehouse 'Auto Stop' parameter, the warehouse will run indefinitely.
# MAGIC
# MAGIC ---
# MAGIC ### Alerts
# MAGIC ---
# MAGIC
# MAGIC Similar to queries and dashboards, alerts can be organized within a folder structure to help you keep track of their location. Alerts allow you to configure notifications when a field returned by a scheduled query meets a specific threshold. Although we just configured a refresh schedule for our query, the alert runs on its own schedule.
# MAGIC
# MAGIC To create an Alert:
# MAGIC
# MAGIC 1.  Click **Alerts** in the sidebar menu.
# MAGIC 2.  Click **Create Alert**.
# MAGIC 3.  On the resulting screen, name your alert.
# MAGIC 4.  From the **Query** drop-down, select the query made earlier, **Sales over three months**.
# MAGIC 5.  Set the trigger conditions by adjusting the following: in the **Value column**, select **Total_Sales** and **Sum**, in the **Operator** column, select the `>` (greater than) operator, and modify the **Threshold value** to **2000000**
# MAGIC 6.  Click **Create alert** at the bottom.
# MAGIC
# MAGIC   The alert is triggered when the count of the top row in the query's results is greater than or equal to 2,000,000. The actual value of the sum of that column is 2,550,552. The next time your query runs, it will trigger an alert.
# MAGIC
# MAGIC   Alerts require a destination to be set for anyone to receive the notification when it is triggered, including the person who created the alert. To do this, we'll add a schedule to check the alert and set destinations for the notification if the alert triggers.
# MAGIC
# MAGIC 7.  Select the **Add Schedule** text on the screen.
# MAGIC 8.  Set the schedule for every 2 minutes at 0 minutes past the hour.
# MAGIC 9.  Click the **Destinations** tab.
# MAGIC 10. Turn the **Notify destinations when the alert is triggered** option on. 
# MAGIC 11.  Use the search bar in that dialog to locate your user email address (this is your Vocareum lab username) and click on it to add it to the destinations list.
# MAGIC 12.  Click **Create** to close the dialog and save the schedule and destination selections.
# MAGIC
# MAGIC   Something to note regarding configuring Alerts and Refresh Schedules: Every time they run, the SQL warehouse will start (if it's stopped), run the query, and go into an idle state. Once the Auto Stop time has expired, the SQL Warehouse will stop. If the refresh schedule is set to a lower time limit than the SQL Warehouse's Auto Stop time, the Warehouse will never stop, which may increase costs.
# MAGIC
# MAGIC To clean up your queries and alerts, follow these steps to remove the alert and turn off the refresh schedule for the query.
# MAGIC
# MAGIC 1.  With the alert you just created open, you can select the <b> ( ⋮ ) </b> kebab menu menu and choose **Move to trash** from the drop-down menu. This action deletes the alert and renders it inactive.
# MAGIC 2.  Return to the Queries from the sidebar menu.
# MAGIC 3.  Select the Sales over three months query to open it back up.
# MAGIC 4.  Click **Schedule (1)** from the upper-right corner of the query screen.
# MAGIC 5.  From the <b> ( ⋮ ) </b> kebab menu menu, select “Delete” in the drop-down to remove the scheduled refresh from the query.
# MAGIC
# MAGIC ---
# MAGIC ### Refresh dashboards and share reports
# MAGIC ---
# MAGIC
# MAGIC You can set up scheduled updates to automatically refresh your dashboard and periodically send your subscribers emails with the latest data. Users with at least **Can Edit** permissions can create a schedule so published dashboards with embedded credentials run periodically.
# MAGIC
# MAGIC For each scheduled dashboard update, the following occurs:
# MAGIC - All SQL logic that defines datasets runs on the designated time interval.
# MAGIC - Results populate the query result cache and help to improve initial dashboard load time.
# MAGIC
# MAGIC If anyone is subscribed to your dashboard, they receive an email with a live dashboard link and a PDF snapshot of the current dashboard.
# MAGIC
# MAGIC  To create a schedule, complete the following steps:
# MAGIC
# MAGIC 1.  Click **Schedule** in the upper right corner of the dashboard you want to schedule for refresh.
# MAGIC 2.  Indicate the frequency at which you want the dashboard to refresh.
# MAGIC 3.  Set the timezone for your schedule if necessary.
# MAGIC 4. Click **Create**. 
# MAGIC 5. Click **Subscribe** to add yourself as a subscriber for dashboard updates. 
# MAGIC
# MAGIC 6.  Click the <b> ( ⋮ ) </b> kebab menu menu next to the **Subscribed** button and then click **Edit**.  
# MAGIC 7.  Click the **Subscribers** tab to add other users of notification destinations for this schedule.
# MAGIC
# MAGIC   WARNING: If the Dashboard refresh interval is less than the SQL Warehouse 'Auto Stop' parameter, the Warehouse will run indefinitely, impacting platform costs.

# COMMAND ----------


