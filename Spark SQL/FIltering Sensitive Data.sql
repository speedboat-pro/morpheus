-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Filter sensitive table data using row filters and column masks
-- MAGIC
-- MAGIC
-- MAGIC ##### Objectives
-- MAGIC 1. Explain row filters and column masks
-- MAGIC 1. Contrast these filters with dynamic views
-- MAGIC 1. Apply a row filter
-- MAGIC 1. Apply a column mask
-- MAGIC 1. Use mapping tables to create an access-control list
-- MAGIC 1. Explain support and limitations
-- MAGIC
-- MAGIC ## Introduction
-- MAGIC This notebook provides guidance and examples for using row filters, column masks, and mapping tables to filter sensitive data in your tables. These features require Unity Catalog.
-- MAGIC
-- MAGIC ### Before you begin
-- MAGIC
-- MAGIC To add row filters and column masks to tables, you must have:
-- MAGIC
-- MAGIC - A workspace that is enabled for Unity Catalog.
-- MAGIC - A function that is registered in Unity Catalog. This can be a SQL UDF, or a Python or Scala UDF that is registered in Unity Catalog and wrapped in a SQL UDF. For details, see [What are user-defined functions (UDFs)?](https://docs.databricks.com/en/udf/index.html), [Column mask clause](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-column-mask.html), and [ROW FILTER clause](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-row-filter.html).
-- MAGIC
-- MAGIC You must also meet the following requirements:
-- MAGIC
-- MAGIC - To assign a function that adds row filters or column masks to a table, you must have the `EXECUTE` privilege on the function, `USE SCHEMA` on the schema, and `USE CATALOG` on the parent catalog.
-- MAGIC - If you are adding filters or masks when you create a new table, you must have the `CREATE TABLE` privilege on the schema.
-- MAGIC - If you are adding filters or masks to an existing table, you must be the table owner or have both the `MODIFY` and `SELECT` privileges on the table.
-- MAGIC
-- MAGIC To access a table that has row filters or column masks, your compute resource must meet one of these requirements:
-- MAGIC
-- MAGIC - A SQL warehouse.
-- MAGIC - Shared access mode on Databricks Runtime 12.2 LTS or above.
-- MAGIC - Single-user access mode on Databricks Runtime 15.4 LTS or above (Public Preview).
-- MAGIC
-- MAGIC You cannot read row filters or column masks using single-user compute on Databricks Runtime 15.3 or below.
-- MAGIC
-- MAGIC To take advantage of the data filtering provided in Databricks Runtime 15.4 LTS and above, you must also verify that your workspace is enabled for serverless compute, because the data filtering functionality that supports row filters and column masks runs on serverless compute. You might therefore be charged for serverless compute resources when you use single-user compute to read tables that use row filters or column masks. See [Fine-grained access control on single-user compute.](https://docs.databricks.com/en/compute/single-user-fgac.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Row Filters
-- MAGIC Row filters allow you to apply a filter to a table so that queries return only rows that meet the filter criteria. You implement a row filter as a [SQL user-defined function (UDF)](https://docs.databricks.com/en/udf/index.html). Python and Scala UDFs are also supported, but only when they are wrapped in SQL UDFs.
-- MAGIC
-- MAGIC This example creates a SQL user-defined function that applies to members of the group admin in the region US.
-- MAGIC
-- MAGIC When this sample function is applied to the sales table, members of the admin group can access all records in the table. If the function is called by a non-admin, the RETURN_IF condition fails and the region='US' expression is evaluated, filtering the table to only show records in the US region.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Using Catalog Explorer
-- MAGIC 1. In your Databricks workspace, click **Catalog**.
-- MAGIC Browse or search for the table you want to filter.
-- MAGIC 1. On the **Overview** tab, click **Row filter: Add filter**.
-- MAGIC 1. On the **Add row filter** dialog, select the catalog and schema that contain the filter function, then select the function.
-- MAGIC 1. On the expanded dialog, view the function definition and select the table columns that match the columns included in the function statement.
-- MAGIC 1. Click **Add**.
-- MAGIC
-- MAGIC To remove the filter from the table, click **fx Row filter** and click **Remove**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Using SQL
-- MAGIC To create a row filter and then add it to an existing table, use CREATE FUNCTION and apply the function using ALTER TABLE. You can also apply a function when you create a table using CREATE TABLE.
-- MAGIC
-- MAGIC This example creates a SQL user-defined function that applies to members of the group admin in the region US.
-- MAGIC
-- MAGIC When this sample function is applied to the sales table, members of the admin group can access all records in the table. If the function is called by a non-admin, the RETURN_IF condition fails and the region='US' expression is evaluated, filtering the table to only show records in the US region.

-- COMMAND ----------

CREATE FUNCTION us_filter(region STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, region='US');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Apply the function to a table as a row filter. Subsequent queries from the sales table then return a subset of rows.
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE TABLE sales (region STRING, id INT);
ALTER TABLE sales SET ROW FILTER us_filter ON (region);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Disable the row filter. 
-- MAGIC
-- MAGIC Future user queries from the sales table then return all of the rows in the table.
-- MAGIC

-- COMMAND ----------

ALTER TABLE sales DROP ROW FILTER;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Using `CREATE TABLE`
-- MAGIC Create a table with the function applied as a row filter as part of the CREATE TABLE statement. Future queries from the sales table then each return a subset of rows.

-- COMMAND ----------

CREATE TABLE sales (region STRING, id INT)
WITH ROW FILTER us_filter ON (region);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Column masks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To apply a column mask, create a function (UDF) and then apply it to a table column.
-- MAGIC
-- MAGIC You can apply a column mask using Catalog Explorer or SQL commands. The Catalog Explorer instructions assume that you have already created a function and that it is registered in Unity Catalog. The SQL instructions include examples of creating a column mask function and applying it to a table column.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Using Catalog Explorer
-- MAGIC 1. In your Databricks workspace, click **Catalog**.
-- MAGIC 1. Browse or search for the table.
-- MAGIC 1. On the **Overview** tab, find the row you want to apply the column mask to and click the **Mask** edit icon.
-- MAGIC 1. On the **Add column mask** dialog, select the catalog and schema that contain the filter function, then select the function.
-- MAGIC 1. On the expanded dialog, view the function definition. If the function includes any parameters in addition to the column that is being masked, select the table columns that you want to cast those additional function parameters to.
-- MAGIC 1. Click **Add**.
-- MAGIC
-- MAGIC To remove the column mask from the table, click **fx Column mask** in the table row and click **Remove**.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Using SQL
-- MAGIC
-- MAGIC In this example, you create a user-defined function that masks the ssn column so that only users who are members of the HumanResourceDept group can view values in that column.

-- COMMAND ----------

CREATE FUNCTION ssn_mask(ssn STRING)
  RETURN CASE WHEN is_member('HumanResourceDept') THEN ssn ELSE '***-**-****' END;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Apply the new function to a table as a column mask. You can add the column mask when you create the table or after.

-- COMMAND ----------

--Create the `users` table and apply the column mask in a single step:

CREATE TABLE users (
  name STRING,
  ssn STRING MASK ssn_mask);


-- COMMAND ----------

CREATE TABLE users
  (name STRING, ssn STRING);

ALTER TABLE users ALTER COLUMN ssn SET MASK ssn_mask;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Queries on that table now return masked ssn column values when the querying user is not a member of the HumanResourceDept group:
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT * FROM users;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC To disable the column mask so that queries return the original values in the ssn column:

-- COMMAND ----------

ALTER TABLE users ALTER COLUMN ssn DROP MASK;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Use mapping tables to create an access-control list
-- MAGIC To achieve row-level security, consider defining a mapping table (or access-control list). Each mapping table is a comprehensive mapping table that encodes which data rows in the original table are accessible to certain users or groups. Mapping tables are useful because they offer simple integration with your fact tables through direct joins.
-- MAGIC
-- MAGIC This methodology proves beneficial in addressing many use cases with custom requirements. Examples include:
-- MAGIC - Imposing restrictions based on the logged-in user while accommodating different rules for specific user groups.
-- MAGIC - Creating intricate hierarchies, such as organizational structures, requiring diverse sets of rules.
-- MAGIC Replicating complex security models from external source systems.
-- MAGIC - By adopting mapping tables in this way, you can effectively tackle these challenging scenarios and ensure robust row-level and column-level security implementations.
-- MAGIC
-- MAGIC #### Mapping table examples
-- MAGIC Use a mapping table and custom UDF to check if the current user is in a list:

-- COMMAND ----------

USE CATALOG main;
DROP TABLE IF EXISTS valid_users;

CREATE TABLE valid_users(username string);
INSERT INTO valid_users
VALUES
  ('fred@databricks.com'),
  ('barney@databricks.com');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Note 
-- MAGIC **All filters run with definer’s rights except for functions that check user context (for example, the CURRENT_USER and IS_MEMBER functions) which run as the invoker.**
-- MAGIC
-- MAGIC In this example the function checks to see if the current user is in the valid_users table. If the user is found, the function returns true.
-- MAGIC
-- MAGIC

-- COMMAND ----------

DROP FUNCTION IF EXISTS row_filter;

CREATE FUNCTION row_filter()
  RETURN EXISTS(
    SELECT 1 FROM valid_users v
    WHERE v.username = CURRENT_USER()
);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC The example below applies the row filter during table creation. You can also add the filter later using an `ALTER TABLE `statement. When applying to a whole table use the `ON () `syntax. For a specific row use `ON (row)`;.
-- MAGIC
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS data_table;

CREATE TABLE data_table
  (x INT, y INT, z INT)
  WITH ROW FILTER row_filter ON ();

INSERT INTO data_table VALUES
  (1, 2, 3),
  (4, 5, 6),
  (7, 8, 9);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Select data from the table. This should only return data if the user is in the valid_users table.

-- COMMAND ----------

SELECT * FROM data_table;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create a mapping table comprising accounts that should always have access to view all the rows in the table, regardless of the column values:

-- COMMAND ----------

CREATE TABLE valid_accounts(account string);
INSERT INTO valid_accounts
VALUES
  ('admin'),
  ('cstaff');


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now create a SQL UDF that returns true if the values of all columns in the row are less than five, or if the invoking user is a member of the above mapping table.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE FUNCTION row_filter_small_values (x INT, y INT, z INT)
  RETURN (x < 5 AND y < 5 AND z < 5)
  OR EXISTS(
    SELECT 1 FROM valid_accounts v
    WHERE IS_ACCOUNT_GROUP_MEMBER(v.account));


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Finally, apply the SQL UDF to the table as a row filter:

-- COMMAND ----------

ALTER TABLE data_table SET ROW FILTER row_filter_small_values ON (x, y, z);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Support and limitations
-- MAGIC Row filters and column masks are not supported with all Databricks functionality or on all compute resources. This section lists supported functionality and limitations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Supported features and formats
-- MAGIC This list of supported functionality is not exhaustive. Some items are listed because they were unsupported during the Public Preview.
-- MAGIC - Databricks SQL and Databricks notebooks for SQL workloads are supported.
-- MAGIC - DML commands by users with MODIFY privileges are supported. Filters and masks are applied to the data that is read by UPDATE and DELETE statements and are not applied to data that is written (including INSERT).
-- MAGIC - Supported data formats:
-- MAGIC   - Delta and Parquet for managed and external tables.
-- MAGIC    - Multiple other data formats for foreign tables registered in Unity Catalog using Lakehouse Federation.
-- MAGIC - Policy parameters can include constant expressions (strings, numeric, intervals, booleans, nulls).
-- MAGIC - SQL, Python, and Scala UDFs are supported as row filter or column mask functions, as long as they are registered in Unity Catalog. Python and Scala UDFs must be wrapped in a SQL UDF.
-- MAGIC - You can create views on tables that reference column masks or row filters, but you cannot add column masks or row filters to a view.
-- MAGIC - Delta Lake change data feeds are supported as long as the schema is compatible with the row filters and column masks that apply to the target table.
-- MAGIC - Foreign tables are supported.
-- MAGIC - Table sampling is supported.
-- MAGIC - MERGE statements are supported when source tables, target tables, or both use row filters and column masks. This includes tables with row filter functions that contain simple subqueries, but there are limitations, listed in the section that follows.
-- MAGIC - Databricks [SQL materialized views](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view.html) and Databricks SQL [streaming tables](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html) support row filters and column masks (Public Preview):
-- MAGIC   - You can add row filters and column masks to a Databricks SQL materialized view or streaming table.
-- MAGIC   - You can define Databricks SQL Materialized views or streaming tables on tables that include row filters and column masks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Performance Considerations
-- MAGIC Row filters and column masks provide guarantees on the visibility of your data by ensuring that no users may view the contents of values of the base tables prior to filtering and masking operations. They are engineered to perform well in response to queries under most common use cases. In less frequent applications, where the query engine must choose between optimizing query performance and protecting against leaking information from the filtered/masked values, it will always make the secure decision at the expense of some impact on query performance. To minimize this performance impact, apply the following principles:
-- MAGIC
-- MAGIC - **Use simple policy functions:** Policy functions with fewer expressions will generally perform better than more complex expressions. Avoid using mapping tables and expression subqueries in favor of simple CASE functions.
-- MAGIC
-- MAGIC - **Reduce the number of function arguments:** Databricks cannot optimize away column references to the source table resulting from policy function arguments, even if these columns are not otherwise used in the query. Use policy functions with fewer arguments, as the queries from these tables will generally perform better.
-- MAGIC
-- MAGIC - **Avoid adding row filters with too many AND conjuncts:** Since each table only supports adding at most one row filter, a common approach is to combine multiple desired policy functions with AND. However, for each conjunct, the chances increase that conjunct(s) include components mentioned elsewhere in this table that could affect performance (such as the use of mapping tables). Use fewer conjuncts to improve performance.
-- MAGIC
-- MAGIC - **Use deterministic expressions that cannot throw errors in table policies and queries from these tables:** Some expressions may throw errors if the provided inputs are invalid, such as ANSI division. In such cases, the SQL compiler must not push down operations with those expressions (such as filters) too far down in the query plan, to avoid the possibility of errors like “division by zero” that reveal information about values prior to filtering and/or masking operations. Use expressions that are deterministic and never throw errors, such as try_divide in this example.
-- MAGIC
-- MAGIC - **Run test queries over your table to gauge performance:** Construct realistic queries that represent the workload you expect for your table with row filters and/or column masks and measure the performance. Make small modifications to the policy functions and observe their effects until you reach a good balance between performance and expressiveness of the filtering and masking logic.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Limitations
-- MAGIC - Databricks Runtime versions below 12.2 LTS do not support row filters or column masks. These runtimes fail securely, meaning that if you try to access tables from unsupported versions of these runtimes, no data is returned.
-- MAGIC - Delta Sharing does not work with row-level security or column masks.
-- MAGIC - You cannot apply row-level security or column masks to a view.
-- MAGIC - Time travel does not work with row-level security or column masks.
-- MAGIC - Path-based access to files in tables with policies is not supported.
-- MAGIC - Row-filter or column-mask policies with circular dependencies back to the original policies are not supported.
-- MAGIC - Deep and shallow clones are not supported.
-- MAGIC - MERGE statements do not support tables with row filter policies that contain nesting, aggregations, windows, limits, or non-deterministic functions.
-- MAGIC - Delta Lake APIs are not supported.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
-- MAGIC
-- MAGIC Adapted from [documentation](https://docs.databricks.com/en/tables/row-and-column-filters.html#language-SQL)
