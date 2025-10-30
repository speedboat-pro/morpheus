-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Initial Tables

-- COMMAND ----------


-- Drop the tables if they exist to start from the beginning
DROP TABLE IF EXISTS emp_bronze_raw;
DROP TABLE IF EXISTS emp_silver_streaming;

-- Create the employees table
CREATE TABLE emp_bronze_raw 
(
    EmployeeID INT,
    FirstName VARCHAR(20),
    Department VARCHAR(20)
);

ALTER TABLE emp_bronze_raw SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- Insert 5 rows of sample data
INSERT INTO emp_bronze_raw (EmployeeID, FirstName, Department)
VALUES
(1, 'John', 'Marketing'),
(2, 'Raul', 'HR'),
(3, 'Michael', 'IT'),
(4, 'Panagiotis', 'Finance'),
(5, 'Aniket', 'Operations');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create CDF Source Table compatible with Declarative Pipelines

-- COMMAND ----------

CREATE OR REPLACE TABLE cdf_bronze_tbl AS
SELECT 
_change_type AS changetype,
_commit_timestamp AS commit_timestamp
 , *
EXCEPT (_change_type, _commit_version, _commit_timestamp) FROM table_changes('emp_bronze_raw',1)
WHERE _change_type != 'update_preimage'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CDC Demo 1: Extra Insert  

-- COMMAND ----------

INSERT INTO emp_bronze_raw (EmployeeID, FirstName, Department)
VALUES
(6, 'Athena', 'Marketing'),
(7, 'Pedro', 'Training');

-- COMMAND ----------

-- MAGIC
-- MAGIC %md
-- MAGIC ## CDC Demo 2: Update

-- COMMAND ----------

UPDATE emp_bronze_raw set Department = 'Analytics' where EmployeeID % 2 = 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update CDF Source Table compatible with Declarative Pipelines

-- COMMAND ----------

INSERT INTO cdf_bronze_tbl
SELECT 
_change_type AS changetype,
_commit_timestamp AS commit_timestamp
 , *
EXCEPT (_change_type, _commit_version, _commit_timestamp) FROM table_changes('emp_bronze_raw',1)
WHERE _change_type != 'update_preimage'

EXCEPT SELECT * FROM cdf_bronze_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CDC Demo 3: Delete

-- COMMAND ----------

DELETE FROM emp_bronze_raw WHERE EmployeeID = 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Update CDF Source Table compatible with Declarative Pipelines

-- COMMAND ----------

INSERT INTO cdf_bronze_tbl
SELECT 
_change_type AS changetype,
_commit_timestamp AS commit_timestamp
 , *
EXCEPT (_change_type, _commit_version, _commit_timestamp) FROM table_changes('emp_bronze_raw',1)
WHERE _change_type != 'update_preimage'

EXCEPT SELECT * FROM cdf_bronze_tbl

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Cleanup

-- COMMAND ----------

DROP TABLE IF EXISTS emp_bronze_raw;
DROP TABLE IF EXISTS cdf_bronze_tbl;
DROP TABLE IF EXISTS cdf_bronze2;
DROP TABLE IF EXISTS emp_silver_streaming;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
