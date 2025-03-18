-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

USE CATALOG big_sample;
USE SCHEMA dbacademy;


-- COMMAND ----------

ALTER TABLE flights_cluster_id
ALTER COLUMN id SET NOT NULL;

ALTER TABLE flights_cluster_id
ADD CONSTRAINT flights_pk PRIMARY KEY (id);

-- COMMAND ----------

DESCRIBE EXTENDED flights

-- COMMAND ----------

ALTER TABLE flights ADD CONSTRAINT fk_flight_cluster_id FOREIGN KEY (id) REFERENCES flights_cluster_id(id)

-- COMMAND ----------

SELECT COUNT( DISTINCT id )FROM flights_cluster_id_flightnum;

-- COMMAND ----------

SELECT COUNT( DISTINCT id )FROM flights_cluster_id;

-- COMMAND ----------

SELECT COUNT( * )FROM flights_cluster_id;

-- COMMAND ----------

SELECT AVG(year)
FROM flights P
LEFT JOIN flights_cluster_id f ON P.id = f.id 

-- COMMAND ----------

SELECT AVG(year)
FROM flights P

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
-- MAGIC Adapted from [blog article](https://www.databricks.com/blog/primary-key-and-foreign-key-constraints-are-ga-and-now-enable-faster-queries)
