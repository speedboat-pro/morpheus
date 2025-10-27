-- Databricks notebook source
-- MAGIC %md
-- MAGIC # DLT Gold Table(s)

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW chol_age_agg
AS
SELECT 
    HighCholest_Group, 
    Age_Group, 
    count(*) as Total
FROM LIVE.health_silver
GROUP BY HighCholest_Group, Age_Group
