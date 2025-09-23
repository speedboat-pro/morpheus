# Databricks notebook source
# MAGIC %md
# MAGIC ### Demo of Delta Lake change data feed

# COMMAND ----------

# MAGIC %md #### Create a silver table that tracks absolute number vaccinations and available doses by country

# COMMAND ----------

spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase("labuser11725301_1758653991")

# COMMAND ----------

countries = [("USA", 10000, 20000), ("India", 1000, 1500), ("UK", 7000, 10000), ("Canada", 500, 700) ]
columns = ["Country","NumVaccinated","AvailableDoses"]
spark.createDataFrame(data=countries, schema = columns).write.format("delta").mode("overwrite").saveAsTable("silverTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverTable

# COMMAND ----------

import pyspark.sql.functions as F
spark.read.format("delta").table("silverTable").withColumn("VaccinationRate", F.col("NumVaccinated") / F.col("AvailableDoses")) \
  .drop("NumVaccinated").drop("AvailableDoses") \
  .write.format("delta").mode("overwrite").saveAsTable("goldTable")

# COMMAND ----------

# MAGIC %md #### Generate gold table showing vaccination rate by country

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM goldTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable change data feed on silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silverTable SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update silver table daily

# COMMAND ----------

# Insert new records
new_countries = [("Australia", 100, 3000)]
spark.createDataFrame(data=new_countries, schema = columns).write.format("delta").mode("append").saveAsTable("silverTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- update a record
# MAGIC UPDATE silverTable SET NumVaccinated = '11000' WHERE Country = 'USA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete a record
# MAGIC DELETE from silverTable WHERE Country = 'UK'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silverTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore the change data in SQL and PySpark 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL silverTable

# COMMAND ----------

# MAGIC %sql
# MAGIC -- view the changes
# MAGIC SELECT * FROM table_changes('silverTable', 2) order by _commit_timestamp

# COMMAND ----------

changes_df = spark.read.format("delta").option("readChangeData", True).option("startingVersion", 2).table('silverTable')
display(changes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Propagate changes from silver to gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Collect only the latest version for each country
# MAGIC CREATE OR REPLACE TEMPORARY VIEW silverTable_latest_version as
# MAGIC SELECT * 
# MAGIC     FROM 
# MAGIC          (SELECT *, rank() over (partition by Country order by _commit_version desc) as rank
# MAGIC           FROM table_changes('silverTable', 2, 5)
# MAGIC           WHERE _change_type !='update_preimage')
# MAGIC     WHERE rank=1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Merge the changes to gold
# MAGIC MERGE INTO goldTable t USING silverTable_latest_version s ON s.Country = t.Country
# MAGIC         WHEN MATCHED AND s._change_type='update_postimage' THEN UPDATE SET VaccinationRate = s.NumVaccinated/s.AvailableDoses
# MAGIC         WHEN NOT MATCHED THEN INSERT (Country, VaccinationRate) VALUES (s.Country, s.NumVaccinated/s.AvailableDoses)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM goldTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean up tables

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE silverTable;
# MAGIC DROP TABLE goldTable;

# COMMAND ----------


