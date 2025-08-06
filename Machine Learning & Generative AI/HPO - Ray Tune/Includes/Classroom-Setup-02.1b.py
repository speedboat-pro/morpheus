# Databricks notebook source
# MAGIC %run ./Classroom-Setup-02.1a

# COMMAND ----------

# Split the dataset into training and test sets
train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
