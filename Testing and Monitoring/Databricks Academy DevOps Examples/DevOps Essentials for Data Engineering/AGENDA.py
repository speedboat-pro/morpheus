# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # DevOps Essentials for Data Engineering
# MAGIC
# MAGIC This course explores software engineering best practices and DevOps principles, specifically designed for data engineers working with Databricks. Participants will build a strong foundation in key topics such as code quality, version control, documentation, and testing. The course emphasizes DevOps, covering core components, benefits, and the role of continuous integration and delivery (CI/CD) in optimizing data engineering workflows.
# MAGIC
# MAGIC You will learn how to apply modularity principles in PySpark to create reusable components and structure code efficiently. Hands-on experience includes designing and implementing unit tests for PySpark functions using the pytest framework, followed by integration testing for Databricks data pipelines with DLT and Workflows to ensure reliability.
# MAGIC
# MAGIC The course also covers essential Git operations within Databricks, including using Databricks Git Folders to integrate continuous integration practices. Finally, you will take a high level look at various deployment methods for Databricks assets, such as REST API, CLI, SDK, and Databricks Asset Bundles (DABs), providing you with the knowledge of techniques to deploy and manage your pipelines.
# MAGIC
# MAGIC By the end of the course, you will be proficient in software engineering and DevOps best practices, enabling you to build scalable, maintainable, and efficient data engineering solutions.
# MAGIC
# MAGIC ---
# MAGIC ## Prerequisites
# MAGIC
# MAGIC - Proficient knowledge of the Databricks platform, including experience with Databricks Workspaces, Apache Spark, Delta Lake and the Medallion Architecture, Unity Catalog, Delta Live Tables, and Workflows. A basic understanding of Git version control is also required.
# MAGIC - Experience ingesting and transforming data, with proficiency in PySpark for data processing and DataFrame manipulations. Additionally, candidates should have experience writing intermediate level SQL queries for data analysis and transformation.
# MAGIC - Knowledge of Python programming, with proficiency in writing intermediate level Python code, including the ability to design and implement functions and classes. Users should also be skilled in creating, importing, and effectively utilizing Python packages.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Course Agenda:
# MAGIC
# MAGIC The following modules are part of the **Data Engineer Learning** Path by Databricks Academy.
# MAGIC
# MAGIC  Module | Notebook Name |
# MAGIC |-------|-------------|
# MAGIC  **[M02 - CI]($./Course Notebooks/M02 - CI)**    |[2.1 - Modularizing PySpark Code - REQUIRED]($./Course Notebooks/M02 - CI/2.1 - Modularizing PySpark Code - REQUIRED)<br>[2.2L - Modularize PySpark Code]($./Course Notebooks/M02 - CI/2.2L - Modularize PySpark Code)<br>[2.3 - Project Setup Exploration]($./Course Notebooks/M02 - CI/2.3 - Project Setup Exploration)<br>[2.4 - Creating and Executing Unit Tests]($./Course Notebooks/M02 - CI/2.4 - Creating and Executing Unit Tests) <br>[2.5L - Create and Execute Unit Tests]($./Course Notebooks/M02 - CI/2.5L - Create and Execute Unit Tests)<br>[2.6 - Performing Integration Tests]($./Course Notebooks/M02 - CI/2.6 - Performing Integration Tests)<br>[2.7L - Version Control with Databricks Git Folders and GitHub]($./Course Notebooks/M02 - CI/2.7L - Version Control with Databricks Git Folders and GitHub)|
# MAGIC  **[M03 - CD]($./Course Notebooks/M03 - CD)** |[3.1 - Deploying the Databricks Assets]($./Course Notebooks/M03 - CD/3.1 - Deploying the Databricks Assets) | 
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use the following Databricks runtime: **`16.4.x-scala2.12`**
# MAGIC * This course will only run in the Databricks Academy labs.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
