# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC ## Get Started with Databricks for Data Warehousing
# MAGIC
# MAGIC This course provides a comprehensive overview of Databricks modern approach to data warehousing, highlighting how a data lakehouse architecture combines the strengths of traditional data warehouses with the flexibility and scalability of the cloud. You’ll learn about the AI-driven features that enhance data transformation and analysis on the Databricks Data Intelligence Platform. Designed for data warehousing practitioners, this course provides you with the foundational information needed to begin building and managing high-performance, AI-powered data warehouses on Databricks. 
# MAGIC
# MAGIC This course is designed for those starting out in data warehousing and those who would like to execute data warehousing workloads on Databricks. Participants may also include data warehousing practitioners who are familiar with traditional data warehousing techniques and concepts and are looking to expand their understanding of how data warehousing workloads are executed on Databricks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC The content was developed for participants with these skills/knowledge/abilities:  
# MAGIC - A basic understanding of data warehousing principles and topics such as database administration, SQL, data manipulation, and storage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Course Agenda
# MAGIC The following modules are part of the **Get Started with Databricks for Data Warehousing** course by **Databricks Academy**.
# MAGIC
# MAGIC |  Module Name | Content |
# MAGIC |:----:|-------|
# MAGIC | [**M01 - Databricks Overview**]($./01 - Databricks Overview) | **Lecture:** Databricks Infrastructure  </br> **Lecture:** Databricks Data Intelligence Platform </br> **Lecture:** Unity Catalog Overview </br> **Demo:** [01 - Databricks Workspace Walkthrough]($./01 - Databricks Overview/01 - Databricks Workspace Walkthrough) |
# MAGIC | [**M02 - Using Delta Lake features with Databricks SQL**]($./02 - Using Delta Lake features with Databricks SQL) | **Lecture:** Introduction to Data Warehousing with Databricks </br> **Lecture:** Databricks SQL Warehouses </br> **Lecture:** Delta Lake Overview </br> **Demo:** [02 - Using Delta Lake features with Databricks SQL]($./02 - Using Delta Lake features with Databricks SQL/02 - Using Delta Lake Features with Databricks SQL) |
# MAGIC | [**M03 - Data Ingestion and Transformation**]($./03 - Data Ingestion and Transformation) | **Lecture:** Delta Lake UniForm </br> **Lecture:** Data Ingestion Techniques Overview </br> **Demo:** [03.1 -  Data Ingestion Techniques]($./03 - Data Ingestion and Transformation/03.1 -  Data Ingestion Techniques) </br> **Lecture:** Data Transformation </br> **Demo:** [03.2 - Exploring Data Transformation in Databricks]($./03 - Data Ingestion and Transformation/03.2 - Exploring Data Transformation in Databricks) </br>|
# MAGIC | [**M04 - Data Orchestration and Querying Capabilities**]($./04 - Data Orchestration and Querying Capabilities) | **Lecture:** Orchestration in Databricks </br> **Demo:** [04 - Setting Up and Managing Serverless Lakeflow Jobs]($./04 - Data Orchestration and Querying Capabilities/04 - Setting Up and Managing Serverless Lakeflow Jobs) </br> **Lecture:** Databricks Querying Capabilities |
# MAGIC | [**M05 - Data Presentation**]($./05 - Data Presentation) | **Lecture:** Introduction to AI/BI </br> **Demo:** [05.1 - Creating AI-BI Dashboard in Databricks]($./05 - Data Presentation/05.1 - Creating AI-BI Dashboard in Databricks) </br> **Demo:** [05.2 - Creating AI-BI Genie Spaces]($./05 - Data Presentation/05.2 - Creating AI-BI Genie Spaces)|
# MAGIC | [**M06 - Data Warehousing Comprehensive Lab**]($./06 - Data Warehousing Comprehensive Lab) | **Lab:** [Lab - Data Warehousing Comprehensive Lab]($./06 - Data Warehousing Comprehensive Lab/Lab - Data Warehousing Comprehensive Lab)|
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use one of the following Databricks runtime(s): **`16.3.x-scala2.12`**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
