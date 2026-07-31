# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC
# MAGIC # [BONUS Lab] - PDF Intelligence and Knowledge Assistants
# MAGIC ## Overview
# MAGIC This lab reinforces concepts from **Demo 02** (Exploring the Knowledge Assistant) and **Demo 04** (Transforming PDFs to Structured Data). You will practice parsing documents, classifying and extracting information, and querying a Knowledge Assistant programmatically.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC By the end of this lab, you will be able to:
# MAGIC 1. Parse PDF documents and explore their structure using `ai_parse_document`
# MAGIC 2. Apply custom classification categories using `ai_classify`
# MAGIC 3. Extract structured fields from unstructured text using `ai_extract`
# MAGIC 4. Query a Knowledge Assistant programmatically and inspect the response
# MAGIC 5. Combine document intelligence with Knowledge Assistant queries for richer insights
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Completed **Demo 02** and **Demo 04**
# MAGIC - Serverless Compute, Version 5

# COMMAND ----------

# DBTITLE 1,Section A Header
# MAGIC %md
# MAGIC
# MAGIC ## A. Classroom Setup
# MAGIC Run the setup cell below to configure your environment variables.

# COMMAND ----------

# DBTITLE 1,Classroom Setup
# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# DBTITLE 1,Define Variables
# Shared volume with Airbnb PDFs
SHARED_VOLUME = "/Volumes/dbacademy/ka_all/vol_all"
LISTINGS_PDF = f"{SHARED_VOLUME}/listings/airbnb_listings.pdf"

print(f"Catalog:      {catalog_name}")
print(f"Schema:       {schema_name}")
print(f"Listings PDF: {LISTINGS_PDF}")

# COMMAND ----------

# DBTITLE 1,Exercise 1 Header
# MAGIC %md
# MAGIC
# MAGIC ## B. Exercise 1: Parse and Explore Document Structure
# MAGIC
# MAGIC ### Background
# MAGIC In Demo 04, you learned that `ai_parse_document` converts a PDF into a structured VARIANT containing an `elements` array. Each element has a `type` (e.g., `Title`, `NarrativeText`, `Table`) and `content`.
# MAGIC
# MAGIC ### Your Task
# MAGIC Write a query that:
# MAGIC 1. Parses the Airbnb listings PDF
# MAGIC 2. Explodes the elements array
# MAGIC 3. Groups elements by their `type` to see how many of each type exist
# MAGIC 4. Orders the result by count descending
# MAGIC
# MAGIC This helps you understand the structure of the parsed document before applying AI functions.
# MAGIC
# MAGIC **Hint:** Use `variant_explode` with `LATERAL` and `GROUP BY` on `element.value:type::STRING`.

# COMMAND ----------

# DBTITLE 1,Exercise 1 - Your Code
# TODO: Write your query to count element types in the parsed PDF
# Replace the <FILL_IN> placeholders with the correct code

element_counts_df = spark.sql(f"""
  WITH parsed_docs AS (
    SELECT
      ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES(
      '{LISTINGS_PDF}',
      format => 'binaryFile'
    )
  )
  SELECT
    <FILL_IN> AS element_type,
    COUNT(*) AS element_count
  FROM
    parsed_docs,
    LATERAL variant_explode(<FILL_IN>) AS element
  GROUP BY <FILL_IN>
  ORDER BY element_count DESC
""")

display(element_counts_df)

# COMMAND ----------

# DBTITLE 1,Exercise 1 Solution Header
# MAGIC %md
# MAGIC
# MAGIC ### Exercise 1 - Solution
# MAGIC Expand the cell below to check your answer.

# COMMAND ----------

# DBTITLE 1,Exercise 1 - Solution
# SOLUTION
element_counts_df = spark.sql(f"""
  WITH parsed_docs AS (
    SELECT
      ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES(
      '{LISTINGS_PDF}',
      format => 'binaryFile'
    )
  )
  SELECT
    element.value:type::STRING AS element_type,
    COUNT(*) AS element_count
  FROM
    parsed_docs,
    LATERAL variant_explode(parsed_docs.parsed:document:elements) AS element
  GROUP BY element.value:type::STRING
  ORDER BY element_count DESC
""")

display(element_counts_df)

# COMMAND ----------

# DBTITLE 1,Exercise 2 Header
# MAGIC %md
# MAGIC
# MAGIC ## C. Exercise 2: Custom Classification with `ai_classify`
# MAGIC
# MAGIC ### Background
# MAGIC In Demo 04, you classified listing content into categories like `property_description`, `pricing_information`, etc. The `ai_classify` function takes text and an array of candidate labels, then returns the best-matching label.
# MAGIC
# MAGIC ### Your Task
# MAGIC Create a more granular classification by categorizing listing content into **guest experience** categories:
# MAGIC - `family_friendly` - mentions kids, families, cribs, or child-related amenities
# MAGIC - `business_travel` - mentions workspace, WiFi, desks, or professional amenities
# MAGIC - `outdoor_activities` - mentions parks, hiking, beaches, or nature
# MAGIC - `nightlife_dining` - mentions restaurants, bars, clubs, or food scene
# MAGIC - `relaxation_wellness` - mentions spa, quiet, peaceful, or retreat-like qualities
# MAGIC - `other` - doesn't fit the above categories
# MAGIC
# MAGIC Filter elements to those with content longer than 80 characters and limit to 15 results.
# MAGIC
# MAGIC **Hint:** Remember that `ai_classify` returns a VARIANT. Access the top label with `cls:response[0]`.

# COMMAND ----------

# DBTITLE 1,Exercise 2 - Your Code
# TODO: Classify listing content into guest experience categories
# Replace the <FILL_IN> placeholders with the correct code

guest_experience_df = spark.sql(f"""
WITH elements AS (
  SELECT
    element.value:content::STRING AS content
  FROM (
    SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES('{LISTINGS_PDF}', format => 'binaryFile')
  ) t,
  LATERAL variant_explode(parsed:document:elements) AS element
  WHERE element.value:content IS NOT NULL
    AND LENGTH(element.value:content::STRING) > 80
),
classified AS (
  SELECT
    content,
    ai_classify(
      content,
      <FILL_IN>
    ) AS cls
  FROM elements
)
SELECT
  content,
  <FILL_IN> AS guest_category
FROM classified
LIMIT 15
""")

display(guest_experience_df)

# COMMAND ----------

# DBTITLE 1,Exercise 2 Solution Header
# MAGIC %md
# MAGIC
# MAGIC ### Exercise 2 - Solution

# COMMAND ----------

# DBTITLE 1,Exercise 2 - Solution
# SOLUTION
guest_experience_df = spark.sql(f"""
WITH elements AS (
  SELECT
    element.value:content::STRING AS content
  FROM (
    SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES('{LISTINGS_PDF}', format => 'binaryFile')
  ) t,
  LATERAL variant_explode(parsed:document:elements) AS element
  WHERE element.value:content IS NOT NULL
    AND LENGTH(element.value:content::STRING) > 80
),
classified AS (
  SELECT
    content,
    ai_classify(
      content,
      '["family_friendly","business_travel","outdoor_activities","nightlife_dining","relaxation_wellness","other"]'
    ) AS cls
  FROM elements
)
SELECT
  content,
  cls:response[0] AS guest_category
FROM classified
LIMIT 15
""")

display(guest_experience_df)

# COMMAND ----------

# DBTITLE 1,Exercise 3 Header
# MAGIC %md
# MAGIC
# MAGIC ## D. Exercise 3: Extract Pricing and Booking Details with `ai_extract`
# MAGIC
# MAGIC ### Background
# MAGIC In Demo 04, you extracted fields like `property_name`, `neighborhood`, `property_type`, and `key_amenities`. The `ai_extract` function accepts a JSON array of field names and returns a VARIANT with a `response` object containing those fields.
# MAGIC
# MAGIC ### Your Task
# MAGIC Extract **pricing and booking** details from the listing text. Extract these fields:
# MAGIC - `nightly_price` - the cost per night
# MAGIC - `cleaning_fee` - any cleaning fee mentioned
# MAGIC - `minimum_stay` - minimum number of nights required
# MAGIC - `cancellation_policy` - the cancellation terms
# MAGIC - `max_guests` - maximum number of guests allowed
# MAGIC
# MAGIC Filter to elements longer than 100 characters and limit to 10 results.
# MAGIC
# MAGIC **Hint:** Access extracted fields with `ext:response.field_name`.

# COMMAND ----------

# DBTITLE 1,Exercise 3 - Your Code
# TODO: Extract pricing and booking details from listings
# Replace the <FILL_IN> placeholders with the correct code

pricing_df = spark.sql(f"""
WITH elements AS (
  SELECT
    element.value:content::STRING AS content
  FROM (
    SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES('{LISTINGS_PDF}', format => 'binaryFile')
  ) t,
  LATERAL variant_explode(parsed:document:elements) AS element
  WHERE element.value:content IS NOT NULL
    AND LENGTH(element.value:content::STRING) > 100
),
extracted AS (
  SELECT
    content,
    ai_extract(
      content,
      <FILL_IN>
    ) AS ext
  FROM elements
)
SELECT
  content,
  ext:response.nightly_price     AS nightly_price,
  ext:response.cleaning_fee      AS cleaning_fee,
  <FILL_IN>                      AS minimum_stay,
  <FILL_IN>                      AS cancellation_policy,
  <FILL_IN>                      AS max_guests
FROM extracted
LIMIT 10
""")

display(pricing_df)

# COMMAND ----------

# DBTITLE 1,Exercise 3 Solution Header
# MAGIC %md
# MAGIC
# MAGIC ### Exercise 3 - Solution

# COMMAND ----------

# DBTITLE 1,Exercise 3 - Solution
# SOLUTION
pricing_df = spark.sql(f"""
WITH elements AS (
  SELECT
    element.value:content::STRING AS content
  FROM (
    SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES('{LISTINGS_PDF}', format => 'binaryFile')
  ) t,
  LATERAL variant_explode(parsed:document:elements) AS element
  WHERE element.value:content IS NOT NULL
    AND LENGTH(element.value:content::STRING) > 100
),
extracted AS (
  SELECT
    content,
    ai_extract(
      content,
      '["nightly_price","cleaning_fee","minimum_stay","cancellation_policy","max_guests"]'
    ) AS ext
  FROM elements
)
SELECT
  content,
  ext:response.nightly_price         AS nightly_price,
  ext:response.cleaning_fee          AS cleaning_fee,
  ext:response.minimum_stay          AS minimum_stay,
  ext:response.cancellation_policy   AS cancellation_policy,
  ext:response.max_guests            AS max_guests
FROM extracted
LIMIT 10
""")

display(pricing_df)

# COMMAND ----------

# DBTITLE 1,Exercise 4 Header
# MAGIC %md
# MAGIC
# MAGIC ## E. Exercise 4: Query the Knowledge Assistant Programmatically
# MAGIC
# MAGIC ### Background
# MAGIC In Demo 02, you learned to query a Knowledge Assistant via its model serving endpoint using the OpenAI client. The steps are:
# MAGIC 1. Authenticate with the workspace token
# MAGIC 2. Create an OpenAI client pointing to the Databricks serving endpoints
# MAGIC 3. Use `client.responses.create()` with the endpoint name
# MAGIC
# MAGIC ### Your Task
# MAGIC 1. Set up the OpenAI client (code provided)
# MAGIC 2. Find the Knowledge Assistant endpoint name using the Databricks SDK
# MAGIC 3. Ask the Knowledge Assistant: **"What neighborhoods have listings with outdoor spaces or gardens?"**
# MAGIC 4. Print the response
# MAGIC
# MAGIC **Hint:** Use `w.knowledge_assistants.list_knowledge_assistants()` to get the endpoint name.

# COMMAND ----------

# DBTITLE 1,Exercise 4 - Setup Client
# Setup - Run this cell first (provided for you)
import os
from openai import OpenAI
from databricks.sdk import WorkspaceClient
import mlflow

mlflow.openai.autolog()

w = WorkspaceClient()

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host = w.config.host

client = OpenAI(
    api_key=token,
    base_url=f"{host}/serving-endpoints",
)

print(f"Client configured for: {host}")

# COMMAND ----------

# DBTITLE 1,Exercise 4 - Your Code
# TODO: Find the Knowledge Assistant endpoint and query it
# Step 1: List Knowledge Assistants and get the endpoint name
kas = w.knowledge_assistants.list_knowledge_assistants()
for ka in kas:
    print(f"Name: {ka.display_name}, Endpoint: {ka.endpoint_name}")
    endpoint_name = ka.endpoint_name

# Step 2: Query the Knowledge Assistant
# Replace <FILL_IN> with your question
response = client.responses.create(
    model=endpoint_name,
    input=[
        {
            "role": "user",
            "content": "<FILL_IN>"
        }
    ]
)

# Step 3: Print the response
print(response.output_text)

# COMMAND ----------

# DBTITLE 1,Exercise 4 Solution Header
# MAGIC %md
# MAGIC
# MAGIC ### Exercise 4 - Solution

# COMMAND ----------

# DBTITLE 1,Exercise 4 - Solution
# SOLUTION
kas = w.knowledge_assistants.list_knowledge_assistants()
for ka in kas:
    print(f"Name: {ka.display_name}, Endpoint: {ka.endpoint_name}")
    endpoint_name = ka.endpoint_name

# Query the Knowledge Assistant
response = client.responses.create(
    model=endpoint_name,
    input=[
        {
            "role": "user",
            "content": "What neighborhoods have listings with outdoor spaces or gardens?"
        }
    ]
)

print(response.output_text)

# COMMAND ----------

# DBTITLE 1,Exercise 5 Header
# MAGIC %md
# MAGIC
# MAGIC ## F. Exercise 5: End-to-End Pipeline - Parse, Classify, Extract, and Save
# MAGIC
# MAGIC ### Background
# MAGIC In Demo 04, you combined all three AI functions into a single `CREATE OR REPLACE TABLE` statement that built a structured Delta table from raw PDF content. This is the **Parse → Classify → Extract** pattern.
# MAGIC
# MAGIC ### Your Task
# MAGIC Build a complete pipeline that:
# MAGIC 1. Parses the PDF and flattens elements (filter to content > 100 chars)
# MAGIC 2. Classifies each element into the original categories: `property_description`, `pricing_information`, `location_details`, `amenities`, `host_information`, `other`
# MAGIC 3. Extracts these fields: `property_name`, `neighborhood`, `nightly_price`, `property_type`
# MAGIC 4. Saves the result as a Delta table called `airbnb_bonus_listings` in your schema
# MAGIC
# MAGIC This exercise combines everything you practiced above into a single production-ready query.

# COMMAND ----------

# DBTITLE 1,Exercise 5 - Your Code
# TODO: Build the full pipeline and save as a Delta table
# Replace the <FILL_IN> placeholders with the correct code

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.airbnb_bonus_listings AS
WITH elements AS (
  SELECT
    element.value:content::STRING AS content
  FROM (
    SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES('{LISTINGS_PDF}', format => 'binaryFile')
  ) t,
  LATERAL variant_explode(parsed:document:elements) AS element
  WHERE element.value:content IS NOT NULL
    AND LENGTH(element.value:content::STRING) > 100
),
classified_extracted AS (
  SELECT
    content,
    ai_classify(
      content,
      <FILL_IN>
    ) AS cls,
    ai_extract(
      content,
      <FILL_IN>
    ) AS ext
  FROM elements
)
SELECT
  content,
  <FILL_IN>   AS category,
  <FILL_IN>   AS property_name,
  <FILL_IN>   AS neighborhood,
  <FILL_IN>   AS nightly_price,
  <FILL_IN>   AS property_type
FROM classified_extracted
""")

print("Table created: airbnb_bonus_listings")

# COMMAND ----------

# DBTITLE 1,Exercise 5 Solution Header
# MAGIC %md
# MAGIC
# MAGIC ### Exercise 5 - Solution

# COMMAND ----------

# DBTITLE 1,Exercise 5 - Solution
# SOLUTION
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.airbnb_bonus_listings AS
WITH elements AS (
  SELECT
    element.value:content::STRING AS content
  FROM (
    SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
    FROM READ_FILES('{LISTINGS_PDF}', format => 'binaryFile')
  ) t,
  LATERAL variant_explode(parsed:document:elements) AS element
  WHERE element.value:content IS NOT NULL
    AND LENGTH(element.value:content::STRING) > 100
),
classified_extracted AS (
  SELECT
    content,
    ai_classify(
      content,
      '["property_description","pricing_information","location_details","amenities","host_information","other"]'
    ) AS cls,
    ai_extract(
      content,
      '["property_name","neighborhood","nightly_price","property_type"]'
    ) AS ext
  FROM elements
)
SELECT
  content,
  cls:response[0]              AS category,
  ext:response.property_name   AS property_name,
  ext:response.neighborhood    AS neighborhood,
  ext:response.nightly_price   AS nightly_price,
  ext:response.property_type   AS property_type
FROM classified_extracted
""")

print("Table created: airbnb_bonus_listings")

# COMMAND ----------

# DBTITLE 1,Verify Table
# Verify the table was created successfully
display(spark.sql(f"SELECT * FROM {catalog_name}.{schema_name}.airbnb_bonus_listings"))

# COMMAND ----------

# DBTITLE 1,Exercise 6 Header
# MAGIC %md
# MAGIC
# MAGIC ## G. Exercise 6: Compare Structured Extraction with Knowledge Assistant Responses
# MAGIC
# MAGIC ### Background
# MAGIC You now have two approaches to get information from the Airbnb data:
# MAGIC - **Structured extraction** (Demo 04): Parse → Classify → Extract gives you tabular data you can query with SQL
# MAGIC - **Knowledge Assistant** (Demo 02): Natural language Q&A over the same source data via RAG
# MAGIC
# MAGIC ### Your Task
# MAGIC 1. Query your `airbnb_bonus_listings` table to find listings in a specific neighborhood
# MAGIC 2. Ask the Knowledge Assistant the same question
# MAGIC 3. Compare the two approaches in the markdown cell provided
# MAGIC
# MAGIC This exercise demonstrates when to use each approach:
# MAGIC - **Structured tables** are best for aggregations, filtering, and joining with other data
# MAGIC - **Knowledge Assistants** are best for natural language exploration and nuanced questions

# COMMAND ----------

# DBTITLE 1,Exercise 6a - SQL Query
# Step 1: Query the structured table for a specific neighborhood
# Try filtering for listings that mention a popular SF neighborhood
display(spark.sql(f"""
  SELECT 
    property_name,
    neighborhood,
    property_type,
    nightly_price,
    category
  FROM {catalog_name}.{schema_name}.airbnb_bonus_listings
  WHERE neighborhood IS NOT NULL
"""))

# COMMAND ----------

# DBTITLE 1,Exercise 6b - Knowledge Assistant Query
# Step 2: Ask the Knowledge Assistant the same type of question
# Compare the richness of the natural language response with the structured data
response = client.responses.create(
    model=endpoint_name,
    input=[
        {
            "role": "user",
            "content": "What are some listings available in popular San Francisco neighborhoods? Include details about amenities and pricing."
        }
    ]
)

print(response.output_text)

# COMMAND ----------

# DBTITLE 1,Exercise 6c - Reflection
# MAGIC %md
# MAGIC
# MAGIC ### Reflection: When to Use Each Approach
# MAGIC
# MAGIC Consider the differences between the two approaches:
# MAGIC
# MAGIC | Aspect | Structured Extraction | Knowledge Assistant |
# MAGIC | --- | --- | --- |
# MAGIC | **Output format** | Tabular (SQL-queryable) | Natural language text |
# MAGIC | **Best for** | Aggregations, dashboards, joins | Exploration, nuanced Q&A |
# MAGIC | **Scalability** | Batch processing of many docs | One question at a time |
# MAGIC | **Flexibility** | Fixed schema (fields defined upfront) | Open-ended questions |
# MAGIC | **Accuracy** | Precise field extraction | Context-aware with citations |
# MAGIC
# MAGIC **Key Takeaway:** Use structured extraction when you need data in tables for analytics. Use Knowledge Assistants when you need conversational access to unstructured content.

# COMMAND ----------

# DBTITLE 1,Conclusion
# MAGIC %md
# MAGIC
# MAGIC ## H. Conclusion
# MAGIC
# MAGIC Congratulations! In this bonus lab, you practiced:
# MAGIC
# MAGIC 1. **Parsing PDFs** and exploring document structure by counting element types
# MAGIC 2. **Custom classification** with `ai_classify` using guest-experience categories
# MAGIC 3. **Field extraction** with `ai_extract` for pricing and booking details
# MAGIC 4. **Querying a Knowledge Assistant** programmatically via the OpenAI client
# MAGIC 5. **Building an end-to-end pipeline** that combines Parse → Classify → Extract into a Delta table
# MAGIC 6. **Comparing approaches** — structured extraction vs. Knowledge Assistants
# MAGIC
# MAGIC ### Key Concepts Reinforced
# MAGIC - `ai_parse_document` with `MAP('version', '2.0')` for consistent parsing
# MAGIC - `variant_explode` with `LATERAL` to flatten nested VARIANT arrays
# MAGIC - `ai_classify` with a JSON array of candidate labels
# MAGIC - `ai_extract` with a JSON array of target field names
# MAGIC - Accessing VARIANT results: `cls:response[0]` and `ext:response.field_name`
# MAGIC - OpenAI-compatible client for Knowledge Assistant serving endpoints
# MAGIC - MLflow autologging for observability

# COMMAND ----------

# DBTITLE 1,Footer
# MAGIC %md
# MAGIC
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
