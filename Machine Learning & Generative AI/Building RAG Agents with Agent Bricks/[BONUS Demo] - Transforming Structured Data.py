# Databricks notebook source
# DBTITLE 1,Header
# MAGIC %md
# MAGIC
# MAGIC # [BONUS Demo] - Transforming Structured Data
# MAGIC ## Deep Dive: VARIANT Data Types & Transformations from `ai_parse_document`
# MAGIC
# MAGIC ### Overview
# MAGIC This notebook builds on Demo 04 and explores **VARIANT** data types in depth — the semi-structured format returned by `ai_parse_document`. You'll learn to navigate, transform, flatten, aggregate, and reshape parsed document output using Databricks SQL's rich VARIANT support.
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC 1. Understand the VARIANT data type and its role in semi-structured processing
# MAGIC 2. Navigate nested VARIANT structures using colon (`:`) path notation
# MAGIC 3. Explode arrays, filter elements, and apply transformations within VARIANT
# MAGIC 4. Cast VARIANT fields to typed columns for analytics
# MAGIC 5. Build reusable views and tables from parsed document data
# MAGIC 6. Apply advanced patterns: conditional extraction, array aggregation, and pivot-style reshaping

# COMMAND ----------

# DBTITLE 1,Classroom Setup
# MAGIC %run ./Includes/Classroom-Setup-04

# COMMAND ----------

# DBTITLE 1,Define source paths
# Reuse the same source PDF and table from Demo 04
SHARED_VOLUME = "/Volumes/dbacademy/ka_all/vol_all"
LISTINGS_PDF = f"{SHARED_VOLUME}/listings/airbnb_listings.pdf"
STRUCTURED_TABLE = f"{catalog_name}.{schema_name}.airbnb_structured_listings"

print(f"Source PDF:       {LISTINGS_PDF}")
print(f"Structured Table: {STRUCTURED_TABLE}")

# COMMAND ----------

# DBTITLE 1,Section A Header
# MAGIC %md
# MAGIC ## A. Understanding the VARIANT Data Type
# MAGIC
# MAGIC The `VARIANT` type in Databricks SQL is a **semi-structured** container that can hold:
# MAGIC - Scalars (strings, numbers, booleans, nulls)
# MAGIC - Arrays (ordered lists of VARIANT values)
# MAGIC - Objects (key-value maps where keys are strings and values are VARIANT)
# MAGIC
# MAGIC When `ai_parse_document` returns its result, it's a single `VARIANT` column containing a deeply nested JSON-like structure. Understanding how to navigate this structure is key to extracting value.
# MAGIC
# MAGIC ### Key Operators
# MAGIC | Operator | Purpose | Example |
# MAGIC | --- | --- | --- |
# MAGIC | `:` (colon) | Navigate into object fields | `parsed:document:elements` |
# MAGIC | `[n]` (bracket) | Index into arrays | `elements[0]` |
# MAGIC | `::TYPE` | Cast VARIANT to a SQL type | `field::STRING`, `field::INT` |
# MAGIC | `variant_explode()` | Expand array to rows | `LATERAL variant_explode(arr)` |
# MAGIC | `variant_get()` | Safe field access with default | `variant_get(obj, 'key', 'default')` |
# MAGIC | `try_cast(... AS TYPE)` | Safe cast that returns NULL on failure | `try_cast(v:field AS INT)` |

# COMMAND ----------

# DBTITLE 1,Explore raw VARIANT structure
# MAGIC %sql
# MAGIC -- Parse the PDF and examine the top-level VARIANT structure
# MAGIC -- Notice: the result has document.pages, document.elements, and metadata
# MAGIC SELECT
# MAGIC   ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC FROM READ_FILES(
# MAGIC   '/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf',
# MAGIC   format => 'binaryFile'
# MAGIC )
# MAGIC LIMIT 1

# COMMAND ----------

# DBTITLE 1,Top-level VARIANT keys
# MAGIC %sql
# MAGIC -- Inspect the top-level keys of the parsed VARIANT
# MAGIC -- schema_of_variant shows the inferred schema without materializing all data
# MAGIC WITH raw AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC )
# MAGIC SELECT
# MAGIC   schema_of_variant(parsed) AS variant_schema,
# MAGIC   parsed:metadata:schema_version::STRING AS schema_version,
# MAGIC   size(try_cast(parsed:document:pages AS ARRAY<VARIANT>)) AS num_pages,
# MAGIC   size(try_cast(parsed:document:elements AS ARRAY<VARIANT>)) AS num_elements
# MAGIC FROM raw

# COMMAND ----------

# DBTITLE 1,Section B Header
# MAGIC %md
# MAGIC ## B. Navigating Nested VARIANT with Path Expressions
# MAGIC
# MAGIC The colon (`:`) operator is your primary navigation tool. It supports chaining to reach deeply nested fields. Let's explore the `elements` array — each element has `type`, `content`, `bbox` (bounding box), and optionally `description`.

# COMMAND ----------

# DBTITLE 1,Element types distribution
# MAGIC %sql
# MAGIC -- Count elements by type to understand the document structure
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC )
# MAGIC SELECT
# MAGIC   element.value:type::STRING AS element_type,
# MAGIC   COUNT(*) AS element_count,
# MAGIC   ROUND(AVG(LENGTH(element.value:content::STRING)), 0) AS avg_content_length,
# MAGIC   MIN(LENGTH(element.value:content::STRING)) AS min_length,
# MAGIC   MAX(LENGTH(element.value:content::STRING)) AS max_length
# MAGIC FROM parsed,
# MAGIC   LATERAL variant_explode(parsed.parsed:document:elements) AS element
# MAGIC WHERE element.value:content IS NOT NULL
# MAGIC GROUP BY element.value:type::STRING
# MAGIC ORDER BY element_count DESC

# COMMAND ----------

# DBTITLE 1,Page-level metadata exploration
# MAGIC %sql
# MAGIC -- Explore page-level metadata from the parsed document
# MAGIC -- Each page has dimensions, page number, and associated elements
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC )
# MAGIC SELECT
# MAGIC   page.value:page_number::INT AS page_number,
# MAGIC   page.value:width::DOUBLE AS page_width,
# MAGIC   page.value:height::DOUBLE AS page_height,
# MAGIC   page.value:unit::STRING AS unit
# MAGIC FROM parsed,
# MAGIC   LATERAL variant_explode(parsed.parsed:document:pages) AS page
# MAGIC ORDER BY page_number

# COMMAND ----------

# DBTITLE 1,Bounding box exploration
# MAGIC %sql
# MAGIC -- Explore bounding box (bbox) data for spatial analysis of elements
# MAGIC -- bbox tells you where on the page each element appears
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC ),
# MAGIC elements AS (
# MAGIC   SELECT
# MAGIC     element.value:type::STRING AS element_type,
# MAGIC     element.value:content::STRING AS content,
# MAGIC     element.value:bbox[0]::DOUBLE AS x_min,
# MAGIC     element.value:bbox[1]::DOUBLE AS y_min,
# MAGIC     element.value:bbox[2]::DOUBLE AS x_max,
# MAGIC     element.value:bbox[3]::DOUBLE AS y_max,
# MAGIC     element.value:page::INT AS page_num
# MAGIC   FROM parsed,
# MAGIC     LATERAL variant_explode(parsed.parsed:document:elements) AS element
# MAGIC   WHERE element.value:content IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   element_type,
# MAGIC   SUBSTRING(content, 1, 60) AS content_preview,
# MAGIC   page_num,
# MAGIC   ROUND(x_min, 2) AS x_min,
# MAGIC   ROUND(y_min, 2) AS y_min,
# MAGIC   ROUND(x_max - x_min, 2) AS width,
# MAGIC   ROUND(y_max - y_min, 2) AS height
# MAGIC FROM elements
# MAGIC ORDER BY page_num, y_min
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Section C Header
# MAGIC %md
# MAGIC ## C. Array Transformations Within VARIANT
# MAGIC
# MAGIC VARIANT arrays support powerful SQL transformations. Databricks SQL provides `transform()`, `filter()`, `aggregate()`, and `exists()` for working with arrays without exploding them to rows.

# COMMAND ----------

# DBTITLE 1,Transform and filter arrays
# MAGIC %sql
# MAGIC -- Use transform() to process each element without exploding
# MAGIC -- Use filter() to select only elements matching criteria
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC ),
# MAGIC transformed AS (
# MAGIC   SELECT
# MAGIC     -- Filter: get only 'text' type elements with substantial content
# MAGIC     filter(
# MAGIC       try_cast(parsed:document:elements AS ARRAY<VARIANT>),
# MAGIC       e -> e:type::STRING = 'text' AND LENGTH(e:content::STRING) > 100
# MAGIC     ) AS text_elements,
# MAGIC     -- Transform: extract just the content strings
# MAGIC     transform(
# MAGIC       filter(
# MAGIC         try_cast(parsed:document:elements AS ARRAY<VARIANT>),
# MAGIC         e -> e:type::STRING = 'text' AND LENGTH(e:content::STRING) > 100
# MAGIC       ),
# MAGIC       e -> e:content::STRING
# MAGIC     ) AS content_strings
# MAGIC   FROM parsed
# MAGIC )
# MAGIC SELECT
# MAGIC   size(text_elements) AS num_substantial_text_blocks,
# MAGIC   content_strings[0] AS first_content_block,
# MAGIC   content_strings[1] AS second_content_block
# MAGIC FROM transformed

# COMMAND ----------

# DBTITLE 1,Aggregate array elements
# MAGIC %sql
# MAGIC -- Use aggregate() to compute statistics across array elements without exploding
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Total character count across all elements
# MAGIC   aggregate(
# MAGIC     try_cast(parsed:document:elements AS ARRAY<VARIANT>),
# MAGIC     CAST(0 AS BIGINT),
# MAGIC     (acc, e) -> acc + COALESCE(LENGTH(e:content::STRING), 0)
# MAGIC   ) AS total_chars,
# MAGIC   -- Count elements by checking existence
# MAGIC   size(
# MAGIC     filter(
# MAGIC       try_cast(parsed:document:elements AS ARRAY<VARIANT>),
# MAGIC       e -> e:type::STRING = 'title'
# MAGIC     )
# MAGIC   ) AS title_count,
# MAGIC   size(
# MAGIC     filter(
# MAGIC       try_cast(parsed:document:elements AS ARRAY<VARIANT>),
# MAGIC       e -> e:type::STRING = 'table'
# MAGIC     )
# MAGIC   ) AS table_count,
# MAGIC   -- Check if any figure elements exist
# MAGIC   exists(
# MAGIC     try_cast(parsed:document:elements AS ARRAY<VARIANT>),
# MAGIC     e -> e:type::STRING = 'figure'
# MAGIC   ) AS has_figures
# MAGIC FROM parsed

# COMMAND ----------

# DBTITLE 1,Section D Header
# MAGIC %md
# MAGIC ## D. Type Casting & Safe Extraction Patterns
# MAGIC
# MAGIC When working with VARIANT data from real documents, you'll encounter missing fields, unexpected types, and nulls. Here are defensive patterns for production use.

# COMMAND ----------

# DBTITLE 1,Safe casting patterns
# MAGIC %sql
# MAGIC -- Defensive extraction patterns for production-quality code
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC ),
# MAGIC elements AS (
# MAGIC   SELECT
# MAGIC     element.value AS raw_element,
# MAGIC     element.value:type::STRING AS element_type,
# MAGIC     element.value:content::STRING AS content
# MAGIC   FROM parsed,
# MAGIC     LATERAL variant_explode(parsed.parsed:document:elements) AS element
# MAGIC )
# MAGIC SELECT
# MAGIC   element_type,
# MAGIC   -- Safe cast with COALESCE for missing fields
# MAGIC   COALESCE(raw_element:type::STRING, 'unknown') AS safe_type,
# MAGIC   
# MAGIC   -- try_cast returns NULL instead of error on type mismatch
# MAGIC   try_cast(raw_element:page AS INT) AS page_number,
# MAGIC   
# MAGIC   -- CASE expressions for conditional extraction
# MAGIC   CASE
# MAGIC     WHEN element_type = 'title' THEN UPPER(content)
# MAGIC     WHEN element_type = 'table' THEN CONCAT('[TABLE] ', SUBSTRING(content, 1, 50))
# MAGIC     ELSE SUBSTRING(content, 1, 100)
# MAGIC   END AS formatted_content,
# MAGIC   
# MAGIC   -- Check if optional nested fields exist with IS NOT NULL
# MAGIC   raw_element:bbox IS NOT NULL AS has_bbox,
# MAGIC   raw_element:description IS NOT NULL AS has_description,
# MAGIC   
# MAGIC   -- Length-based categorization
# MAGIC   CASE
# MAGIC     WHEN LENGTH(content) < 50 THEN 'short'
# MAGIC     WHEN LENGTH(content) < 200 THEN 'medium'
# MAGIC     WHEN LENGTH(content) < 500 THEN 'long'
# MAGIC     ELSE 'very_long'
# MAGIC   END AS content_size_category
# MAGIC FROM elements
# MAGIC WHERE content IS NOT NULL
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Section E Header
# MAGIC %md
# MAGIC ## E. Flattening & Reshaping: From Nested to Tabular
# MAGIC
# MAGIC The most common workflow is converting deeply nested VARIANT into a flat, queryable table. This section shows multiple flattening patterns.

# COMMAND ----------

# DBTITLE 1,Multi-level flatten with page context
# MAGIC %sql
# MAGIC -- Flatten elements while preserving page context and positional ordering
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC ),
# MAGIC flattened AS (
# MAGIC   SELECT
# MAGIC     element.pos AS element_position,
# MAGIC     element.value:type::STRING AS element_type,
# MAGIC     element.value:content::STRING AS content,
# MAGIC     element.value:page::INT AS page_number,
# MAGIC     -- Extract bbox as individual columns for spatial queries
# MAGIC     element.value:bbox[0]::DOUBLE AS bbox_left,
# MAGIC     element.value:bbox[1]::DOUBLE AS bbox_top,
# MAGIC     element.value:bbox[2]::DOUBLE AS bbox_right,
# MAGIC     element.value:bbox[3]::DOUBLE AS bbox_bottom,
# MAGIC     -- Compute derived fields
# MAGIC     LENGTH(element.value:content::STRING) AS content_length,
# MAGIC     -- Use LAG/LEAD for document flow analysis
# MAGIC     LAG(element.value:type::STRING) OVER (ORDER BY element.pos) AS prev_element_type,
# MAGIC     LEAD(element.value:type::STRING) OVER (ORDER BY element.pos) AS next_element_type
# MAGIC   FROM parsed,
# MAGIC     LATERAL variant_explode(parsed.parsed:document:elements) AS element
# MAGIC   WHERE element.value:content IS NOT NULL
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM flattened
# MAGIC ORDER BY element_position
# MAGIC LIMIT 25

# COMMAND ----------

# DBTITLE 1,Reconstruct sections from title elements
# MAGIC %sql
# MAGIC -- Group content by sections (titles act as section headers)
# MAGIC -- This reconstructs the document's logical hierarchy
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC ),
# MAGIC elements_with_pos AS (
# MAGIC   SELECT
# MAGIC     element.pos AS pos,
# MAGIC     element.value:type::STRING AS element_type,
# MAGIC     element.value:content::STRING AS content
# MAGIC   FROM parsed,
# MAGIC     LATERAL variant_explode(parsed.parsed:document:elements) AS element
# MAGIC   WHERE element.value:content IS NOT NULL
# MAGIC ),
# MAGIC sections AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- Create section boundaries: each title starts a new section
# MAGIC     SUM(CASE WHEN element_type IN ('title', 'section_header') THEN 1 ELSE 0 END)
# MAGIC       OVER (ORDER BY pos) AS section_id
# MAGIC   FROM elements_with_pos
# MAGIC )
# MAGIC SELECT
# MAGIC   section_id,
# MAGIC   -- Get section title
# MAGIC   FIRST_VALUE(CASE WHEN element_type IN ('title', 'section_header') THEN content END)
# MAGIC     OVER (PARTITION BY section_id ORDER BY pos) AS section_title,
# MAGIC   element_type,
# MAGIC   SUBSTRING(content, 1, 120) AS content_preview,
# MAGIC   LENGTH(content) AS content_length
# MAGIC FROM sections
# MAGIC WHERE section_id > 0
# MAGIC ORDER BY section_id, pos
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,Concatenate section content
# MAGIC %sql
# MAGIC -- Aggregate text within sections to create full-section documents
# MAGIC WITH parsed AS (
# MAGIC   SELECT ai_parse_document(content, MAP('version', '2.0')) AS parsed
# MAGIC   FROM READ_FILES('/Volumes/dbacademy/ka_all/vol_all/listings/airbnb_listings.pdf', format => 'binaryFile')
# MAGIC ),
# MAGIC elements_with_pos AS (
# MAGIC   SELECT
# MAGIC     element.pos AS pos,
# MAGIC     element.value:type::STRING AS element_type,
# MAGIC     element.value:content::STRING AS content
# MAGIC   FROM parsed,
# MAGIC     LATERAL variant_explode(parsed.parsed:document:elements) AS element
# MAGIC   WHERE element.value:content IS NOT NULL
# MAGIC ),
# MAGIC sections AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     SUM(CASE WHEN element_type IN ('title', 'section_header') THEN 1 ELSE 0 END)
# MAGIC       OVER (ORDER BY pos) AS section_id
# MAGIC   FROM elements_with_pos
# MAGIC )
# MAGIC SELECT
# MAGIC   section_id,
# MAGIC   MAX(CASE WHEN element_type IN ('title', 'section_header') THEN content END) AS section_title,
# MAGIC   COUNT(*) AS num_elements,
# MAGIC   SUM(LENGTH(content)) AS total_chars,
# MAGIC   -- Reconstruct full section text by concatenating elements in order
# MAGIC   concat_ws('\n\n',
# MAGIC     collect_list(content)
# MAGIC   ) AS full_section_text
# MAGIC FROM sections
# MAGIC WHERE section_id > 0
# MAGIC GROUP BY section_id
# MAGIC ORDER BY section_id

# COMMAND ----------

# DBTITLE 1,Section F Header
# MAGIC %md
# MAGIC ## F. Working with the Structured Listings Table
# MAGIC
# MAGIC Now let's work with the `airbnb_structured_listings` table created in Demo 04. This table contains classified and extracted fields from `ai_parse_document` output. We'll apply advanced transformations to analyze and reshape this data.

# COMMAND ----------

# DBTITLE 1,Explore structured table schema
# MAGIC %sql
# MAGIC -- Explore the structured table's data types and content distribution
# MAGIC DESCRIBE TABLE airbnb_structured_listings

# COMMAND ----------

# DBTITLE 1,Category distribution analysis
# MAGIC %sql
# MAGIC -- Analyze the distribution of classified categories
# MAGIC SELECT
# MAGIC   category::STRING AS category,
# MAGIC   COUNT(*) AS count,
# MAGIC   ROUND(AVG(LENGTH(content)), 0) AS avg_content_length,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
# MAGIC FROM airbnb_structured_listings
# MAGIC GROUP BY category::STRING
# MAGIC ORDER BY count DESC

# COMMAND ----------

# DBTITLE 1,VARIANT field analysis from extraction
# MAGIC %sql
# MAGIC -- Analyze the extracted VARIANT fields: how often are they populated?
# MAGIC -- This helps understand extraction quality and coverage
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   SUM(CASE WHEN property_name IS NOT NULL AND property_name::STRING != 'null' THEN 1 ELSE 0 END) AS has_property_name,
# MAGIC   SUM(CASE WHEN neighborhood IS NOT NULL AND neighborhood::STRING != 'null' THEN 1 ELSE 0 END) AS has_neighborhood,
# MAGIC   SUM(CASE WHEN property_type IS NOT NULL AND property_type::STRING != 'null' THEN 1 ELSE 0 END) AS has_property_type,
# MAGIC   SUM(CASE WHEN key_amenities IS NOT NULL AND key_amenities::STRING != 'null' THEN 1 ELSE 0 END) AS has_key_amenities
# MAGIC FROM airbnb_structured_listings

# COMMAND ----------

# DBTITLE 1,Pivot by category and property type
# MAGIC %sql
# MAGIC -- Pivot: show property types by category for a cross-tabulation view
# MAGIC SELECT
# MAGIC   property_type::STRING AS property_type,
# MAGIC   SUM(CASE WHEN category::STRING = 'property_description' THEN 1 ELSE 0 END) AS descriptions,
# MAGIC   SUM(CASE WHEN category::STRING = 'pricing_information' THEN 1 ELSE 0 END) AS pricing,
# MAGIC   SUM(CASE WHEN category::STRING = 'location_details' THEN 1 ELSE 0 END) AS location,
# MAGIC   SUM(CASE WHEN category::STRING = 'amenities' THEN 1 ELSE 0 END) AS amenities,
# MAGIC   SUM(CASE WHEN category::STRING = 'host_information' THEN 1 ELSE 0 END) AS host_info,
# MAGIC   COUNT(*) AS total
# MAGIC FROM airbnb_structured_listings
# MAGIC WHERE property_type IS NOT NULL AND property_type::STRING != 'null'
# MAGIC GROUP BY property_type::STRING
# MAGIC ORDER BY total DESC

# COMMAND ----------

# DBTITLE 1,Section G Header
# MAGIC %md
# MAGIC ## G. Advanced VARIANT Patterns: JSON Construction & Nested Extraction
# MAGIC
# MAGIC These patterns show how to **construct** new VARIANT/JSON objects from existing data and how to handle deeply nested or array-valued extraction results (like `key_amenities`).

# COMMAND ----------

# DBTITLE 1,Explode amenities arrays
# MAGIC %sql
# MAGIC -- key_amenities from ai_extract is often a JSON array or comma-separated string
# MAGIC -- Let's parse and explode it to analyze individual amenities
# MAGIC WITH amenities_parsed AS (
# MAGIC   SELECT
# MAGIC     property_name::STRING AS property_name,
# MAGIC     neighborhood::STRING AS neighborhood,
# MAGIC     key_amenities::STRING AS raw_amenities,
# MAGIC     -- Try to parse as JSON array first, fall back to split on comma
# MAGIC     CASE
# MAGIC       WHEN key_amenities::STRING LIKE '[%' 
# MAGIC         THEN try_cast(key_amenities AS ARRAY<STRING>)
# MAGIC       ELSE split(key_amenities::STRING, ',')
# MAGIC     END AS amenities_array
# MAGIC   FROM airbnb_structured_listings
# MAGIC   WHERE key_amenities IS NOT NULL 
# MAGIC     AND key_amenities::STRING != 'null'
# MAGIC     AND LENGTH(key_amenities::STRING) > 2
# MAGIC )
# MAGIC SELECT
# MAGIC   property_name,
# MAGIC   neighborhood,
# MAGIC   TRIM(amenity.value) AS amenity,
# MAGIC   amenity.pos AS amenity_position
# MAGIC FROM amenities_parsed,
# MAGIC   LATERAL posexplode(amenities_array) AS amenity(pos, value)
# MAGIC WHERE TRIM(amenity.value) != ''
# MAGIC ORDER BY property_name, amenity_position
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,Top amenities frequency
# MAGIC %sql
# MAGIC -- Find the most frequently mentioned amenities across all listings
# MAGIC WITH amenities_parsed AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN key_amenities::STRING LIKE '[%' 
# MAGIC         THEN try_cast(key_amenities AS ARRAY<STRING>)
# MAGIC       ELSE split(key_amenities::STRING, ',')
# MAGIC     END AS amenities_array
# MAGIC   FROM airbnb_structured_listings
# MAGIC   WHERE key_amenities IS NOT NULL 
# MAGIC     AND key_amenities::STRING != 'null'
# MAGIC     AND LENGTH(key_amenities::STRING) > 2
# MAGIC )
# MAGIC SELECT
# MAGIC   LOWER(TRIM(amenity.value)) AS amenity,
# MAGIC   COUNT(*) AS frequency
# MAGIC FROM amenities_parsed,
# MAGIC   LATERAL posexplode(amenities_array) AS amenity(pos, value)
# MAGIC WHERE TRIM(amenity.value) != ''
# MAGIC GROUP BY LOWER(TRIM(amenity.value))
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY frequency DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Construct enriched JSON
# MAGIC %sql
# MAGIC -- Construct new structured JSON objects from the table data
# MAGIC -- Useful for API responses, downstream pipelines, or re-embedding
# MAGIC SELECT
# MAGIC   to_json(
# MAGIC     named_struct(
# MAGIC       'listing', named_struct(
# MAGIC         'name', property_name::STRING,
# MAGIC         'type', property_type::STRING,
# MAGIC         'neighborhood', neighborhood::STRING,
# MAGIC         'category', category::STRING
# MAGIC       ),
# MAGIC       'content_stats', named_struct(
# MAGIC         'char_count', LENGTH(content),
# MAGIC         'word_count', size(split(content, '\\s+')),
# MAGIC         'has_amenities', key_amenities IS NOT NULL AND key_amenities::STRING != 'null'
# MAGIC       ),
# MAGIC       'content_preview', SUBSTRING(content, 1, 200)
# MAGIC     )
# MAGIC   ) AS enriched_json
# MAGIC FROM airbnb_structured_listings
# MAGIC WHERE property_name IS NOT NULL AND property_name::STRING != 'null'
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Section H Header
# MAGIC %md
# MAGIC ## H. Building a Reusable View for Downstream Analytics
# MAGIC
# MAGIC Let's create a clean, typed view that downstream consumers can query without needing to understand VARIANT syntax. This is a best practice for making parsed document data accessible to analysts who prefer standard SQL.

# COMMAND ----------

# DBTITLE 1,Create analytics view
# MAGIC %sql
# MAGIC -- Create a clean, typed view for downstream analytics
# MAGIC CREATE OR REPLACE TEMPORARY VIEW listings_analytics AS
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     content,
# MAGIC     category::STRING AS category,
# MAGIC     property_name::STRING AS property_name,
# MAGIC     neighborhood::STRING AS neighborhood,
# MAGIC     property_type::STRING AS property_type,
# MAGIC     key_amenities::STRING AS raw_amenities
# MAGIC   FROM airbnb_structured_listings
# MAGIC ),
# MAGIC cleaned AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     -- Clean up null-like strings from ai_extract
# MAGIC     CASE WHEN property_name IN ('null', '', 'N/A', 'None') THEN NULL ELSE property_name END AS clean_name,
# MAGIC     CASE WHEN neighborhood IN ('null', '', 'N/A', 'None') THEN NULL ELSE neighborhood END AS clean_neighborhood,
# MAGIC     CASE WHEN property_type IN ('null', '', 'N/A', 'None') THEN NULL ELSE property_type END AS clean_property_type,
# MAGIC     -- Compute content metrics
# MAGIC     LENGTH(content) AS content_length,
# MAGIC     size(split(content, '\\s+')) AS word_count,
# MAGIC     size(split(content, '[.!?]')) - 1 AS sentence_count
# MAGIC   FROM base
# MAGIC )
# MAGIC SELECT
# MAGIC   clean_name AS property_name,
# MAGIC   clean_neighborhood AS neighborhood,
# MAGIC   clean_property_type AS property_type,
# MAGIC   category,
# MAGIC   content_length,
# MAGIC   word_count,
# MAGIC   sentence_count,
# MAGIC   ROUND(content_length * 1.0 / GREATEST(word_count, 1), 1) AS avg_word_length,
# MAGIC   raw_amenities,
# MAGIC   content
# MAGIC FROM cleaned;
# MAGIC
# MAGIC -- Query the clean view
# MAGIC SELECT * FROM listings_analytics
# MAGIC ORDER BY content_length DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Conclusion
# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this bonus demo, you explored:
# MAGIC
# MAGIC 1. **VARIANT fundamentals** — colon path notation, bracket indexing, and type casting
# MAGIC 2. **Document structure analysis** — element types, page metadata, and bounding boxes
# MAGIC 3. **Array operations** — `transform()`, `filter()`, `aggregate()`, and `exists()` on VARIANT arrays
# MAGIC 4. **Defensive patterns** — `try_cast`, `COALESCE`, null handling for production robustness
# MAGIC 5. **Flattening strategies** — positional ordering, section reconstruction, and content aggregation
# MAGIC 6. **Advanced reshaping** — pivots, amenity explosion, JSON construction, and analytics views
# MAGIC
# MAGIC These patterns apply to any `ai_parse_document` output and are essential for building production data pipelines over unstructured documents.
