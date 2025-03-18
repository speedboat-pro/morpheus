-- Databricks notebook source
-- MAGIC %md # Transforming Complex Data Types in Spark SQL
-- MAGIC
-- MAGIC In this notebook we're going to go through some data transformation examples using Spark SQL. Spark SQL supports many
-- MAGIC built-in transformation functions natively in SQL.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC # Convenience function for turning JSON strings into DataFrames.
-- MAGIC def jsonToDataFrame(json, schema=None):
-- MAGIC   # SparkSessions are available with Spark 2.0+
-- MAGIC   reader = spark.read
-- MAGIC   if schema:
-- MAGIC     reader.schema(schema)
-- MAGIC   reader.json(sc.parallelize([json])).createOrReplaceTempView("events")

-- COMMAND ----------

-- MAGIC %md <b>Selecting from nested columns</b> - Dots (`"."`) can be used to access nested columns for structs and maps.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Using a struct
-- MAGIC schema = StructType().add("a", StructType().add("b", IntegerType()))
-- MAGIC                           
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": {
-- MAGIC      "b": 1
-- MAGIC   }
-- MAGIC }
-- MAGIC """, schema)

-- COMMAND ----------

select a.b from events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Using a map
-- MAGIC schema = StructType().add("a", MapType(StringType(), IntegerType()))
-- MAGIC                           
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": {
-- MAGIC      "b": 1
-- MAGIC   }
-- MAGIC }
-- MAGIC """, schema)

-- COMMAND ----------

select a.b from events

-- COMMAND ----------

-- MAGIC %md <b>Flattening structs</b> - A star (`"*"`) can be used to select all of the subfields in a struct.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": {
-- MAGIC      "b": 1,
-- MAGIC      "c": 2
-- MAGIC   }
-- MAGIC }
-- MAGIC """)

-- COMMAND ----------

select a.* from events

-- COMMAND ----------

-- MAGIC %md <b>Nesting columns</b> - The `struct()` function or just parentheses in SQL can be used to create a new struct.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": 1,
-- MAGIC   "b": 2,
-- MAGIC   "c": 3
-- MAGIC }
-- MAGIC """)

-- COMMAND ----------

select named_struct("y", a) as x from events

-- COMMAND ----------

-- MAGIC %md <b>Nesting all columns</b> - The star (`"*"`) can also be used to include all columns in a nested struct.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": 1,
-- MAGIC   "b": 2
-- MAGIC }
-- MAGIC """)

-- COMMAND ----------

select struct(*) as x from events

-- COMMAND ----------

-- MAGIC %md <b>Selecting a single array or map element</b> - `getItem()` or square brackets (i.e. `[ ]`) can be used to select a single element out of an array or a map.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": [1, 2]
-- MAGIC }
-- MAGIC """)

-- COMMAND ----------

select a[0] as x from events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Using a map
-- MAGIC schema = StructType().add("a", MapType(StringType(), IntegerType()))
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": {
-- MAGIC     "b": 1
-- MAGIC   }
-- MAGIC }
-- MAGIC """, schema)

-- COMMAND ----------

select a['b'] as x from events

-- COMMAND ----------

-- MAGIC %md <b>Creating a row for each array or map element</b> - `explode()` can be used to create a new row for each element in an array or each key-value pair.  This is similar to `LATERAL VIEW EXPLODE` in HiveQL.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": [1, 2]
-- MAGIC }
-- MAGIC """)

-- COMMAND ----------

select explode(a) as x from events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC schema = StructType().add("a", MapType(StringType(), IntegerType()))
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": {
-- MAGIC     "b": 1,
-- MAGIC     "c": 2
-- MAGIC   }
-- MAGIC }
-- MAGIC """, schema)

-- COMMAND ----------

select explode(a) as (x, y) from events

-- COMMAND ----------

-- MAGIC %md <b>Collecting multiple rows into an array</b> - `collect_list()` and `collect_set()` can be used to aggregate items into an array.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC [{ "x": 1 }, { "x": 2 }]
-- MAGIC """)

-- COMMAND ----------

select collect_list(x) as x from events

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC [{ "x": 1, "y": "a" }, { "x": 2, "y": "b" }]
-- MAGIC """)
-- MAGIC

-- COMMAND ----------

select y, collect_list(x) as x from events group by y

-- COMMAND ----------

-- MAGIC %md <b>Selecting one field from each item in an array</b> - when you use dot notation on an array we return a new array where that field has been selected from each array element.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": [
-- MAGIC     {"b": 1},
-- MAGIC     {"b": 2}
-- MAGIC   ]
-- MAGIC }
-- MAGIC """)

-- COMMAND ----------

select a.b from events

-- COMMAND ----------

-- MAGIC %md <b>Parse a set of fields from a column containing json</b> - `json_tuple()` can be used to extract a fields available in a string column with json data.
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC {
-- MAGIC   "a": "{\\"b\\":1}"
-- MAGIC }
-- MAGIC """)

-- COMMAND ----------

select json_tuple(a, "b") as c from events

-- COMMAND ----------

-- MAGIC %md <b>Parse a well formed string column</b> - `regexp_extract()` can be used to parse strings using regular expressions.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC jsonToDataFrame("""
-- MAGIC [{ "a": "x: 1" }, { "a": "y: 2" }]
-- MAGIC """)

-- COMMAND ----------

select regexp_extract(a, "([a-z]):", 1) as c from events
