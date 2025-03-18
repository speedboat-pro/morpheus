-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create a table with a variant column
-- MAGIC Run the following query to create a table with highly nested data stored as VARIANT. The examples in this article all reference this table.

-- COMMAND ----------

CREATE CATALOG variant_sample;
USE CATALOG variant_sample;
CREATE SCHEMA variant;
USE SCHEMA variant;



-- COMMAND ----------

CREATE OR REPLACE TABLE store_data AS
SELECT parse_json(
  '{
    "store":{
        "fruit": [
          {"weight":8,"type":"apple"},
          {"weight":9,"type":"pear"}
        ],
        "basket":[
          [1,2,{"b":"y","a":"x"}],
          [3,4],
          [5,6]
        ],
        "book":[
          {
            "author":"Nigel Rees",
            "title":"Sayings of the Century",
            "category":"reference",
            "price":8.95
          },
          {
            "author":"Herman Melville",
            "title":"Moby Dick",
            "category":"fiction",
            "price":8.99,
            "isbn":"0-553-21311-3"
          },
          {
            "author":"J. R. R. Tolkien",
            "title":"The Lord of the Rings",
            "category":"fiction",
            "reader":[
              {"age":25,"name":"bob"},
              {"age":26,"name":"jack"}
            ],
            "price":22.99,
            "isbn":"0-395-19395-8"
          }
        ],
        "bicycle":{
          "price":19.95,
          "color":"red"
        }
      },
      "owner":"amy",
      "zip code":"94025",
      "fb:testid":"1234",
      "test.dot":"1"
  }'
) as raw


-- COMMAND ----------

select * from store_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Query fields in a variant column
-- MAGIC The syntax for querying JSON strings and other complex data types on Databricks applies to VARIANT data, including the following:
-- MAGIC
-- MAGIC - Use `:` to select top level fields.
-- MAGIC - Use `.`or `[<key>] `to select nested fields with named keys.
-- MAGIC - Use `[<index>] `to select values from arrays.
-- MAGIC
-- MAGIC **Note**
-- MAGIC
-- MAGIC If a field name contains a period `(.)`, you must escape it with square brackets `([ ])`. For example, the following query selects a field named `test.dot`:
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT raw:['test.dot'] FROM store_data


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extract a top-level variant field
-- MAGIC To extract a field, specify the name of the JSON field in your extraction path. Field names are always case sensitive.

-- COMMAND ----------

SELECT raw:owner FROM store_data


-- COMMAND ----------

-- Use backticks to escape special characters.
SELECT raw:["zip code"], raw:`fb:testid` FROM store_data


-- COMMAND ----------

-- MAGIC %md
-- MAGIC If a path cannot be found, the result is NULL of type VARIANT.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extract variant nested fields
-- MAGIC You specify nested fields through dot notation or using brackets. Field names are always case sensitive.

-- COMMAND ----------

-- Use dot notation
SELECT raw:store.bicycle FROM store_data


-- COMMAND ----------

-- Use brackets
SELECT raw:store['bicycle'] FROM store_data


-- COMMAND ----------

-- MAGIC %md
-- MAGIC If a path cannot be found, the result is NULL of type VARIANT.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extract values from variant arrays
-- MAGIC You index elements in arrays with brackets. Indices are 0-based.

-- COMMAND ----------

-- Index elements
SELECT raw:store.fruit[0], raw:store.fruit[1] FROM store_data


-- COMMAND ----------

-- MAGIC %md
-- MAGIC If the path cannot be found, or if the array-index is out of bounds, the result is NULL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Flatten variant objects and arrays
-- MAGIC The variant_explode table-valued generator function can be used to flatten VARIANT arrays and objects.
-- MAGIC
-- MAGIC Because variant_explode is a generator function, you use it as part of the FROM clause rather than in the SELECT list, as in the following examples:

-- COMMAND ----------

SELECT key, value
  FROM store_data,
  LATERAL variant_explode(store_data.raw:store);


-- COMMAND ----------

SELECT pos, value
  FROM store_data,
  LATERAL variant_explode(store_data.raw:store.basket[0]);


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Variant type casting rules
-- MAGIC You can store arrays and scalars using VARIANT type. When trying to cast variant types to other types, normal casting rules apply for individual values and fields, with the following additional rules.
-- MAGIC
-- MAGIC **Note**
-- MAGIC
-- MAGIC _variant_get and try_variant_get take type arguments and follow these casting rules._
-- MAGIC
-- MAGIC |Source type| Behavior|
-- MAGIC |-----|----|
-- MAGIC |`VOID`|The result is a NULL of type VARIANT.}
-- MAGIC |`ARRAY<elementType>`|The elementType must be a type that can be cast to VARIANT.|
-- MAGIC
-- MAGIC When inferring type with `schema_of_variant` or `schema_of_variant_agg`, functions fall back to VARIANT type rather than STRING type when conflicting types are present that can’t be resolved.
-- MAGIC
-- MAGIC You can use `::` or `cast` to cast values to supported data types.

-- COMMAND ----------

SELECT schema_of_variant(parse_json('{"key": 123, "data": [4, 5]}'))

-- COMMAND ----------

SELECT schema_of_variant_agg(a) FROM VALUES(parse_json('{"foo": "bar"}')) AS data(a);


-- COMMAND ----------

-- price is returned as a double, not a string
SELECT raw:store.bicycle.price::double FROM store_data


-- COMMAND ----------

-- cast into more complex types
SELECT cast(raw:store.bicycle AS STRUCT<price DOUBLE, color STRING>) bicycle FROM store_data;
-- `::` also supported
SELECT raw:store.bicycle::STRUCT<price DOUBLE, color STRING> bicycle FROM store_data;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Variant null rules
-- MAGIC Variants can contain two kinds of nulls:
-- MAGIC
-- MAGIC - **SQL NULL:** SQL `NULL`s indicate that the value is missing. These are the same `NULLs` as when dealing with structured data.
-- MAGIC - **Variant NULL:** Variant `NULLs` indicate that the variant explicitly contains a `NULL` value. These are not the same as SQL `NULLs`, because the `NULL` value is stored in the data.
-- MAGIC
-- MAGIC Use the `is_variant_null` function to determine if the variant value is a variant `NULL`.

-- COMMAND ----------

SELECT
  is_variant_null(parse_json(NULL)) AS sql_null,
  is_variant_null(parse_json('null')) AS variant_null,
  is_variant_null(parse_json('{ "field_a": null }'):field_a) AS variant_null_value,
  is_variant_null(parse_json('{ "field_a": null }'):missing) AS missing_sql_value_null


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Variant Extraction

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `try_variant` Examples

-- COMMAND ----------

SELECT variant_get(parse_json('{"key": 123, "data": [4, {"a": "hello"}, "str"]}'), '$.data[1].a', 'string')

-- COMMAND ----------

-- missing path
SELECT variant_get(parse_json('{"key": 123, "data": [4, {"a": "hello"}, "str"]}'), '$.missing', 'int')

-- COMMAND ----------

-- Invalid cast
SELECT variant_get(parse_json('{"key": 123, "data": [4, {"a": "hello"}, "str"]}'), '$.key', 'array<int>')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### `try_variant_get` Examples

-- COMMAND ----------

SELECT try_variant_get(parse_json('{"key": 123, "data": [4, {"a": "hello"}, "str"]}'), '$.data[1].a', 'string')

-- COMMAND ----------

SELECT try_variant_get(parse_json('{"key": 123, "data": [4, {"a": "hello"}, "str"]}'), '$.missing', 'int')
  null

-- COMMAND ----------

SELECT try_variant_get(parse_json('{"key": 123, "data": [4, {"a": "hello"}, "str"]}'), '$.key', 'array<int>')
  null

-- COMMAND ----------


