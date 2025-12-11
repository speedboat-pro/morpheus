-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create a table with nested data

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW nested_data AS
SELECT   id AS key,
         ARRAY(CAST(RAND(1) * 100 AS INT), CAST(RAND(2) * 100 AS INT), CAST(RAND(3) * 100 AS INT), CAST(RAND(4) * 100 AS INT), CAST(RAND(5) * 100 AS INT)) AS values
         ,
         ARRAY(ARRAY(CAST(RAND(1) * 100 AS INT), CAST(RAND(2) * 100 AS INT)), ARRAY(CAST(RAND(3) * 100 AS INT), CAST(RAND(4) * 100 AS INT), CAST(RAND(5) * 100 AS INT))) AS nested_values
FROM range(5)

-- COMMAND ----------

-- MAGIC %sql SELECT * FROM nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ### A simple example
-- MAGIC
-- MAGIC Let's ground the concepts with a basic transformation. In this case, the higher order function, ``transform``, will iterate over the array ``values``, apply the associated lambda function to each element, and create a new array. The lambda function, ``element + 1``, specifies how each element is manipulated. In SQL this will look like this:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, transform
-- MAGIC
-- MAGIC result_df = spark.table('nested_data').select(
-- MAGIC     col("key"),
-- MAGIC     col("values"),
-- MAGIC     transform(col("values"), lambda x: x + 1).alias("values_plus_one")
-- MAGIC )
-- MAGIC
-- MAGIC display(result_df)

-- COMMAND ----------

SELECT  key,
        values,
        TRANSFORM(values, value -> value + 1) AS values_plus_one
FROM    nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC The transformation `TRANSFORM(values, value -> value + 1)` has two components:
-- MAGIC 1. `TRANSFORM(values..)` is the higher order function. This takes an array and an anonymous function as its input. Internally transform will take care of setting up a new array, applying the anonymous function to each element, and assigning the result to the output array.
-- MAGIC 2. The `value -> value + 1` which is the anonymous function. The function is divided into two components separated by a `->` symbol:
-- MAGIC  - The argument list. In this case we only have one argument: `value`. We also support multiple arguments by creating a comma separated list of arguments enclosed by parenthesis, for example: `(x, y) -> x + y`.
-- MAGIC  - The body. This is an expression that can use the arguments and outer variables to calculate the new value. In this case we add `1` to the value `argument`.
-- MAGIC  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Capturing variables
-- MAGIC
-- MAGIC You can also use other variables than the arguments in a lambda function; this is called capture. You can use variables defined on the top level, or variables defined in intermediate lambda functions. For example, the following transform adds the `key` (top level) variable to each element in the `values` array:

-- COMMAND ----------

SELECT  key,
        values,
        TRANSFORM(values, value -> value + key) AS values_plus_key
FROM    nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Nesting
-- MAGIC
-- MAGIC If you want to transform deeply nested data, you can use nested lambda functions to do this. The following example transforms an array of integer arrays, and adds the `key` (top level) column and the size of the intermediate array to each element in the nested array.

-- COMMAND ----------

SELECT   key,
         nested_values,
         TRANSFORM(nested_values,
           values -> TRANSFORM(values,
             value -> value + key + SIZE(values))) AS new_nested_values
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md ###Supported functions
-- MAGIC
-- MAGIC #####`transform(array<T>, function<T, U>): array<U>`
-- MAGIC Transform an array<U> by applying a `function<T, U>` to each element of an input `array<T>`.
-- MAGIC
-- MAGIC The functional programming equivalent operation is `map`. This has been named `transform` in order to prevent confusion with the map expression (that creates a map from a key value expression).
-- MAGIC
-- MAGIC The following query transforms the `values` array by adding the `key` value to each element:

-- COMMAND ----------

SELECT   key,
         values,
         TRANSFORM(values, value -> value + key) transformed_values
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####`exists(array<T>, function<T, V, Boolean>): Boolean`
-- MAGIC
-- MAGIC Test whether a predicate function<T, Boolean> holds for any element in input array<T>.
-- MAGIC
-- MAGIC The following examples checks if the `values` array contains an elements for which the modulo 10 is equal to 1:

-- COMMAND ----------

SELECT   key,
         values,
         EXISTS(values, value -> value % 10 == 1) filtered_values
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####`filter(array<T>, function<T, Boolean>): array<T>`
-- MAGIC
-- MAGIC Filter an output `array<T>` from an input `array<T>` by only only adding elements for which the predicate `function<T, boolean>` holds.
-- MAGIC
-- MAGIC The following examples filters the `values` array such that only elements with a `value > 50` are allowed:

-- COMMAND ----------

SELECT   key,
         values,
         FILTER(values, value -> value > 50) filtered_values
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####`aggregate(array<T>, B, function<B, T, B>, function<B, R>): R`
-- MAGIC
-- MAGIC Reduce the elements of `array<T>` into a single value `R` by merging the elements into a buffer `B` using `function<B, T, B>` and by applying a finish `function<B, R>` on the final buffer. The initial value `B` is determined by a zero expression. The finalize function is optional, if you do not specify the function the finalize function the identity function `(id -> id)` is used.
-- MAGIC
-- MAGIC This is the only higher order function that takes two lambda functions.
-- MAGIC
-- MAGIC The following example sums (aggregates) the `values` array into a single (sum) value. Both a version with a finalize function (`summed_values`) and one without a finalize function `summed_values_simple` is shown:

-- COMMAND ----------

SELECT   key,
         values,
         REDUCE(values, 0, (value, acc) -> value + acc, acc -> acc) summed_values,
         REDUCE(values, 0, (value, acc) -> value + acc) summed_values_simple
FROM     nested_data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can also compute more complex aggregates. The code below shows the computation of the geometric mean of the array elements.

-- COMMAND ----------

SELECT key,
       values,
       AGGREGATE(
         values,
         named_struct('product', CAST(1.0 AS DECIMAL(2,1)), 'N', 0),
         (buffer, value) -> named_struct('product', CAST(value * buffer.product AS DECIMAL(2,1)), 'N', buffer.N + 1),
         buffer -> POWER(buffer.product, 1.0 / buffer.N)
       ) AS geomean
FROM nested_data

-- COMMAND ----------


