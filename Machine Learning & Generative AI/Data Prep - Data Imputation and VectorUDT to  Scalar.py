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
# MAGIC # Demo - Data Imputation and Transformation Pipeline
# MAGIC
# MAGIC In this demo, we'll delve into techniques such as preparing modeling data, including splitting data, handling missing values, encoding categorical features, and standardizing features. We will also discuss outlier removal and coercing columns to the correct data type. By the end, you will have a comprehensive understanding of data preparation for modeling and feature preparation.
# MAGIC
# MAGIC **Learning Objectives:**
# MAGIC
# MAGIC By the end of this demo, you will be able to: 
# MAGIC
# MAGIC - Coerce columns to be the correct data type based on feature or target variable type.
# MAGIC - Identify and remove outliers from the modeling data.
# MAGIC - Drop rows/columns that contain missing values.
# MAGIC - Impute categorical missing values with the mode value.
# MAGIC - Replace missing values with a specified replacement value.
# MAGIC - One-hot encode categorical features.
# MAGIC - Perform ordered indexing as an alternative categorical feature preparation for random forest modeling.
# MAGIC - Apply pre-existing embeddings to categorical features.
# MAGIC - Standardize features in a training set.
# MAGIC - Split modeling data into a train-test-holdout split as part of a modeling process.
# MAGIC - Split training data into cross-validation sets as part of a modeling process.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC    - Click **More** in the drop-down.
# MAGIC    
# MAGIC    - In the **Attach to an existing compute resource** window, use the first drop-down to select your unique cluster.
# MAGIC
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC
# MAGIC 2. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC
# MAGIC 3. Wait a few minutes for the cluster to start.
# MAGIC
# MAGIC 4. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need to use one of the following Databricks runtime(s): **16.3.x-cpu-ml-scala2.12**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo. Execute the following cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-2.1

# COMMAND ----------

# MAGIC %md
# MAGIC **Other Conventions:**
# MAGIC
# MAGIC Throughout this demo, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Data Cleaning and Imputation
# MAGIC
# MAGIC - Load the dataset from the specified path using Spark and read it as a DataFrame.
# MAGIC
# MAGIC - Drop any rows with missing values from the DataFrame using the **`dropna()`** method.
# MAGIC
# MAGIC - Fill any remaining missing values in the DataFrame with the 0 using the **`fillna()`** method.
# MAGIC
# MAGIC - Create a temporary view named as **`telco_customer_churn`**

# COMMAND ----------

# Load dataset with spark
shared_volume_name = 'telco' # From Marketplace
csv_name = 'telco-customer-churn-noisy' # CSV file name
dataset_path = f"{DA.paths.datasets.telco}/{shared_volume_name}/{csv_name}.csv" # Full path


telco_df = spark.read.csv(dataset_path, header="true", inferSchema="true", multiLine="true", escape='"')

# telco_df.printSchema()
display(telco_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Coerce/Fix Data Types
# MAGIC
# MAGIC Even though most of the data types are correct let's do the following to have a better memory footprint of the dataframe in memory
# MAGIC
# MAGIC * Convert **`SeniorCitizen`** and **`Churn`** binary columns to boolean type.
# MAGIC
# MAGIC * Converting the **`tenure`** column to a long integer using **`.selectExpr`** and reordering the columns.
# MAGIC
# MAGIC * Using **`spark.sql`** to convert **`Partner`** , **`Dependents`**, **`PhoneService`** and **`PaperlessBilling`** columns to boolean, and reordering the columns. Then, saving the dataframe as a DELTA table.

# COMMAND ----------

from pyspark.sql.types import BooleanType, ShortType, IntegerType
from pyspark.sql.functions import col, when


binary_columns = ["SeniorCitizen", "Churn"]
telco_customer_churn_df = telco_df
for column in binary_columns:
    telco_customer_churn_df = telco_customer_churn_df.withColumn(column, col(column).cast(BooleanType()))

telco_customer_churn_df.select(*binary_columns).printSchema()

# COMMAND ----------

# PhoneService & PaperlessBilling to new boolean using spark.sql and re-order columns
telco_customer_churn_df.createOrReplaceTempView("telco_customer_churn_temp_view")

telco_customer_casted_df = spark.sql("""
    SELECT
        customerID,
        BOOLEAN(Dependents),
        BOOLEAN(Partner),
        BOOLEAN(PhoneService),
        BOOLEAN(PaperlessBilling),
        * 
        EXCEPT (customerID, Dependents, Partner, PhoneService, PaperlessBilling, Churn),
        Churn
    FROM telco_customer_churn_temp_view
""")

telco_customer_casted_df.select("Dependents","Partner","PaperlessBilling", "PhoneService").printSchema()

# COMMAND ----------

# Tenure months to Long/Integer using .selectExpr
telco_customer_casted_df = telco_customer_casted_df.selectExpr("* except(tenure)", "cast(tenure as long) tenure")
telco_customer_casted_df.select("tenure").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Handling Outliers
# MAGIC
# MAGIC We will see how to handle outliers in column by identifying and addressing data points that fall far outside the typical range of values in a dataset. Common methods for handling outliers include removing them, filtering, transforming the data, or replacing outliers with more representative values. 
# MAGIC
# MAGIC Follow these steps for handling outliers:
# MAGIC * Create a new silver table named as **`telco_customer_full_silver`** by appending **`silver`** to the original table name and then accessing it using Spark SQL.
# MAGIC
# MAGIC * Filtering out outliers from the **`TotalCharges`** column by removing rows where the column value exceeds the specified cutoff value.

# COMMAND ----------

telco_customer_name_full = "telco_customer_full"

# [OPTIONAL] Save as DELTA table (silver)
telco_customer_full_silver = f"{telco_customer_name_full}_silver"
telco_customer_casted_df.write.mode("overwrite").option("mergeSchema",True).saveAsTable(telco_customer_full_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC Filtering out outliers from the **`TotalCharges`** column by removing rows where the column value exceeds the specified cutoff value (e.g. negative values)

# COMMAND ----------

# print(telco_customer_casted_df)
telco_customer_casted_df.select("TotalCharges", "tenure").display()

# COMMAND ----------

from pyspark.sql.functions import col


# Remove customers with negative TotalCharges 
TotalCharges_cutoff = 0

# Use .filter method and SQL col() function
telco_no_outliers_df = telco_customer_casted_df.filter(\
    (col("TotalCharges") > TotalCharges_cutoff) | \
    (col("TotalCharges").isNull())) # Keep Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC **Removing outliers from PaymentMethod**
# MAGIC * Identify the two lowest occurrence groups in the **`PaymentMethod`** column and calculating the total count and average **`MonthlyCharges`** for each group.
# MAGIC
# MAGIC * Removing customers from the identified low occurrence groups in the **`PaymentMethod`** column to filter out outliers.
# MAGIC
# MAGIC * Create a new dataframe **`telco_filtered_df`** containing the filtered data.
# MAGIC
# MAGIC * Comparing the count of records before and after by dividing the count of **`telco_casted_full_df`** and **`telco_no_outliers_df`** dataframe removing outliers and then materializing the resulting dataframe as a new table.

# COMMAND ----------

from pyspark.sql.functions import col, count, avg


# Identify 2 lowest group occurrences
group_var = "PaymentMethod"
stats_df = telco_no_outliers_df.groupBy(group_var) \
                      .agg(count("*").alias("Total"),\
                           avg("MonthlyCharges").alias("MonthlyCharges")) \
                      .orderBy(col("Total").desc())

# Display
display(stats_df)

# COMMAND ----------

# Gather 2 groups with the lowest counts assuming the count threshold is below 20% of the full dataset and monthly charges < $70
N = telco_no_outliers_df.count()  # total count
lower_groups = [elem[group_var] if elem[group_var] is not None else "null" for elem in stats_df.tail(2) if elem['Total']/N < 0.2 and elem['MonthlyCharges'] < 70]
print(f"Removing groups: {','.join(lower_groups)}")

# COMMAND ----------

# Filter/Remove listings from these low occurrence groups while keeping null occurrences
telco_no_outliers_df = telco_no_outliers_df.filter( \
    ~col(group_var).isin(lower_groups) | \
    col(group_var).isNull())

# COMMAND ----------

# Count/Compare datasets before/after removing outliers
print(f"Count - Before: {telco_customer_casted_df.count()} / After: {telco_no_outliers_df.count()}")

# COMMAND ----------

# Materialize/Snap table [OPTIONAL/for instructor only]
telco_no_outliers_df.write.mode("overwrite").saveAsTable(telco_customer_full_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Handling Missing Values
# MAGIC
# MAGIC To Handle missing values in dataset we need to identify columns with high percentages of missing data and drops those columns. Then, we will remove rows with missing values. Numeric columns are imputed with 0, and string columns are imputed with 'N/A'. Overall, the code demonstrates a comprehensive approach to handling missing values in the dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Delete Columns 
# MAGIC
# MAGIC * Create a DataFrame called **`missing_df`** to count the missing values per column in the **`telco_no_outliers_df`** dataset.
# MAGIC
# MAGIC * The **`missing_df`** DataFrame is then transposed for better readability using the TransposeDF function, which allows for easier analysis of missing values.

# COMMAND ----------

from pyspark.sql.functions import col, when, count, concat_ws, collect_list # isnan


def calculate_missing(input_df, show=True):
  """
  Helper function to calculate and display missing data
  """

  # First get count of missing values per column to get a singleton row DF
  missing_df_ = input_df.select([count(when(col(c).contains('None') | \
                                                  col(c).contains('NULL') | \
                                                  (col(c) == '' ) | \
                                                  col(c).isNull(), c)).alias(c) \
                                                  for c in input_df.columns
                                            ])

  # Transpose for better readability
  def TransposeDF(df, columns, pivotCol):
    """Helper function to transpose spark dataframe"""
    columnsValue = list(map(lambda x: str("'") + str(x) + str("',")  + str(x), columns))
    stackCols = ','.join(x for x in columnsValue)
    df_1 = df.selectExpr(pivotCol, "stack(" + str(len(columns)) + "," + stackCols + ")")\
            .select(pivotCol, "col0", "col1")
    final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1"))))\
                  .withColumnRenamed("col0", pivotCol)
    return final_df

  missing_df_out_T = TransposeDF(
    spark.createDataFrame([{"Column":"Number of Missing Values"}]).join(missing_df_),
    missing_df_.columns,
    "Column").withColumn("Number of Missing Values", col("Number of Missing Values").cast("long"))

  if show:
    display(missing_df_out_T.orderBy("Number of Missing Values", ascending=False))

  return missing_df_out_T

missing_df = calculate_missing(telco_no_outliers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop columns with more than x% of missing rows**
# MAGIC
# MAGIC Columns with more than 60% missing data are identified and stored in the **`to_drop_missing`** list, and these columns are subsequently dropped from the **`telco_no_outliers_df`** dataset.

# COMMAND ----------

per_thresh = 0.6  # Drop if column has more than 60% missing data

N = telco_no_outliers_df.count()  # total count
to_drop_missing = [x.asDict()['Column'] for x in missing_df.select("Column").where(col("Number of Missing Values") / N >= per_thresh).collect()]

# COMMAND ----------



# COMMAND ----------

print(f"Dropping columns {to_drop_missing} for more than {per_thresh * 100}% missing data")
telco_no_missing_df = telco_no_outliers_df.drop(*to_drop_missing)
display(telco_no_missing_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop rows containing specific numbers of missing columns/fields**
# MAGIC
# MAGIC Rows with more than 80% the columns missing values are dropped using the **`na.drop()`** and the remaining missing values in numeric columns are imputed with 0, while missing values in string columns are imputed with 'N/A'.

# COMMAND ----------

n_cols = len(telco_no_missing_df.columns)
telco_no_missing_df = telco_no_missing_df.na.drop(how='any', thresh=round(n_cols*.80)) # Drop rows where number of non-null values is below threshold

# COMMAND ----------

# Count/Compare datasets before/after removing missing
print(f"Count - Before: {telco_no_outliers_df.count()} / After: {telco_no_missing_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Impute Missing Data
# MAGIC
# MAGIC Replace missing values with a specified replacement value.
# MAGIC
# MAGIC * The **`num_cols`** and **`string_cols`** lists are created to identify numeric and string columns in the dataset, respectively.
# MAGIC
# MAGIC * Finally, missing values in the numeric and string columns are imputed with appropriate values using the **`na.fill()`**, resulting in the **`telco_imputed_df`** dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC **Replace boolean missing with `False`**

# COMMAND ----------

from pyspark.sql.types import BooleanType


# Get a list of boolean columns
bool_cols = [c.name for c in telco_no_missing_df.schema.fields if (c.dataType == BooleanType())]

# Impute
telco_imputed_df = telco_no_missing_df.na.fill(value=False, subset=bool_cols)

# COMMAND ----------

# MAGIC %md
# MAGIC **Replace string missing with `No`**
# MAGIC
# MAGIC All string cols except `gender`, `Contract` and `PaymentMethod`

# COMMAND ----------

from pyspark.sql.types import StringType


# Get list of string cols
to_exclude = ["customerID", "gender", "Contract", "PaymentMethod"]
string_cols = [c.name for c in telco_no_missing_df.drop(*to_exclude).schema.fields if c.dataType == StringType()]

# Impute
telco_imputed_df = telco_imputed_df.na.fill(value='No', subset=string_cols)

# COMMAND ----------

# Compare missing stats again
calculate_missing(telco_imputed_df)

# COMMAND ----------

telco_imputed_df.write.mode("overwrite").saveAsTable(f"{DA.catalog_name}.{DA.schema_name}.telco_imputed_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Encoding Categorical Features
# MAGIC
# MAGIC In this section, we will one-hot encode categorical/string features using Spark MLlib's `OneHotEncoder` estimator.
# MAGIC
# MAGIC If you are unfamiliar with one-hot encoding, there's a description below. If you're already familiar, you can skip ahead to the **One-hot encoding in Spark MLlib** section toward the bottom of the cell.
# MAGIC
# MAGIC #### Categorical features in machine learning
# MAGIC
# MAGIC Many machine learning algorithms are not able to accept categorical features as inputs. As a result, data scientists and machine learning engineers need to determine how to handle them. 
# MAGIC
# MAGIC An easy solution would be remove the categorical features from the feature set. While this is quick, **you are removing potentially predictive information** &mdash; so this usually isn't the best strategy.
# MAGIC
# MAGIC Other options include ways to represent categorical features as numeric features. A few common options are:
# MAGIC
# MAGIC 1. **One-hot encoding**: create dummy/binary variables for each category
# MAGIC 2. **Target/label encoding**: replace each category value with a value that represents the target variable (e.g. replace a specific category value with the mean of the target variable for rows with that category value)
# MAGIC 3. **Embeddings**: use/create a vector-representation of meaningful words in each category's value
# MAGIC
# MAGIC Each of these options can be really useful in different scenarios. We're going to focus on one-hot encoding here.
# MAGIC
# MAGIC #### One-hot encoding basics
# MAGIC
# MAGIC One-hot encoding creates a binary/dummy feature for each category in each categorical feature.
# MAGIC
# MAGIC In the example below, the feature **Animal** is split into three binary features &mdash; one for each value in **Animal**. Each binary feature's value is equal to 1 if its respective category value is present in **Animal** for each row. If its category value is not present in the row, the binary feature's value will be 0.
# MAGIC
# MAGIC ![one-hot-encoding](../Includes/images/one-hot-encoding.png)
# MAGIC
# MAGIC #### One-hot encoding in Spark MLlib
# MAGIC
# MAGIC Even if you understand one-hot encoding, it's important to learn how to perform it using Spark MLlib.
# MAGIC
# MAGIC To one-hot encode categorical features in Spark MLlib, we are going to use two classes: [the **`StringIndexer`** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.StringIndexer.html#pyspark.ml.feature.StringIndexer) and [the **`OneHotEncoder`** class](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.OneHotEncoder.html#pyspark.ml.feature.OneHotEncoder).
# MAGIC
# MAGIC * The `StringIndexer` class indexes string-type columns to a numerical index. Each unique value in the string-type column is mapped to a unique integer.
# MAGIC * The `OneHotEncoder` class accepts indexed columns and converts them to a one-hot encoded vector-type feature.
# MAGIC
# MAGIC #### Applying the `StringIndexer` -> `OneHotEncoder` -> `VectorAssembler`workflow
# MAGIC
# MAGIC First, we'll need to index the categorical features of the DataFrame. `StringIndexer` takes a few arguments:
# MAGIC
# MAGIC 1. A list of categorical columns to index.
# MAGIC 2. A list names for the indexed columns being created.
# MAGIC 3. Directions for how to handle new categories when transforming data.
# MAGIC
# MAGIC Because `StringIndexer` has to learn which categories are present before indexing, it's an **estimator** &mdash; remember that means we need to call its `fit` method. Its result can then be used to transform our data.

# COMMAND ----------

sample_df = telco_imputed_df.select("Contract").distinct()
sample_df.show()

# COMMAND ----------

from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col


# StringIndexer
string_cols = ["Contract"]
index_cols = [column + "_index" for column in string_cols]

string_indexer = StringIndexer(inputCols=string_cols, outputCols=index_cols, handleInvalid="skip")
string_indexer_model = string_indexer.fit(sample_df)
indexed_df = string_indexer_model.transform(sample_df)

indexed_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Once our data has been indexed, we are ready to use the `OneHotEncoder` estimator.
# MAGIC
# MAGIC <!-- <img src="https://files.training.databricks.com/images/icon_hint_24.png"/>&nbsp; -->
# MAGIC **💡Hint:** Look at the [`OneHotEncoder` documentation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.feature.OneHotEncoder.html#pyspark.ml.feature.OneHotEncoder) and our previous Spark MLlib workflows that use estimators for guidance.

# COMMAND ----------

from pyspark.ml.feature import OneHotEncoder


# Create a list of one-hot encoded feature names
ohe_cols = [column + "_ohe" for column in string_cols]

# Instantiate the OneHotEncoder with the column lists
ohe = OneHotEncoder(inputCols=index_cols, outputCols=ohe_cols, handleInvalid="keep")

# Fit the OneHotEncoder on the indexed data
ohe_model = ohe.fit(indexed_df)

# Transform indexed_df using the ohe_model
ohe_df = ohe_model.transform(indexed_df)
ohe_df.show()

# COMMAND ----------

display(ohe_df)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler


selected_ohe_cols = ["Contract_ohe"]

# Use VectorAssembler to assemble the selected one-hot encoded columns into a dense vector
assembler = VectorAssembler(inputCols=selected_ohe_cols, outputCol="features")
result_df_dense = assembler.transform(ohe_df)

# Select relevant columns for display
result_df_display = result_df_dense.select("Contract", "features")

result_df_display.show(truncate=False)

# COMMAND ----------

display(result_df_display)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Apply pre-existing embeddings to categorical/discrete features

# COMMAND ----------

# MAGIC %md
# MAGIC Let's bin **`tenure`** to convert the discrete data into bins/categories format for further analysis and modeling.

# COMMAND ----------

column_to_bin = "tenure"
display(telco_imputed_df.select(column_to_bin))

# COMMAND ----------

from pyspark.ml.feature import Bucketizer
from pyspark.sql.functions import col


# Specify bin ranges and column to bin
bucketizer = Bucketizer(
    splits=[0, 24, 48, float('Inf')],
    inputCol=column_to_bin,
    outputCol=f"{column_to_bin}_bins"
)

# Apply the bucketizer to the DataFrame
bins_df = bucketizer.transform(telco_imputed_df.select(column_to_bin))

# Recast bin numbers to integer
bins_df = bins_df.withColumn(f"{column_to_bin}_bins", col(f"{column_to_bin}_bins").cast("integer"))

# Display the result
display(bins_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Map back to human-readable embedding scores

# COMMAND ----------

bins_embedded_df = (
  bins_df.withColumn(f"{column_to_bin}_embedded", col(f"{column_to_bin}_bins").cast(StringType()))
         .replace(to_replace = 
                  {
                    "0":"<2y",
                    "1":"2-4y",
                    "2":"<4y"
                  },
                  subset=[f"{column_to_bin}_embedded"])
)
display(bins_embedded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Ordered Indexing
# MAGIC
# MAGIC Perform ordered indexing as an alternative categorical feature preparation for random forest modeling.
# MAGIC
# MAGIC Some categoricals are in fact `ordinal` and thus may require additional/manual encoding

# COMMAND ----------

ordinal_cat = "Contract"
telco_imputed_df.select(ordinal_cat).distinct().show(truncate=False)

# COMMAND ----------

# Define Ordinal (category:index) map/dict
ordered_list = [
    "Month-to-month",
    "One year",
    "Two year"
]

ordinal_dict = {category: f"{index+1}" for index, category in enumerate(ordered_list)}
display(ordinal_dict)

# COMMAND ----------

# Create a new column with ordered indexing
from pyspark.sql.functions import expr


ordinal_df = (
    telco_imputed_df
    .withColumn(f"{ordinal_cat}_ord", col(ordinal_cat)) # Duplicate
    .replace(to_replace=ordinal_dict, subset=[f"{ordinal_cat}_ord"]) # Map 
    .withColumn(f"{ordinal_cat}_ord", col(f"{ordinal_cat}_ord").cast('int')) # Cast to integer
)

display(ordinal_df.select(ordinal_cat, f"{ordinal_cat}_ord"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Splitting Data (for Cross-Validation)
# MAGIC
# MAGIC Split modeling data into a train-test-holdout split as part of a modeling process
# MAGIC
# MAGIC In this section, we will perform the best-practice workflow for a train-test split using the Spark DataFrame API.
# MAGIC
# MAGIC Recall that due to things like changing cluster configurations and data partitioning, it can be difficult to ensure a reproducible train-test split. As a result, we recommend:
# MAGIC
# MAGIC 1. Split the data using the **same random seed**
# MAGIC 2. Write out the train and test DataFrames
# MAGIC
# MAGIC <!-- <img src="https://files.training.databricks.com/images/icon_hint_24.png"/>&nbsp; -->
# MAGIC **💡Hint:** Check out the [**`randomSplit`** documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.randomSplit.html).

# COMMAND ----------

# Split with 80 percent of the data in train_df and 20 percent of the data in test_df
train_df, test_df = telco_imputed_df.randomSplit([.8, .2], seed=42)

# COMMAND ----------

# Materialize (OPTIONAL)
train_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{DA.catalog_name}.{DA.schema_name}.telco_customers_train")
test_df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{DA.catalog_name}.{DA.schema_name}.telco_customers_baseline")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Standardize Features in a Training Set
# MAGIC
# MAGIC For sake of example, we'll pick a column without missing data (e.g. `MonthlyCharges`)

# COMMAND ----------

from pyspark.ml.feature import StandardScaler, RobustScaler, VectorAssembler


num_cols_to_scale = ["MonthlyCharges"] # num_cols
assembler = VectorAssembler().setInputCols(num_cols_to_scale).setOutputCol("numerical_assembled")
train_assembled_df = assembler.transform(train_df.select(*num_cols_to_scale))
test_assembled_df = assembler.transform(test_df.select(*num_cols_to_scale))

# Define scaler and fit on training set
scaler = RobustScaler(inputCol="numerical_assembled", outputCol="numerical_scaled")
scaler_fitted = scaler.fit(train_assembled_df)


# Apply to both training and test set
train_scaled_df = scaler_fitted.transform(train_assembled_df)
test_scaled_df = scaler_fitted.transform(test_assembled_df)

# COMMAND ----------

display(train_scaled_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert VectorUDT back to Scalar

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType, DoubleType

vector_to_array_udf = udf(lambda vector: vector.toArray().tolist(), ArrayType(DoubleType()))

display(train_scaled_df.withColumn("numerical_dense", vector_to_array_udf("numerical_scaled")).select(col("numerical_dense")).withColumn("scaled",explode("numerical_dense")))


# COMMAND ----------

print("Peek at Training set")
train_scaled_df.show(5)

# COMMAND ----------

print("Peek at Test set")
test_scaled_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Impute categorical missing values with the mode value using sparkml
# MAGIC
# MAGIC How to handle missing data only at training time and bake as part of inference pipeline to avoid data leakage and ensure that observation with missing data is used for training.

# COMMAND ----------

categorical_cols_to_impute = ["PaymentMethod"] # string_cols

# COMMAND ----------

# MAGIC %md
# MAGIC Index categoricals first as `Imputer` doesn't handle categoricals directly

# COMMAND ----------

from pyspark.ml.feature import StringIndexer
from pyspark.sql.functions import col


# Index categorical columns using StringIndexer
cat_index_cols = [column + "_index" for column in categorical_cols_to_impute]
cat_indexer = StringIndexer(
    inputCols=categorical_cols_to_impute,
    outputCols=cat_index_cols,
    handleInvalid="keep"
)

# Fit on training set
cat_indexer_model = cat_indexer.fit(train_df.select(categorical_cols_to_impute))

# COMMAND ----------

# Transform both train & test set using the fitted StringIndexer model
cat_indexed_train_df = cat_indexer_model.transform(train_df.select(*categorical_cols_to_impute))
cat_indexed_test_df = cat_indexer_model.transform(test_df.select(*categorical_cols_to_impute))
# display(cat_indexed_train_df)

# COMMAND ----------

cat_indexed_train_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC The `StringIndexer` will create a new label (_e.g._ `4`) for missing when setting the `handleInvalid` flag to `keep` so it's important to keep track/revert indexes values back to `null` if we want to impute them, otherwise `null` will be treated as their own/separate category automatically.
# MAGIC
# MAGIC Alternatively for imputing categorical/strings, we can use `.fillna()` method by providing the `mode` value manually (as described above).

# COMMAND ----------

# Revert indexes to `null` for missing categories
for c in categorical_cols_to_impute:
    cat_indexed_train_df = cat_indexed_train_df.withColumn(f"{c}_index", when(col(c).isNull(), None).otherwise(col(f"{c}_index")))
    cat_indexed_test_df = cat_indexed_test_df.withColumn(f"{c}_index", when(col(c).isNull(), None).otherwise(col(f"{c}_index")))

# COMMAND ----------

cat_indexed_train_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Fit the imputer on indexed categoricals

# COMMAND ----------

from pyspark.ml.feature import Imputer


# Define 'mode' imputer
output_cat_index_cols_imputed = [col+'_imputed' for col in cat_index_cols]
mode_imputer = Imputer(
  inputCols=cat_index_cols,
  outputCols=output_cat_index_cols_imputed,
  strategy="mode"
  )

# Fit on training_df
mode_imputer_fitted = mode_imputer.fit(cat_indexed_train_df)

# COMMAND ----------

# Transform both training & test sets
cat_indexed_train_imputed_df = mode_imputer_fitted.transform(cat_indexed_train_df)
cat_indexed_test_imputed_df  = mode_imputer_fitted.transform(cat_indexed_test_df)

# COMMAND ----------

# Peek at test set
display(cat_indexed_test_imputed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC
# MAGIC This demo successfully provided a comprehensive understanding of data preparation for modeling and feature preparation, equipping you with the knowledge and skills to effectively prepare your data for modeling and analysis. We seamlessly saw how to correct data type, identifying and removing outliers, handling missing values through imputation or replacement, encoding categorical features, and standardizing features.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
