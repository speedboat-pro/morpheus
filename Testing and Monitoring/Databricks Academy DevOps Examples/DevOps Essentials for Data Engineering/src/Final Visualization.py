# Databricks notebook source
# MAGIC %md
# MAGIC # Final Visualization

# COMMAND ----------

# MAGIC %md
# MAGIC Obtain the job parameter and store it in the variable **my_catalog**.

# COMMAND ----------

my_catalog = dbutils.widgets.get('catalog_name')
target = dbutils.widgets.get('target')
print(f'Accessing the {target} pipeline')

# COMMAND ----------

# MAGIC %md
# MAGIC Create the visualization on the **chol_age_agg**.

# COMMAND ----------

def create_pandas_df(catalog, schema, table):
    df_spark = spark.sql(f"SELECT * FROM {catalog}.default.{table}")
    df_pandas = df_spark.toPandas()
    return df_pandas

df_pandas = create_pandas_df(catalog = my_catalog, schema = 'default', table = 'chol_age_agg')
df_pandas.head()

# COMMAND ----------

def create_stacked_bar_chart(pandas_df):
    import pandas as pd
    import matplotlib.pyplot as plt


    # Pivot the DataFrame
    df_pivot = pandas_df.pivot_table(index='Age_Group', columns='HighCholest_Group', values='Total', aggfunc='sum', fill_value=0)

    # Plot the Stacked Bar Chart
    ax = df_pivot.plot(kind='bar', stacked=True, figsize=(10, 6))

    # Title and labels
    plt.title('Cholesterol Group Distribution by Age Group', fontsize=14)
    plt.xlabel('Age Group', fontsize=12)
    plt.ylabel('Total Count', fontsize=12)

    # Display the chart
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.show()

create_stacked_bar_chart(df_pandas)
