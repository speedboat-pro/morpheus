## The syntax was formerly `import dlt`
## Documentation: https://docs.databricks.com/aws/en/ldp/developer/python-ref

from pyspark import pipelines as dp
import pyspark.sql.functions as F

source = spark.conf.get("source")


## A. Create the bronze streaming table in your labuser.1_bronze_db schema from a JSON files in your volume
  # NOTE: read_files references the 'source' configuration key from your  pipeline settings. 
  # NOTE: 'source' = '/Volumes/dbacademy/ops/your-labuser-name'
@dp.table(name = "sdp_1_bronze.orders_bronze_demo6_py",
           comment = "Ingest order JSON files from cloud storage",
           table_properties = {
                            "quality":"bronze",
                            "pipelines.reset.allowed":"false"
                        })
def orders_bronze_demo6():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                "*",
                F.current_timestamp().alias("processing_time"), 
                "_metadata.file_name"
            )
    )


## B. Create the silver streaming table in your labuser.2_silver_db schema (database)
@dp.table(name = "sdp_2_silver.orders_silver_demo6_py",
           comment = "Silver clean orders table",
           table_properties = {
                            "quality":"silver"
                        })

# Expectations
@dp.expect("valid_notifications", "notifications IN ('Y','N')")
@dp.expect_or_drop("valid_date", F.col("order_timestamp") > "2021-12-25")
@dp.expect_or_fail("valid_id", F.col("customer_id").isNotNull())

def orders_silver_demo6():
    return (
        dp.read_stream("sdp_1_bronze.orders_bronze_demo6_py")
            .select(
                "order_id",
                F.col("order_timestamp").cast("timestamp").alias("order_timestamp"),
                "customer_id",
                "notifications"
            )
    )


## C. Create the materialized view aggregation from the orders_silver table with the summarization
@dp.materialized_view(name = "sdp_3_gold.gold_orders_by_date_demo6_py",
                      comment = "Aggregate gold data for downstream analysis",
                      table_properties = {
                            "quality":"gold"
                        })
def gold_orders_by_date_demo6():
    return (
        dp.read("sdp_2_silver.orders_silver_demo6_py")
            .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
    )