## The syntax was formerly `import dlt`
## Documentation: https://docs.databricks.com/aws/en/ldp/developer/python-ref

from pyspark import pipelines as dp
import pyspark.sql.functions as F

source = spark.conf.get("source")


## A. Create the bronze streaming table in your labuser.1_bronze_db schema from a JSON files in your volume
  # NOTE: read_files references the 'source' configuration key from your pipeline settings. 
  # NOTE: 'source' = '/Volumes/dbacademy/ops/your-labuser-name'
@dp.table(name = "sdp_1_bronze.status_bronze_demo12_py",
           comment = "Ingest raw JSON order status files from cloud storage",
           table_properties = {
                            "quality":"bronze",
                            "pipelines.reset.allowed":"false"
                        })
def status_bronze_demo12():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/status")
            .select(
                "*",
                F.current_timestamp().alias("processing_time"), 
                "_metadata.file_name"
            )
    )


## B. Create the silver streaming table in your labuser.2_silver_db schema (database)
@dp.table(name = "sdp_2_silver.status_silver_demo12_py",
          comment = "Order with each status and timestamp",
          table_properties = {
                            "quality":"silver"
                        })

# Expectations
@dp.expect_or_drop("valid_timestamp", "order_status_timestamp > '2021-12-25'")
@dp.expect("valid_order_status", "order_status IN ('on the way','canceled','return canceled','delivered','return processed','placed','preparing')")

def status_silver_demo12():
    return (
        dp.read_stream("sdp_1_bronze.status_bronze_demo12_py")
           .select(
                "order_id",
                "order_status",
                F.col("status_timestamp").cast("timestamp").alias("order_status_timestamp")
            )
    )


## C. Use a Materialized View to Join Two Streaming Tables
@dp.materialized_view(
    name = "sdp_3_gold.full_order_info_gold_demo12_py",
    comment = "Joining the orders and order status silver tables to view all orders with each individual status per order",
    table_properties = {
                        "quality":"gold"
                    })
def full_order_info_gold_demo12():
    return (
        dp
        .read("sdp_2_silver.status_silver_demo12_py").alias("status")
        .join(dp.read("sdp_2_silver.orders_silver_demo12_py").alias("orders"), on = "order_id", how = "inner")
        .select(
            "orders.order_id",
            "orders.order_timestamp",
            "status.order_status",
            "status.order_status_timestamp"
        )
    )


##
## D. Create Materialized Views for Cancelled and Delivered Orders
##

## CANCELLED ORDERS
@dp.materialized_view(
    name="sdp_3_gold.cancelled_orders_gold_demo12_py",
    comment="All cancelled orders",
    table_properties={
        "quality": "gold"
    }
)
def cancelled_orders_gold_demo12():
    full_orders = dp.read("sdp_3_gold.full_order_info_gold_demo12_py")
    return (
        full_orders
        .filter(F.col("order_status") == "canceled")
        .select(
            "order_id",
            "order_timestamp",
            "order_status",
            "order_status_timestamp",
            F.datediff("order_status_timestamp", "order_timestamp").alias("days_to_cancel")
        )
    )


## DELIVERED ORDERS
@dp.materialized_view(
    name="sdp_3_gold.delivered_orders_gold_demo12_py",
    comment="All delivered orders",
    table_properties={
        "quality": "gold"
    }
)
def delivered_orders_gold_demo12():
    full_orders = dp.read("sdp_3_gold.full_order_info_gold_demo12_py")
    return (
        full_orders
        .filter(F.col("order_status") == "delivered")
        .select(
            "order_id",
            "order_timestamp",
            "order_status",
            "order_status_timestamp",
            F.datediff("order_status_timestamp", "order_timestamp").alias("days_to_delivery")
        )
    )