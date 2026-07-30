## The syntax was formerly `import dlt`
## Documentation: https://docs.databricks.com/aws/en/ldp/developer/python-ref

from pyspark import pipelines as dp
import pyspark.sql.functions as F

source = spark.conf.get("source")

################################################################################
## STEP 1: JSON -> Bronze Ingestion
## Ingestion the JSON files from cloud storage into a streaming table using Auto Loader
################################################################################
@dp.table(
    name="sdp_1_bronze.customers_bronze_raw_demo12_py",
    comment="Raw data from customers CDC feed",
    table_properties={
        "quality": "bronze"
    }
)
def customers_bronze_raw_demo12():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source}/customers")
        .select(
            "*",
            F.current_timestamp().alias("processing_time"), 
            "_metadata.file_name"
        )
        .withColumnRenamed("file_name", "source_file")
    )


################################################################################
## STEP 2: Create the Bronze Clean Streaming Table with Data Quality Enforcement
################################################################################
@dp.table(
    name="sdp_1_bronze.customers_bronze_clean_demo12_py",
    comment="Raw data from customers CDC feed",
    table_properties={
        "quality": "bronze",
    }
)
@dp.expect_or_fail("valid_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_operation", "operation IS NOT NULL")
@dp.expect("valid_name", "name IS NOT NULL or operation = 'DELETE'")
@dp.expect("valid_adress", """
    (address IS NOT NULL and 
    city IS NOT NULL and 
    state IS NOT NULL and 
    zip_code IS NOT NULL) or
    operation = "DELETE"
    """)
@dp.expect_or_drop("valid_email", """
    rlike(email, '^([a-zA-Z0-9_\\\\-\\\\.]+)@([a-zA-Z0-9_\\\\-\\\\.]+)\\\\.([a-zA-Z]{2,5})$') or 
    operation = "DELETE"
    """)
def customers_bronze_clean_demo12():
    return (
        dp
        .read_stream("sdp_1_bronze.customers_bronze_raw_demo12_py")
        .select(
            "*",
            F.from_unixtime(F.col("timestamp")).cast("timestamp").alias("timestamp_datetime")
        )
    )


################################################################################
## STEP 3: Processing CDC Data with AUTO CDC INTO
################################################################################
dp.create_streaming_table(
    name = "sdp_2_silver.scd_type_1_customers_silver_demo12_py",
    comment = 'SCD Type 2 Historical Customer Data')


dp.create_auto_cdc_flow(
    target = "sdp_2_silver.scd_type_1_customers_silver_demo12_py",
    source = "sdp_1_bronze.customers_bronze_clean_demo12_py",
    keys = ["customer_id"],
    sequence_by = "timestamp_datetime",
    apply_as_deletes = F.expr("operation = 'DELETE'"),
    except_column_list = ["operation", "timestamp", "_rescued_data"],
    stored_as_scd_type = 1
)