from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


@dp.foreach_batch_sink(name="foreachbatch_target_sink")
def foreachbatch_target_sink(df, batch_id):
    window = Window.partitionBy("customer_id").orderBy(col("timestamp_datetime").desc())
    deduped = df.withColumn("_rank", row_number().over(window)).filter("_rank = 1").drop("_rank")
    deduped.createOrReplaceTempView("batch_data")
    df.sparkSession.sql("""
        MERGE INTO labuser12596051_1787331522.sdp_2_silver.foreachbatch_target AS target
        USING batch_data AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


@dp.append_flow(target="foreachbatch_target_sink")
def foreachbatch_target_flow():
    return (
        spark.readStream.table("labuser12596051_1787331522.sdp_1_bronze.customers_bronze_clean_demo12")
        .select(
            "address",
            "city",
            "customer_id",
            "email",
            "name",
            "state",
            "zip_code",
            "processing_time",
            "source_file",
            "timestamp_datetime"
        )
    )
