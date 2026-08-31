from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, DateType


@dp.view( name="stg_sales")
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
def stg_sales():
    return (
        spark.readStream.table("cyntexa_dev.bronze.bronze_sales")
            .withColumn("quantity", col("quantity").cast(IntegerType()))
            .withColumn("discount_amount", col("discount_amount").cast(DoubleType()))
            .withColumn("total_amount", col("total_amount").cast(DoubleType()))
            .withColumn("order_date", col("order_date").cast(DateType()))
            .select(
                "order_id", "customer_id", "transaction_id", "product_id", "quantity", "discount_amount", "total_amount", "order_date", "_ingested_at"
            )
    )

dp.create_streaming_table(
    name="silver.silver_sales",
    comment="Cleaned, deduplicated sales data",
    table_properties={"quality": "silver"}
)

dp.apply_changes(
    target = "silver.silver_sales", 
    source = "stg_sales", 
    keys = ["order_id"], 
    sequence_by = col("_ingested_at"),
    except_column_list = ["_ingested_at"],
    stored_as_scd_type=1
)