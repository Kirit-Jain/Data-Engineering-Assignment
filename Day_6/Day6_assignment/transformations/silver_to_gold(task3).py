from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, count, date_trunc

@dp.materialized_view(
    name="gold.daily_revenue_by_store",
    comment="Daily revenue and order volume aggregated by store, for business reporting.",
    table_properties={"quality": "gold"}
)
def daily_revenue_by_store():
    return(
        spark.read.table("silver.silver_sales")
            .withColumn("sale_day", date_trunc("day", col("order_date")))
            .groupBy("sale_day")
            .agg(
                count("order_id").alias("total_orders"),
                sum("total_amount").alias("total_revenue"),
                sum("discount_amount").alias("total_discounts"),
                sum("quantity").alias("total_units_sold")
            )
    )

@dp.materialized_view(
    name="gold.daily_units_by_product",
    comment="Daily units sold aggergated by store",
    table_properties={"quality": "gold"}
)
def daily_units_by_product():
    return(
        spark.read.table("silver.silver_sales")
            .withColumn("sale_day", date_trunc("day", col("order_date")))
            .groupBy("sale_day", "product_id")
            .agg(
                sum("quantity").alias("total_units_sold"),
                count("order_id").alias("total_orders")
            )
    )