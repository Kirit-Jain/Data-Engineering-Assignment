from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="bronze_sales",
    comment="Raw data is ingested in the the bonze_sales table as is from the volume"
)
def bronze_sales():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/Volumes/cyntexa_dev/bronze/raw/sales_raw/_schema/sales")
        .option("header", "true")
        .load("/Volumes/cyntexa_dev/bronze/raw/sales_raw")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )