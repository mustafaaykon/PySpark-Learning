from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from src.aggregations.lib.logger import Log4j
from src.aggregations.lib.utils import *

if __name__ == "__main__":
    spark_conf_file_path = "/Users/mustafaaliaykon/Desktop/PERSONAL/projects/pyspark_learning/udemy/aggregations/src/aggregations/AggDemo/spark.conf"
    conf = get_spark_app_config(config_file_path= spark_conf_file_path)
    spark = SparkSession.builder.config(conf=conf) \
                        .getOrCreate()
    logger = Log4j(spark)
    logger.info("Starting AggDemo")
    invoice_df = spark.read \
                    .format("csv") \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .load("data/invoices.csv")
    invoice_df.show()

    invoice_df.select(f.count("*").alias("Count *"),
    f.sum(col="Quantity").alias("Total Quantity"),
    f.avg("UnitPrice").alias("AvgPrice"),
    f.count_distinct(col= "InvoiceNo").alias("CountDistinct")).show()

    invoice_df.selectExpr("count(1)",
                        "sum(Quantity)",
                        "count(StockCode)",
                        "avg(UnitPrice) ").show()
    
    # invoice_df = invoice_df.withColumn("Quantity", invoice_df["Quantity"].cast(IntegerType()))
    # invoice_df = invoice_df.withColumn("UnitPrice", invoice_df["UnitPrice"].cast(IntegerType()))
    invoice_df.printSchema()
    
    invoice_df.groupBy("Country", "InvoiceNo") \
        .agg(f.sum(col="Quantity").alias("TotalQuantity"),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue"),
             f.expr("round(sum(Quantity * UnitPrice), 2) as InvoiceValueExpr")) \
        .show()


    logger.info("Stopping AggDemo")
    spark.stop()