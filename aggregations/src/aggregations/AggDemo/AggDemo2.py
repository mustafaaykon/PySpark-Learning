from pyspark.sql import SparkSession, Window
from aggregations.lib.logger import Log4j
from aggregations.lib.utils import *
from pyspark.sql import functions as F


def main():
    spark_conf_file_path = "/Users/mustafaaliaykon/Desktop/PERSONAL/projects/pyspark_learning/udemy/aggregations/src/aggregations/AggDemo/spark.conf"
    conf = get_spark_app_config(config_file_path=spark_conf_file_path)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = Log4j(spark)

    logger.info("Starting AggDemo")

    invoice_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("/Users/mustafaaliaykon/Desktop/PERSONAL/projects/pyspark_learning/udemy/aggregations/src/aggregations/AggDemo/data/invoices.csv")


    invoice_df.show()
    invoice_df.printSchema()

    NumInvoices = F.count_distinct("InvoiceNo").alias("NumInvoices")
    TotalQuantity = F.sum("Quantity").alias("TotalQuantity")
    InvoiceValue= F.round(F.sum(F.expr("Quantity * UnitPrice")), 2).alias("InvoiceValue")
    df = invoice_df \
        .withColumn("InvoiceDate", F.to_date("InvoiceDate", "dd-MM-yyyy H.mm")) \
        .withColumn("WeekNumber", F.weekofyear("InvoiceDate")) \
        .groupBy("Country", "WeekNumber") \
        .agg(NumInvoices, TotalQuantity, InvoiceValue)
    df.show()

    window = Window.partitionBy("Country") \
                    .orderBy("WeekNumber") \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    window_df = df.withColumn("RunningTotal", F.round(F.sum("InvoiceValue").over(window), 2))
    window_df.show()

    logger.info("Stopping AggDemo")
    spark.stop()


if __name__ == "__main__":
    main()
