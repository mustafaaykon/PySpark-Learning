from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from aggregations.lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Spark Join Demo") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)
    logger.info("Starting AggDemo")

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]

    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")
    product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")
    product_renamed_df.show()
    order_df.show()

    # INNER JOIN
    order_df.join(product_renamed_df, on= order_df['prod_id'] == product_renamed_df['prod_id'], how= "inner") \
        .drop(product_renamed_df.prod_id) \
        .select("order_id", order_df["prod_id"], "prod_name", "unit_price", "qty") \
        .show()

    # OUTER JOIN
    # We can set NULL values for prod_name as prod_id and list_price as unit_price. We don't want null values.
    order_df.join(product_renamed_df, on= order_df['prod_id'] == product_renamed_df['prod_id'], how= "left") \
        .drop(product_renamed_df.prod_id) \
        .withColumn("prod_name", F.expr("coalesce(prod_name, prod_id)")) \
        .withColumn("list_price", F.expr("coalesce(list_price, unit_price)")) \
        .select("order_id", "prod_id", "prod_name", "unit_price", "list_price", "qty") \
        .orderBy("order_id") \
        .show()