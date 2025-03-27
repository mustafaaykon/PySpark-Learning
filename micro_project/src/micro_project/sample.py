import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from micro_project.lib.logger import Log4j
from micro_project.lib.utils import get_spark_app_config, read_csv_file, count_by_country

if __name__ == "__main__":
    load_dotenv(dotenv_path="/Users/mustafaaliaykon/Desktop/PERSONAL/projects/pyspark_learning/udemy/micro_project/src/micro_project/.env")
    sample_data_file_path = os.getenv("SAMPLE_DATA_FILE_PATH")
    spark_conf_file_path = os.getenv("SPARK_CONF_FILE_PATH")

    # SET SPARK CONFIGS
    spark_conf = get_spark_app_config(config_file_path= spark_conf_file_path)
    
    # CREATE SPARK SESSION
    spark = SparkSession.builder \
                        .config(conf = spark_conf) \
                        .getOrCreate()

    # Create a logger    
    logger = Log4j(spark)
    logger.info("Session started")    
    
    # READ DATA FROM FILE
    raw_df = read_csv_file(spark= spark, file_path= sample_data_file_path)
    partitioned_df = raw_df.repartition(2)

    count_df = count_by_country(partitioned_df)

    logger.info(count_df.collect())
        
    logger.info("Session stopped")
    spark.stop()




