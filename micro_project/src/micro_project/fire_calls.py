import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from lib.logger import Log4j
from pyspark.sql import functions as F
from lib.utils import get_spark_app_config, read_csv_file


if __name__ == "__main__":
    # Load the environment variables
    load_dotenv()
    fire_calls_file_path = os.getenv("FIRE_CALLS_DATA_FILE_PATH")
    spark_conf_file_path = os.getenv("SPARK_CONF_FILE_PATH")

    # Create a SparkSession    
    conf = get_spark_app_config(config_file_path= spark_conf_file_path)

    spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

    # Create a logger    
    logger = Log4j(spark)
    logger.info("Session started")

    # Read the CSV file
    raw_df = read_csv_file(spark= spark, file_path= fire_calls_file_path)
    
    # Rename the columns
    new_column_names = [column_name.replace(' ', '_').replace('-', '_').replace('__', '_').lower() for column_name in raw_df.columns]
    column_renamed_df = raw_df.toDF(*new_column_names)

    column_renamed_df  = column_renamed_df.withColumn(colName='call_date', col= F.to_date(col='call_date', format='MM/dd/yyyy')) \
                                        .withColumn(colName= 'watch_date', col= F.to_date(col= 'watch_date', format= 'MM/dd/yyyy')) \
                                        .withColumn(colName= 'received_dttm', col= F.to_timestamp(col= 'received_dttm', format= 'MM/dd/yyyy hh:mm:ss a')) \
                                        .withColumn(colName= 'entry_dttm', col= F.to_timestamp(col= 'entry_dttm', format= 'MM/dd/yyyy hh:mm:ss a')) \
                                        .withColumn(colName= 'dispatch_dttm', col= F.to_timestamp(col= 'dispatch_dttm', format= 'MM/dd/yyyy hh:mm:ss a')) \
                                        .withColumn(colName= 'response_dttm', col= F.to_timestamp(col= 'response_dttm', format= 'MM/dd/yyyy hh:mm:ss a')) 
    column_renamed_df.cache()

#Question 1----------------------------------------------------------------------------------------------------------------
    # 1. How many different types of calls were made to the fire department?
    # spark.sql("""SELECT COUNT(DISTINCT(call_number)) FROM fire_calls""").show()
    q1_df = column_renamed_df.select('call_number') \
                            .distinct()
    q1_df.count()

#Question 2----------------------------------------------------------------------------------------------------------------

    #2. How many incidents of each call type were there? 
    # spark.sql("""SELECT call_type_group, COUNT(incident_number) FROM fire_calls GROUP BY call_type_group""").show()
    q2_df = column_renamed_df.groupBy('call_type_group').agg(F.count('incident_number').alias('total_incident'))
    q2_df.show()

#Question 3----------------------------------------------------------------------------------------------------------------
    #3. Find out all responses or delayed times greater than 5 mins
    # spark.sql(""" SELECT * FROM fire_calls WHERE (unix_timestamp(TO_TIMESTAMP(response_dttm, 'MM/dd/yyyy hh:mm:ss a')) 
    #     - unix_timestamp(TO_TIMESTAMP(received_dttm, 'MM/dd/yyyy hh:mm:ss a'))) / 60 > 5 """).show()
    q3_df = column_renamed_df.where((F.unix_timestamp('response_dttm') - F.unix_timestamp('received_dttm')) / 60 > 300)
    q3_df.show()

#Question 4----------------------------------------------------------------------------------------------------------------
    #4 What are the most common call types
    # spark.sql("""SELECT call_type_group, COUNT(incident_number) as count_incident_number FROM fire_calls GROUP BY call_type_group ORDER BY count_incident_number DESC LIMIT 1""").show()
    q4_df = column_renamed_df.groupBy('call_type_group').agg(F.count('incident_number').alias('count_incident_number'))
    q4_df.sort(F.desc('count_incident_number')).limit(1).show()

#Question 5----------------------------------------------------------------------------------------------------------------
    #5 What zip codes accounted for the most common calls?
    # spark.sql("""SELECT zipcode_of_incident, count(zipcode_of_incident) as count_zipcode_of_incident FROM fire_calls GROUP BY 1 ORDER BY 2 DESC""").show()
    q5_df = column_renamed_df.groupby('zipcode_of_incident').agg(F.count('zipcode_of_incident').alias('count_zipcode_of_incident'))
    q5_df.sort(F.desc('count_zipcode_of_incident')).show()

#Question 6----------------------------------------------------------------------------------------------------------------
    #6 What San Francisco neighborhoods are in the zip codes 94102 and 94103
    spark.sql(""" SELECT neighborhooods___analysis_boundaries, city, zipcode_of_incident  FROM fire_calls WHERE city = 'San Francisco' AND  zipcode_of_incident BETWEEN '94102' AND '94103' """).show()        

#Question 7----------------------------------------------------------------------------------------------------------------
    #7 What was the sum of calls, average, min and max of the call response times?
    spark.sql(""" SELECT SUM(call_number) AS total_call, 
                        AVG(unix_timestamp(TO_TIMESTAMP(response_dttm, 'MM/dd/yyyy hh:mm:ss a')) - unix_timestamp(TO_TIMESTAMP(entry_dttm, 'MM/dd/yyyy hh:mm:ss a')) / 60) as avg_response, 
                        min(unix_timestamp(TO_TIMESTAMP(response_dttm, 'MM/dd/yyyy hh:mm:ss a')) - unix_timestamp(TO_TIMESTAMP(entry_dttm, 'MM/dd/yyyy hh:mm:ss a')) / 60) as min_response, 
                        max(unix_timestamp(TO_TIMESTAMP(response_dttm, 'MM/dd/yyyy hh:mm:ss a')) - unix_timestamp(TO_TIMESTAMP(entry_dttm, 'MM/dd/yyyy hh:mm:ss a')) / 60) as max_response 
                FROM fire_calls""").show()

#Question 8----------------------------------------------------------------------------------------------------------------
    #8 How many distinct years of data are in the CSV file
    spark.sql(""" SELECT COUNT(DISTINCT(YEAR(TO_DATE(call_date, 'MM/dd/yyyy')))) as call_date FROM fire_calls """).show()

#Question 9----------------------------------------------------------------------------------------------------------------
    #9 What week of the year in 2018 had the most fire calls
    spark.sql(""" SELECT count(call_number) as calls, EXTRACT(week FROM TO_DATE(call_date, 'MM/dd/yyyy')) AS week FROM fire_calls WHERE YEAR(TO_DATE(call_date, 'MM/dd/yyyy')) = 2018 GROUP BY 2 ORDER BY 1 DESC LIMIT 10""").show()    

#Question 10----------------------------------------------------------------------------------------------------------------
    #10 What neighborhoods in San Francisco had the worst response time in 2018?
    spark.sql("""   SELECT neighborhooods___analysis_boundaries, 
                        SUM(unix_timestamp(TO_TIMESTAMP(response_dttm, 'MM/dd/yyyy hh:mm:ss a')) - unix_timestamp(TO_TIMESTAMP(entry_dttm, 'MM/dd/yyyy hh:mm:ss a'))) as sum_response
                    FROM fire_calls
                    WHERE  YEAR(TO_DATE(call_date, 'MM/dd/yyyy')) = 2018
                    GROUP BY 1
                    ORDER BY 2 DESC          
            
            """).show()

    logger.info("Finished")
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    spark.stop()    