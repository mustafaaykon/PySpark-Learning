import os
import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from micro_project.lib.utils import read_csv_file, count_by_country


@pytest.fixture(scope="session")
def data_file_path():
    load_dotenv(dotenv_path= "/Users/mustafaaliaykon/Desktop/PERSONAL/projects/pyspark_learning/udemy/micro_project/src/micro_project/.env")
    path = os.getenv("SAMPLE_DATA_FILE_PATH")
    assert path is not None, "SAMPLE_FILE_PATH is not exist in .env file."
    return path


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
                .master("local[3]") \
                .appName("SparkTest") \
                .getOrCreate()
    yield spark
    spark.stop()

def test_data_file_loading(spark, data_file_path):
    sample_df = read_csv_file(spark, file_path=data_file_path)
    assert sample_df.count() == 9, "Record count should be 9"

def test_country_count(spark, data_file_path):
    sample_df = read_csv_file(spark, file_path= data_file_path)
    count_list = count_by_country(df= sample_df).collect()
    count_dict = {}
    for row in count_list:
        count_dict[row["Country"]] = row["count"]
    
    assert count_dict["United States"] == 4, "Count for United States should be 4"
    assert count_dict["Canada"] == 2, "Count for Canada should be 2"
    assert count_dict["United Kingdom"] == 1, "Count for United Kingdom should be 1"