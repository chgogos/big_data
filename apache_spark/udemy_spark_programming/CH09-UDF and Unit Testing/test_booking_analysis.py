#Unit test booking analysis

import pytest
from pyspark.testing.utils import assertDataFrameEqual
from pyspark.sql import SparkSession
from booking_analysis import read_booking_summary, top_3_revenue

@pytest.fixture(scope="session")
def spark():
    #Global Spark Session passed as fixture
    return SparkSession.builder.getOrCreate()

def test_read_booking_summary(spark):
    #Get Actual Results
    summary_df = read_booking_summary(spark)
    records_loaded = summary_df.count()
    #Assert the actuals with expected
    assert records_loaded == 58

def test_top_3_revenue(spark):
    #Get Actual Results 
    summary_df = read_booking_summary(spark)   
    result_df = top_3_revenue(summary_df)
    #Get the Expected results
    file_schema = "booked_by string, booking_date date, revenue double"
    expected_df = (
        spark.read.format("csv")
            .option("header", "true")
            .schema(file_schema)
            .load("/Volumes/dev/spark_db/datasets/spark_programming/data/top-3-days-test-data.csv")
    )
    #Assert
    assertDataFrameEqual(expected_df, result_df)









