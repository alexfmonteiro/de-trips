import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession


def create_spark_session():
    """
    creates spark session
    """
    spark = SparkSession.builder \
        .appName("Data Engineering Trips Analytics") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.14") \
        .getOrCreate()
    return spark


def extract(spark, input_data):
    pass


def transform(spark, input_data):
    pass

def load(spark, input_data):
    pass

def process_data():
    """
    creates spark session
    configures input and output data paths
    calls process_song_data and process_log_data
    """
    spark = create_spark_session()

    input_data = None

    df = extract(spark, input_data)
    df = transform(spark, df)
    load(spark, df)


if __name__ == "__main__":
    process_data()

