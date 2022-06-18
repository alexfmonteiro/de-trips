import logging
import configparser

from sqlalchemy import create_engine

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType
from pyspark.sql.functions import split, regexp_replace, expr

config = configparser.ConfigParser()
config.read_file(open('./config/dl.cfg'))
logger = logging.getLogger('pyspark')


def create_spark_session():
    """
    creates spark session
    """
    spark = SparkSession.builder \
        .appName("Data Engineering Trips Analytics") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.14") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def extract(spark, input_data):
    """
    extracts data from input file into a dataframe
    enforces raw schema
    """
    schema = StructType() \
        .add("region", StringType(), True) \
        .add("origin_coord", StringType(), True) \
        .add("destination_coord", StringType(), True) \
        .add("datetime", TimestampType(), True) \
        .add("datasource", StringType(), True)

    df = spark.read.options(header='True')\
        .schema(schema)\
        .csv(input_data)
    count = df.count()
    print(f"Input CSV file {input_data} loaded with {count} rows.\n")
    return df, count


def transform(spark, input_df):
    """
    transform origin_coord and destination_coord to lat long as new columns
    enforces new schema
    adds input filename as a column
    """
    df = input_df\
        .withColumn('origin', regexp_replace('origin_coord', 'POINT \(', '')) \
        .withColumn('origin', regexp_replace('origin', '\)', ''))\
        .drop('origin_coord') \
        .withColumn('destination', regexp_replace('destination_coord', 'POINT \(', '')) \
        .withColumn('destination', regexp_replace('destination', '\)', '')) \
        .drop('destination_coord')

    df = df.withColumn('lat_origin', split(df['origin'], ' ').getItem(0).cast(DoubleType())) \
        .withColumn('lon_origin', split(df['origin'], ' ').getItem(1).cast(DoubleType())) \
        .withColumn('lat_destin', split(df['destination'], ' ').getItem(0).cast(DoubleType())) \
        .withColumn('lon_destin', split(df['destination'], ' ').getItem(1).cast(DoubleType())) \
        .drop('origin').drop('destination')

    spark.udf.register("filename_trim", lambda x: x.rsplit('/', 1)[-1])
    df = df.withColumn('filename', expr('filename_trim(input_file_name())'))

    print(f"New schema:")
    df.printSchema()

    return df


def load_staging(spark, df, url, user, password, driver):
    """
    loads transformed data into postgres db
    query loaded data to make sure all rows were loaded
    """
    table_name = "trips_staged"
    df.write.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .mode("overwrite") \
        .save()

    df_pg = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .load()

    count = df_pg.count()
    print(f"{count} rows loaded into Postgres {table_name} table.\n")
    return count


def upsert_fact(engine_string):
    """
    upsert fact spatial table with new data from staged table
    """
    engine = create_engine(engine_string)
    connection = engine.connect()

    my_query = 'SELECT * FROM trips_staged'
    results = connection.execute(my_query).fetchmany(20)
    print(results)


def process_data():
    """
    creates spark session
    gets config properties
    extracts data from csv
    apply the transformations
    load new data into postgres staging table
    upsert new data into fact spatial table
    """
    spark = create_spark_session()

    input_data = config.get('LOCAL', 'INPUT_DATA')
    url = config.get('POSTGRES', 'URL')
    user = config.get('POSTGRES', 'USER')
    password = config.get('POSTGRES', 'PASS')
    driver = config.get('POSTGRES', 'DRIVER')
    engine = config.get('POSTGRES', 'ENGINE')

    df, extracted_count = extract(spark, input_data)
    df = transform(spark, df)
    loaded_count = load_staging(spark, df, url, user, password, driver)

    try:
        assert extracted_count == loaded_count
        print('\033[1m', f"All data from file {input_data} was loaded into staging table")
    except AssertionError:
        print(f"{input_data} was not correctly loaded into Postgres")

    upsert_fact(engine)


if __name__ == "__main__":
    process_data()
