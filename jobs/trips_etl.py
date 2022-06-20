import configparser
import sys

from sqlalchemy import create_engine

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, TimestampType, DoubleType
from pyspark.sql.functions import split, regexp_replace, expr

config = configparser.ConfigParser()
config.read_file(open('./config/dl.cfg'))


def create_spark_session():
    """
    creates spark session
    sets log level to error
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

    df = spark.read.options(header='True') \
        .schema(schema) \
        .csv(input_data)
    count = df.count()
    print(f"Input CSV file {input_data} loaded with {count} rows.\n")
    return df, count


def transform(spark, input_df):
    """
    adds input filename as a column metadata
    drops duplicates (requirement: Trips with similar origin, destination, and time of day should be grouped together.)
    transform origin_coord and destination_coord to lat/long as new columns
    enforces new schema
    """
    spark.udf.register("filename_trim", lambda x: x.rsplit('/', 1)[-1])
    input_df = input_df.withColumn('input_file_name', expr('filename_trim(input_file_name())'))

    input_count = input_df.count()
    dedup_df = input_df.dropDuplicates(['origin_coord', 'destination_coord', 'datetime'])
    dedup_count = dedup_df.count()
    dup_count = input_count - dedup_count

    print(f"Duplicates not ingested: {dup_count}\n")

    df = dedup_df \
        .withColumn('origin', regexp_replace('origin_coord', 'POINT \(', '')) \
        .withColumn('origin', regexp_replace('origin', '\)', '')) \
        .drop('origin_coord') \
        .withColumn('destination', regexp_replace('destination_coord', 'POINT \(', '')) \
        .withColumn('destination', regexp_replace('destination', '\)', '')) \
        .drop('destination_coord')

    df = df.withColumn('lat_orig', split(df['origin'], ' ').getItem(1).cast(DoubleType())) \
        .withColumn('lon_orig', split(df['origin'], ' ').getItem(0).cast(DoubleType())) \
        .withColumn('lat_dest', split(df['destination'], ' ').getItem(1).cast(DoubleType())) \
        .withColumn('lon_dest', split(df['destination'], ' ').getItem(0).cast(DoubleType())) \
        .drop('origin').drop('destination')

    print("Schema from CSV file:")
    input_df.printSchema()
    print("New transformed schema:")
    df.printSchema()

    return df, dup_count


def load_staging(df, url, user, password, driver, engine):
    """
    loads transformed data into postgres db
    query loaded data to make sure all rows were loaded
    """
    table_name = "trips_staging"
    df.write.format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .mode("overwrite") \
        .save()

    with engine.connect() as con:
        query = f"SELECT COUNT(*) AS cnt FROM {table_name}"
        result = con.execute(query).fetchone()
        count = result['cnt']

    print(f"{count} rows loaded into Postgres {table_name} table.\n")
    return count


def check_counts(initial_count, end_count, step, input_data):
    """
    check if counts are matching and write assertion results to console
    """
    try:
        assert initial_count == end_count
        print(f"{initial_count} rows from input data {input_data} correctly processed in the {step} step.\n")
        return True
    except AssertionError:
        print(f"{input_data} was not correctly processed in the {step} step.\n")
        return False


def read_file(script_path):
    """
    helper function to read sql script file and return sqlalchemy textual sql
    """
    from sqlalchemy.sql import text
    with open(script_path, 'r') as sql_file:
        sql_query = sql_file.read()

    return text(sql_query)


def upsert_spatial(engine, last_input_file_path):
    """
    creates spatial table if not exists
    upserts spatial table with new data from staging table
    returns rowcount of upserted rows in the spatial table
    """
    filename = last_input_file_path.split('/')[-1]
    with engine.connect() as con:
        create_sql = read_file('./scripts/create_trips.sql')
        upsert_sql = read_file('./scripts/upsert_trips.sql')
        count_sql = read_file('./scripts/count_upserts.sql')

        params = {'last_input_file': filename}

        con.execute(create_sql)
        con.execute(upsert_sql, **params)
        result = con.execute(count_sql, **params).fetchone()

    print(f"{result['cnt']} rows upserted into Postgres spatial trips table.\n")
    return result['cnt']


def process_data():
    """
    creates spark session
    gets config properties
    creates postgres engine
    extracts data from csv
    apply the transformations
    load new data into postgres staging table
    upsert new data from staging into final spatial table
    """
    spark = create_spark_session()

    if len(sys.argv) > 1:
        input_data = sys.argv[1]
    else:
        input_data = config.get('LOCAL', 'INPUT_DATA')

    url = config.get('POSTGRES', 'URL')
    user = config.get('POSTGRES', 'USER')
    password = config.get('POSTGRES', 'PASS')
    driver = config.get('POSTGRES', 'DRIVER')
    engine_string = config.get('POSTGRES', 'ENGINE')

    engine = create_engine(engine_string)

    extracted_df, extracted_count = extract(spark, input_data)
    transformed_df, dup_count = transform(spark, extracted_df)
    staging_count = load_staging(transformed_df, url, user, password, driver, engine)

    if not check_counts(extracted_count, staging_count+dup_count, 'staging', input_data):
        return

    upserts_count = upsert_spatial(engine, input_data)

    check_counts(extracted_count, upserts_count+dup_count, 'upserting', input_data)

    print("ETL Processing finished. Check the report notebook for analytics.")


if __name__ == "__main__":
    process_data()
