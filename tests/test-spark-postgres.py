from pyspark.sql import SparkSession


spark = (SparkSession.builder
         .appName("test")
         .config("spark.jars", "/usr/local/spark/jars/postgresql-42.2.18.jar")
         .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.options(header='True', inferSchema='True', delimiter=',') \
  .csv("./data/input/trips.csv")

df.printSchema()
df.limit(5).show()

df.write.format("jdbc")\
    .option("url", "jdbc:postgresql://db:5432/postgres")\
    .option("dbtable", "trips_test")\
    .option("user", "postgres")\
    .option("password", "postgres")\
    .option("driver", "org.postgresql.Driver")\
    .mode("overwrite")\
    .save()


df_pg = spark.read\
    .format("jdbc")\
    .option("url", "jdbc:postgresql://db:5432/postgres")\
    .option("dbtable", "trips_test")\
    .option("user", "postgres")\
    .option("password", "postgres")\
    .option("driver", "org.postgresql.Driver")\
    .load()

df_pg.printSchema()
df_pg.limit(5).show()
