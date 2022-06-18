from pyspark.sql import SparkSession

# Spark session & context
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Sum of the first 100 whole numbers
rdd = sc.parallelize(range(100 + 1))
print(rdd.sum())
