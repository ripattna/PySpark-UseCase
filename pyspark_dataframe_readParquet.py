from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

data = spark.read.parquet("C:\\BigData\\part-00009-bb979922-b429-4ed7-9caf-d1b77dc3ba11.c000.snappy.parquet")

data.show(truncate=False)
