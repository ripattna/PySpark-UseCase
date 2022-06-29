from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Test.com").getOrCreate()

data = spark.read.parquet("resources/part-00009-ba11.c000.snappy.parquet")

data.show(truncate=False)
