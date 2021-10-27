from pyspark.sql import SparkSession

# Creating Spark Session
spark = SparkSession.builder.appName("PySpark-UseCase").master("local").getOrCreate()

data = ["", "Alice’s Adventures in Wonderland", "Project Gutenberg’s", "Adventures in Wonderland", "Project Gutenberg’s"]

rdd = spark.sparkContext.parallelize(data)

for i in rdd.collect():
    print(i)
