from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

df = spark.read\
    .options("inferSchema", True)\
    .option("header", True)\
    .format("csv").load("resources/scores_article_level.csv")

df.show()
df.printSchema()
