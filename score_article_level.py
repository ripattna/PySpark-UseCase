from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, mean, ntile, count

# SparkSession
spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

# Read the file
scores_article_level = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True, header=True)
scores_article_level.show()

window = [14, 28]

for i in window:
    scores_article_level = scores_article_level.withColumn("aggregation", lit(i))
    scores_article_level.show()


scores_article_level.show()
