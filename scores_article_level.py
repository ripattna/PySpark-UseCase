from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, mean, count

# SparkSession
spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

# Read the file
scores_article_level = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True, header=True)
scores_article_level.show()

scores_article_level_14 = scores_article_level.withColumn("aggregation", lit(14))
scores_article_level_28 = scores_article_level.withColumn("aggregation", lit(28))

scores_article_level = scores_article_level_14.union(scores_article_level_28)
scores_article_level.show()
