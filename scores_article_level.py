from pyspark.sql import SparkSession
from datetime import datetime, timedelta, date, time
from pyspark.sql.functions import udf, to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum, lit, col, mean, ntile, count
import pandas as pd

# SparkSession
spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

# Read the file
scores_article_level = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True, header=True)

scores_article_level.show()
scores_article_level.printSchema()

scores_article_level.select(col("dt"), to_date(col("dt"), "MM-dd-yyyy").alias("dt")).show()

scores_article_level = scores_article_level.withColumn("dt", col("dt").cast("date"))
scores_article_level.show()
scores_article_level.printSchema()