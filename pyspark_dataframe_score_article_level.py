"""
This program is to calculate the score for different season
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, mean, count
from datetime import date
from functools import reduce
from pyspark.sql import DataFrame

spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

scores_article_level_read = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True, header=True)
scores_article_level_read.show()

current_month = date.today().month
if 6 <= current_month <= 11:
    session_start_date = date(date.today().year, 6, 1)
elif current_month == 12:
    session_start_date = date(date.today().year, 12, 1)
else:
    session_start_date = date(date.today().year - 1, 12, 1)

current_date = date.today()
session_till_date = abs(current_date - session_start_date).days

append_pair_scores = []
window = [14, 28, session_till_date]

for i in window:
    scores_article_level = scores_article_level_read.withColumn("aggregation", lit(i))
    append_pair_scores.append(scores_article_level)
    scores_article_level = reduce(DataFrame.unionAll, append_pair_scores)

scores_article_level.show()
