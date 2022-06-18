from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, mean, count
from datetime import datetime, timedelta, date

# SparkSession
spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

# Read the file
scores_article_level = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True,
                                                     header=True)
scores_article_level.show()

current_month = date.today().month
if 6 <= current_month <= 11:
    session_start_date = date(date.today().year, 6, 1)
elif current_month == 12:
    session_start_date = date(date.today().year, 12, 1)
else:
    session_start_date = date(date.today().year - 1, 12, 1)

current_date = date.today()
session_till_date = abs(current_date - session_start_date).days

window = [14, 28, session_till_date]

for i in window:
    scores_article_level = scores_article_level.withColumn("aggregation", lit(i))
    scores_article_level.show()

scores_article_level.show()
