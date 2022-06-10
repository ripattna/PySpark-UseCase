"""
This program is to calculate the score for different season
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import date

# SparkSession
spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

# Read the file
scores_article_level = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True, header=True)
scores_article_level.show()

# Season till date logic
current_month = date.today().month
if current_month >= 6 and current_month <= 11:
    season_start_date = date(date.today().year, 6, 1)
elif current_month == 12:
    season_start_date = date(date.today().year, 12, 1)
else:
    season_start_date = date(date.today().year - 1, 12, 1)

current_date = date.today()
season_till_date = abs(current_date - season_start_date).days

window = [14, 28, season_till_date]

for i in window:
    scores_article_level = scores_article_level.withColumn("aggregation", lit(i))
    scores_article_level.show()

scores_article_level.show()
