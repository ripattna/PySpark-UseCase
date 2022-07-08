"""
This program is to calculate the score for different season
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, mean, count, when
from datetime import date
from functools import reduce
from pyspark.sql import DataFrame

spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

scores_article_level_read = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True,
                                                          header=True)
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
aggregation_windows = [14, 28, session_till_date]

for aggregation_window in aggregation_windows:
    scores_article_level = scores_article_level_read.withColumn("aggregation", lit(aggregation_window))
    append_pair_scores.append(scores_article_level)
    scores_article_level = reduce(DataFrame.unionAll, append_pair_scores)
    scores_article_level = scores_article_level.withColumn('aggregation',
                                                           when(col("aggregation") == 14, '2_Week')
                                                           .when(col("aggregation") == 28, '4_Week')
                                                           .otherwise(lit("season")))

scores_article_level.show()
