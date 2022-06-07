from datetime import date
# from datetime import datetime, timedelta, date


def diff_dates(date1, date2):
    return abs(date2 - date1).days


def main():
    session_start_date = date(2022, 6, 1)
    print(type(session_start_date))
    current_date = date(2022, 6, 13)
    result1 = diff_dates(session_start_date, current_date)
    print('{} days between {}: and {}:'.format(result1, session_start_date, current_date))
    print("Happy programmer's day!")


main()


# from pyspark.sql import SparkSession
# from pyspark.sql.functions import sum, lit, col, mean, count
#
# # SparkSession
# spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()
#
# # Read the file
# scores_article_level = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True, header=True)
# scores_article_level.show()
#
# session_date = 30
# window = [14, 28, session_date]
#
# for i in window:
#     scores_article_level = scores_article_level.withColumn("aggregation", lit(i))
#     scores_article_level.show()
#     # scores_article_level = scores_article_level.unionAll(scores_article_level)
#     # scores_article_level.show()
#
# scores_article_level.show()
