from pyspark.sql import SparkSession
from datetime import datetime, timedelta, date, time
from pyspark.sql.functions import udf, to_date
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import sum, lit, col, mean, ntile, count

# SparkSession
spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

# Read the file
scores_article_level = spark.read.format("csv") \
                       .load("resources/scores_article_level.csv", inferSchema=False, header=True)  \

scores_article_level.show()
scores_article_level.printSchema()

scores_article_level = scores_article_level.withColumn("aggregation", lit(14))
scores_article_level.show()
scores_article_level.printSchema()


# scores_article_level.select(col("dt"), to_date(col("dt"), "MM-dd-yyyy").alias("dt")).show()

# scores_article_level = scores_article_level.withColumn("dt", col("dt").cast("date"))
# scores_article_level.show()
# scores_article_level.printSchema()

# def define_quantile(column1, column2):
#     if column1 == 4 and column2 == 4:
#         return 1
#     else:
#         return 0
#
#
# func_udf = udf(define_quantile, IntegerType())
#
# # scores_article_level = scores_article_level.withColumn("dt", lit(date.strftime("%Y-%m-%d")))
# # scores_article_level.show()
# # scores_article_level.printSchema()
#
# new_list = [14, 28, 32]
# print(type(new_list))
#
# scores_article_level = scores_article_level.withColumn("aggregation", lit(14))
# scores_article_level.show()
# scores_article_level.printSchema()
#
