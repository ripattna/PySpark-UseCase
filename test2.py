from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import when, col

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

data = [
    ('14', datetime(2010, 3, 24, 3, 19, 58), 13),
    ('14', datetime(2020, 9, 24, 3, 19, 6), 8),
    ('George', datetime(2009, 12, 12, 17, 21, 30), 5),
    ('Micheal', datetime(2010, 11, 22, 13, 29, 40), 12),
    ('Maggie', datetime(2010, 2, 8, 3, 31, 23), 8),
    ('Ravi', datetime(2009, 1, 1, 4, 19, 47), 2),
    ('Xien', datetime(2010, 3, 2, 4, 33, 51), 3),
]

df1 = spark.createDataFrame(data, ['aggregation', 'trial_start_time', 'purchase_time'])
df1.show(truncate=False)

df1.withColumn('aggregation',
               when(col('aggregation') == 'George', 'George_Week')
               .when(col('aggregation') == 'Ravi', 'Ravi_Week')
               .otherwise(col('aggregation'))).show()
