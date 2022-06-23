"""
This program is to calculate Regex
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, when, sum, lit, col, mean, count, col

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

data = [(14, "14851 Jeffrey Rd", "DE"),
        (28, "43421 Margarita St", "NY"),
        (30, "13111 Simon Ave", "CA")]
df = spark.createDataFrame(data, ["aggregation", "address", "state"])
df.show()

df.withColumn('aggregation',
              when(col("aggregation") == 14, '2_Week')
              .when(col("aggregation") == 28, '4_Week')
              .otherwise(lit("season"))).show()

