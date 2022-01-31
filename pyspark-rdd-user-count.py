"""
Find the number of users from each Country (Iplookup dataset)
Sample op:(ind,4 uk,8 usa,11)
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

Iplookup = spark.sparkContext.textFile("resources/goShopping_IpLookup.txt")
Iplookup_Split = Iplookup.map(lambda x: x.split(","))
country_user = Iplookup_Split.map(lambda x: (x[1], 1))
Result = country_user.reduceByKey(lambda x, y: x + y)
for i in Result.collect():
    print(i)
