"""
List the unique devices, which each ip address used(Weblclicks dataset)
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

Webclicks = spark.sparkContext.textFile("resources/goShopping_WebClicks.dat")
Webclicks_split = Webclicks.map(lambda x: x.split("\t"))
Device_ip = Webclicks_split.map(lambda x: (x[4], x[8]))
dist_device_ip = Device_ip.distinct()
result = dist_device_ip.reduceByKey(lambda x, y: x + " | " + y)
for i in result.collect():
    print(i)
