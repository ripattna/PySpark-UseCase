"""
How many times each IP address searched for product type = "Jewellery" (Weblclicks dataset)
Hint: after extracting product, filter only for "jewellry"
Sample op: 100.0.0.0,4
100.0.0.1,10
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

Webclicks = spark.sparkContext.textFile("resources/goShopping_WebClicks.dat")
Webclicks_split = Webclicks.map(lambda x: x.split("\t"))
Device_Products = Webclicks_split.map(lambda x: (x[4], x[5].split("&")[0].split("=")[1]))
filter_product = Device_Products.filter(lambda x: x[1] == 'jewellery')
count_product = filter_product.map(lambda x: (x[0], 1))
result = count_product.reduceByKey(lambda x, y: x + y)
for i in result.collect():
    print(i)
