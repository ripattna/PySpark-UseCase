"""
How many times each IP address searched for product type = "Jewellery" (Weblclicks dataset)
Hint: after extracting product, filter only for "jewellry"
Sample op: 100.0.0.0,4
100.0.0.1,10
"""

from pyspark.sql import SparkSession

# Creating Spark Session
spark = SparkSession.builder.appName("Test").getOrCreate()

# Reading the WebClicks Dataset
webClicks = spark.sparkContext.textFile("resources/goShopping_WebClicks.dat")
# Splitting the data with \t
webClicks_split = webClicks.map(lambda x: x.split("\t"))

deviceProducts = webClicks_split.map(lambda x: (x[4], x[5].split("&")[0].split("=")[1]))
filter_product = deviceProducts.filter(lambda x: x[1] == 'jewellery')
count_product = filter_product.map(lambda x: (x[0], 1))
result = count_product.reduceByKey(lambda x, y: x + y)
for i in result.collect():
    print(i)
