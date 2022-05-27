"""

Webclicks.dat usecase:Display all the unique products from each device type
O/p:
(andrioid,....|.....|.....|......)
(windows,....|.....|.....|......)
(linux,....|.....|.....|......)
(mac,....|.....|.....|......)

"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test').getOrCreate()

web = spark.sparkContext.textFile("resources/goShopping_WebClicks.dat")
fields = web.map(lambda x: x.split("\t"))
pair_rdd = fields.map(lambda x: (x[8], x[5].split("&")[0].split("=")[1]))
dist_prd = pair_rdd.distinct()
result = dist_prd.reduceByKey(lambda x, y: x + " |" + y)
for i in result.collect():
    print(i)

