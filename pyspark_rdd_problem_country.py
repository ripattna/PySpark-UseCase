"""
Display all states for each country:
SAMPLE O/p: (UK,....|.....|.....|......)(USA,....|.....|.....|......)(IND,....|.....|.....|......)
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test').getOrCreate()

iplkp = spark.sparkContext.textFile("resources/goShopping_IpLookup.txt")
fields = iplkp.map(lambda x: x.split(","))
country_state = fields.map(lambda x: (x[1], x[3]))
dist_states = country_state.distinct()
result = dist_states.reduceByKey(lambda x, y: x + " | " + y)
for i in result.collect():
    print(i)

result.saveAsTextFile("/tmp")

