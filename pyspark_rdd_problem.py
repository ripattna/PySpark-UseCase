from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test').getOrCreate()

L = ["Harrani", "Karthik", "Sanjay", "Sanjay", "Harrani", "karthik"]
rdd = spark.sparkContext.parallelize(L)
startsWith = ['a', 'd']
names = rdd.filter(lambda x: x[0].lower() in startsWith)
for i in names.collect():
    print(i)
