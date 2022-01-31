import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('Test').getOrCreate()

L = ["Dharani", "Karthik", "Sanjay", "Akshay", "dharani", "karthik"]
rdd = spark.sparkContext.parallelize(L)
startswith = ['a', 'd']
names = rdd.filter(lambda x: x[0].lower() in (startswith))
for i in names.collect():
    print(i)
