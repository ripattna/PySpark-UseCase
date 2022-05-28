"""
To find which user has spent max time for searching the product Shoes.
"""

from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test').getOrCreate()

schema1 = StructType([StructField("Date", StringType(), True),
                      StructField("Time", StringType(), True), \
                      StructField("HostIP", StringType(), True), \
                      StructField("Cs-Method", StringType(), True), \
                      StructField("CustomerIP", StringType(), True), \
                      StructField("URL", StringType(), True), \
                      StructField("TimeSpent", StringType(), True), \
                      StructField("RedirectedFrom", StringType(), True), \
                      StructField("DeviceType", StringType(), True)])

web_clicks = spark.read.format('com.databricks.spark.csv')\
    .option('delimiter', ",")\
    .schema(schema1)\
    .option("delimiter", "\t")\
    .load("resources/goShopping_WebClicks.dat")

web_clicks.createOrReplaceTempView("web_clicks")

resultDF = spark.sql("select * from web_clicks")
# resultDF.show(5, False)

shoes1 = spark.sql("select CustomerIP,sum(TimeSpent) as TotalTs from web_clicks where URL like '%shoes%' group by CustomerIP order by TotalTs desc").show(2)