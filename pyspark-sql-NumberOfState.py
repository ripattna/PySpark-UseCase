"""
Find the number of state for each Country (ipLookup Dataset)
"""

from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Creating Spark Spark Session
spark = SparkSession.builder.appName('Test').getOrCreate()

# Creating the schema
schema1 = StructType([StructField("HostIp", StringType(), True),
                      StructField("Country", StringType(), True),
                      StructField("CityCode", StringType(), True),
                      StructField("State", StringType(), True),
                      StructField("Latitude", StringType(), True),
                      StructField("Longitude", StringType(), True)])

ipLookupDF = spark.read.format('com.databricks.spark.csv')\
    .option('delimiter', ",")\
    .schema(schema1)\
    .load("resources/goShopping_IpLookup.txt")

# Creating TempView
ipLookupDF.createOrReplaceTempView("ipLookupTable")
# Spark-Sql query to get number of state of each country
resultDF = spark.sql("select Country,count(State) from ipLookupTable group by Country")
resultDF.show()

