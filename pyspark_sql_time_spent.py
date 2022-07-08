"""
To find the overall time spend from the device Android
"""
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Creating Spark Session
spark = SparkSession.builder.appName('Test').getOrCreate()

schema1 = StructType([StructField("Date", StringType(), True),
                      StructField("Time", StringType(), True),
                      StructField("HostIP", StringType(), True),
                      StructField("Cs-Method", StringType(), True),
                      StructField("CustomerIP", StringType(), True),
                      StructField("URL", StringType(), True),
                      StructField("TimeSpent", StringType(), True),
                      StructField("RedirectedFrom", StringType(), True),
                      StructField("DeviceType", StringType(), True)])

web_clicks = spark.read.format('com.databricks.spark.csv')\
    .option('delimiter', ",")\
    .schema(schema1)\
    .option("delimiter", "\t")\
    .load("resources/goShopping_WebClicks.dat")

schema2 = StructType([StructField("HostIp", StringType(), True),
                      StructField("Country", StringType(), True),
                      StructField("CityCode", StringType(), True),
                      StructField("State", StringType(), True),
                      StructField("Latitude", StringType(), True),
                      StructField("Longitude", StringType(), True)])

ipLookupDF = spark.read.format('com.databricks.spark.csv')\
    .option('delimiter', ",")\
    .schema(schema2)\
    .load("resources/goShopping_IpLookup.txt")


joinDF = web_clicks.join(ipLookupDF, web_clicks.HostIp == ipLookupDF.HostIp, join='inner')
joinDF.show(truncate=False)

android_TimeSpent = spark.sql("select sum(TimeSpent) as Time_Spent from web_clicks where DeviceType='android' ").show()


"""
Write a query to find amount of time spent by each user who are all from INDIA (Country Code: IND)
"""
ind = spark.sql("select sum(TimeSpent) as Time_Spent from web_clicks where DeviceType='android' ").show()
