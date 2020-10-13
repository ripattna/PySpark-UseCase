# from logging import getLogger
from pyspark.sql import SparkSession
from os.path import abspath

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# Creating SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("demo", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

'''log = getLogger("jobLogger")
log.info("Info message")
log.error("Error message")'''

spark.sql("create database if not exists demo")

# spark.sql("drop table if exists src")
spark.sql("show databases").show()
spark.sql("use demo")

# spark.sql("show databases").show()
# spark.sql("CREATE TABLE src(key INT, value STRING) USING hive")
# spark.sql("show tables").show()
# spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
#