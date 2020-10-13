from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from os.path import abspath

# warehouse_location points to the default location for managed databases and tables
# warehouse_location = abspath('hdfs://quickstart.cloudera:8020/user/hive/warehouse')

'''
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("hive.metastore.uris", "thrift://192.168.56.101:9083") \
    .enableHiveSupport() \
    .getOrCreate()'''

spark = SparkSession \
    .builder.config("hive.metastore.uris", "thrift://chddemo:9083")\
    .config("hive.metastore.warehouse.dir", "/user/hive/warehouse")\
    .enableHiveSupport().getOrCreate()

spark.sql("show databases").show()
# print(sparkSessionObj.sql("show databases").show().count())
# sparkSessionObj.sql("drop table if exists src")
# spark is an existing SparkSession
# spark.sql("""create database hive_test""")
# spark.sql("show databases").show()
# spark.sql("use hive_test")
# spark.sql("CREATE TABLE src(key INT, value STRING) USING hive")
# spark.sql("show tables").show()
# spark.sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

