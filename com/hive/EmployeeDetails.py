from pyspark.sql import SparkSession
from os.path import abspath

# warehouse_location points to the default location for managed databases and tables
warehouse_location = abspath('spark-warehouse')

# Creating SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL Hive integration example") \
    .config("employee", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Create Database demo
spark.sql("create database if not exists demo")
# spark.sql("show databases").show()

# Setting the current database to Demo Database
spark.catalog.setCurrentDatabase("demo")

table_list = spark.sql("show tables in demo")
table_name = table_list.filter(table_list.tableName == "employee").collect()

if len(table_name) > 0:
    print("Table Employee is present in Demo database")
else:
    print("Table not found,Creating the table")
    spark.sql('create table employee \
         (id int,name string,gender string,salary int) \
     row format delimited fields terminated by "," \
     LINES TERMINATED BY "\n" \
     stored as TEXTFILE')

    spark.sql("LOAD DATA LOCAL INPATH '/C:/Project/Files/Input/text/Employee.txt' INTO TABLE employee")

spark.sql("show tables").show()

spark.sql("select * from employee").show()
# spark.sql("drop table employee")

""""
spark.sql("select * from"
          "(select *,row_number() over (partition by commodity,version order by price desc) rn from test)v"
          " where rn=1").show()

spark.sql("select * from"
          "(select *,row_number() over (partition by commodity order by price desc) rn from test)v"
          " where rn=1").show()

spark.sql("SELECT id,commodity,price,version,"
          "rank() over (order by price desc) as rank,"
          "dense_rank() over (order by price desc) as denserank "
          "from test").show()

spark.sql("SELECT id,commodity,price,version,"
          "rank() over (partition by commodity order by price desc) as rank,"
          "dense_rank() over (partition by commodity order by price desc) as denserank "
          "from test").show()

spark.sql("WITH Result AS"
          "(SELECT price,DENSE_RANK() OVER (partition by version ORDER BY price DESC)AS Price_Rank FROM Test)"
          "SELECT * FROM Result WHERE Price_Rank = 1").show()
"""
# spark.sql("drop table if exists test")

