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

# Create Database demo
spark.sql("create database if not exists demo")
# spark.sql("show databases").show()

# Setting the current database to Demo Database
spark.catalog.setCurrentDatabase("demo")

table_list = spark.sql("show tables in demo")
table_name = table_list.filter(table_list.tableName == "product").collect()

if len(table_name) > 0:
    print("Table product is present in Demo database")
else:
    print("Table not found,Creating the table")
    spark.sql('create table product \
         (id int,commodity string,price int,version int) \
     row format delimited fields terminated by "," \
     LINES TERMINATED BY "\n" \
     stored as TEXTFILE')

    spark.sql("LOAD DATA LOCAL INPATH '/C:/Project/Files/Input/text/Product.txt' INTO TABLE product")

# spark.sql("show tables").show()

# spark.sql("select * from product").show()

# To get the highest price of each commodity

spark.sql("select * from"
          "(select *,row_number() over (partition by commodity,version ORDER BY price desc) rn from product)v"
          " where rn=1").show()

print("The First highest Commodity")

spark.sql("select * from"
          "(select *,row_number() over (partition by commodity ORDER BY price desc)as rn from product)v"
          " where rn=1").show()

spark.sql("select * from"
          "(SELECT *,DENSE_RANK() OVER (partition by commodity ORDER BY price DESC)as dn from product)v"
          " where dn=1").show()

spark.sql("WITH Result AS"
          "(SELECT *,DENSE_RANK() OVER (partition by commodity ORDER BY price DESC)as Price_Rank FROM product)"
          "SELECT * FROM Result WHERE Price_Rank = 1").show()

# Select the second highest commodity of each category to
print("The Second Highest Commodity")
spark.sql("select * from"
          "(select *,row_number() over (partition by commodity,version ORDER BY price desc) rn from product)v"
          " where rn=2").show()

spark.sql("SELECT id,commodity,price,version,"
          "rank() over (order by price desc) as rank,"
          "dense_rank() over (order by price desc) as dense_rank "
          "from product").show()

spark.sql("SELECT id,commodity,price,version,"
          "rank() over (partition by commodity order by price desc) as rank,"
          "dense_rank() over (partition by commodity order by price desc) as dense_rank "
          "from product").show()



# spark.sql("drop table if exists product")


