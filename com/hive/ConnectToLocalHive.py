# Configure spark variables
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from os.path import abspath

if __name__ == "__main__":

    # warehouse_location points to the default location for managed databases and tables
    # warehouse_location = abspath('spark-warehouse')

    # Creating SparkSession
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("Python Spark SQL Hive integration") \
        .config("spark.sql.warehouse.dir", "C:\\Project\\Files\\Hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # Create Database demo
    spark.sql("create database if not exists Demo")

    # Listing all the Databases
    spark.sql("show databases").show()

    # Setting the current database to Demo Database
    spark.catalog.setCurrentDatabase("Demo")

    table_list = spark.sql("show tables in Demo")
    table_name = table_list.filter(table_list.tableName == "movies").collect()

    if len(table_name) > 0:
        print("Table movies is present in Demo database")
    else:
        print("Table not found,Creating the table")
        spark.sql('create table movies \
         (Film string,Genres string,Lead_Studio string,Audition_Score int,'
                  'Profit string,Rotten string,Year int) \
         row format delimited fields terminated by "," \
         LINES TERMINATED BY "\n" \
         stored as TEXTFILE')

        spark.sql("LOAD DATA LOCAL INPATH '/C:/Project/Files/Input/Movie_1.txt' INTO TABLE movies")

    spark.sql("show tables").show()

    # print("Movies table data before the data load:")
    # spark.sql("select * from movies").show()

    spark.sql("select * from movies").show()
    spark.sql("drop table movies")

    # spark.sql("CREATE TABLE src(key INT, value STRING) USING hive")
    # spark.sql("insert into movies (movieId, title,genres) VALUES (12,"xyz","abc")
    # ...
