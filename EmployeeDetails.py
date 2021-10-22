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
    print("Table Employee is already present in Demo database.\n")
else:
    print("Table not found,Creating the table")
    spark.sql('create table employee \
         (id int,name string,gender string,salary int) \
     row format delimited fields terminated by "," \
     LINES TERMINATED BY "\n" \
     stored as TEXTFILE')

    spark.sql("LOAD DATA LOCAL INPATH '/C:/Project/Files/Input/text/Employee.txt' INTO TABLE employee")

# Select the highest salary
print("Select the highest salary:")
spark.sql("select * from"
          "(select *,DENSE_RANK() over(order by salary desc)as DENSE_RANK FROM employee)v "
          "where DENSE_RANK=1").show()

# Select the third highest salary
print("Select the third highest salary:")
spark.sql("select * from"
          "(select *,DENSE_RANK() over(order by salary desc)as DENSE_RANK FROM employee)v "
          "where DENSE_RANK=3").show()

# Select highest salary of male and the female employee
print("Select highest salary of male and the female employee using row_number")
spark.sql("select * from"
          "(select *,row_number() over(partition by gender order by salary desc) rn from employee)v "
          "where rn=1").show()

print("Select highest salary of male and the female employee using dense_rank:")
spark.sql("select * from"
          "(SELECT *,DENSE_RANK() over(partition by gender order by salary desc)as DENSE_RANK FROM employee)v "
          "where DENSE_RANK=1").show()

# Select Second max salary of male and the female employee
print("Select Second max salary of male and the female employee")
spark.sql("select * from"
          "(select *,row_number() over (partition by gender order by salary desc) rn from employee)v "
          "where rn=2").show()

print("Select second highest salary of male and the female employee using dense_rank:")
spark.sql("select * from"
          "(SELECT *,DENSE_RANK() over(partition by gender order by salary desc)as DENSE_RANK FROM employee)v "
          "where DENSE_RANK=2").show()

spark.sql("SELECT id,name,salary,gender,"
          "rank() over (order by salary desc) as rank,"
          "dense_rank() over (order by salary desc) as dense_rank "
          "from employee").show()

spark.sql("SELECT id,name,salary,gender,"
          "rank() over (partition by gender order by salary desc) as rank,"
          "dense_rank() over (partition by gender order by salary desc) as dense_rank "
          "from employee").show()

# spark.sql("drop table if exists employee")

id,name ,salary
1-
2
3-
4

1
3
5

vertiluzation
----
1
1
1
null
null
2

---
1
2
2
null

