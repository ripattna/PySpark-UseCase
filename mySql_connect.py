from pyspark.sql import SparkSession

import mysql.connector

# Creating SparkSession
# spark = SparkSession.builder.appName("MySql_Connection").getOrCreate()

my_db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root"
)

my_cursor = my_db.cursor()

my_cursor.execute("show databases")
for x in my_cursor:
    print(x)


