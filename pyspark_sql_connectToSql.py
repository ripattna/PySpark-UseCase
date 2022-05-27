"""
Connect to SQl and Convert the data to SparkDF and then load to HIVE table
"""
import logging
import pymysql
import pandas as pd
from pyspark.sql import SparkSession


def sql_to_spark():
    """This function will convert Sql-Data to Spark-DataFrame"""

    try:
        logging.info("Creating spark session")
        spark = SparkSession.builder.appName('Test').master("local").enableHiveSupport().getOrCreate()

    except NameError:
        print('ourVariable is not defined')

    try:
        # Open database connection
        db_con = pymysql.connect(host="localhost", user="root", password="root", database="demo")

        try:
            sql_query = pd.read_sql_query('''select Id,Name,Gender,Salary from employee''', db_con)
            pdf = pd.DataFrame(sql_query, columns=['Id', 'Name', 'Gender', "Salary"])
            print(pdf)
            print('The data type of pdf is: ', type(pdf))

            # Create a Spark DataFrame from a Pandas DataFrame
            spark_df = spark.createDataFrame(pdf)
            print(spark_df.show())
            print('The data type of spark_df is: ', type(spark_df))

            # Writing the data to Hive
            # spark_df.write().partitionBy("Gender").format("hive").saveAsTable("default.employee")
            print("Getting the data from HIVE table")
            spark_df.write.saveAsTable("default.employee")

            # Querying the hive table
            spark.sql("select * from default.employee").show()

        except NameError:
            print("Something wrong to convert Sql data to Spark DF")

    except NameError:
        print("Unable to connect my Sql")


sql_to_spark()

