import pymysql
import pandas as pd
from pyspark.sql import SparkSession

# Creating the SparkSession
spark = SparkSession.builder.appName('Test').master("local").getOrCreate()

# Open database connection
dbcon = pymysql.connect(host="localhost", user="rissan", password="rissan", database="demo")

try:
    SQL_Query = pd.read_sql_query('''select Id,Name,Gender,Salary from employee''', dbcon)

    pdf = pd.DataFrame(SQL_Query, columns=['Id', 'Name', 'Gender', "Salary"])
    print(pdf)
    print('The data type of pdf is: ', type(pdf))

    # Create a Spark DataFrame from a Pandas DataFrame
    df = spark.createDataFrame(pdf)
    print(df.show())
    print('The data type of df is: ', type(df))

    # Writing the data to Hive
    df.write().partitionBy("Gender").format("hive").saveAsTable("base.employee")
    # df.write.saveAsTable(base.employee)

    # Querying the hive table
    spark.sql("select * from base.employee").show()

    """ 
    df.createOrReplaceTempView("mytempTable")
    spark.sql("select * from mytempTable").show()
    """

except:
    print("Error: unable to convert the data")

dbcon.close()

