from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.\
        builder \
        .master("local[*]") \
        .appName("Hive External Table")\
        .config("spark.sql.warehouse.dir", "C:\\Project\\Files\\Hive")\
        .enableHiveSupport()\
        .getOrCreate()

    # Creating a database if not exists
    spark.sql("create database if not exists my_spark_db")

    # Listing all the databases
    spark.sql("show databases").show()

    # Setting the current database
    spark.catalog.setCurrentDatabase("my_spark_db")

    # Creating an external table
    spark.sql("""create table if not exists summary
    (TIME_STAMP TIMESTAMP,
    GENDER STRING,
    COUNTRY STRING,
    STATE STRING)
    STORED AS PARQUET
    location "file:///C:/tmp/" """)
    spark.sql("""show tables""").show()
    spark.sql("""describe table summary""").show()

