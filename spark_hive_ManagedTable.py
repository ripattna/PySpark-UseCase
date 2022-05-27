from pyspark.sql.session import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Demo").enableHiveSupport().getOrCreate()

    spark.sql("create database if not exists demo")
    spark.sql("show databases").show()
    spark.catalog.setCurrentDatabase("demo")
    spark.sql("drop table if exists src")
    '''
    spark.sql("CREATE TABLE src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '"
              " LINES TERMINATED BY '\\n' ")
    spark.sql("show tables").show()
    spark.sql("LOAD DATA LOCAL INPATH 'file:///C:/Project/Files/Input/text/Src_Table.txt' INTO TABLE src")
    spark.sql("select * from src").show()
    '''
