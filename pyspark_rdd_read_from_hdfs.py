""" Reading a file from HDFS """
from pyspark.sql import SparkSession


class ReadFromHdfs:
    """This class will Read a file from HDFS"""

    @staticmethod
    def csv_test():
        """This method will Read a file from HDFS"""
        try:
            # Creating Spark Session
            spark = SparkSession.builder.appName("test").getOrCreate()
            # Specifying the HDFS path
            path = "hdfs://localhost:9003/user/data/Sample.csv"
            # Read the CSV file from HDFS
            data_df = spark.read.csv(path, inferSchema=True, header=True)

            print('The type of csv file data is:', type(data_df))

            print(data_df.show())
            print("The schema is:", data_df.printSchema())

            # Creating a Temp View from the data_frame
            # data_df.createOrReplaceTempView("records")

            # sql_df = spark.sql("select * from records where Units > 50")
            # print(sql_df.show())
            # print(type(data_df))
            # print(type(sql_df))

        except ValueError:
            print("Not able to read the csv file")


if __name__ == "__main__":
    ReadFromHdfs.csv_test()
