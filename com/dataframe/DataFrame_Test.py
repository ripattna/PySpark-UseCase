from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
from pyspark.sql import SparkSession


class DataFrame_Test:
    @staticmethod
    def data_frame_function():
        try:
            print("#############################  This is try block!  ####################################")
            # Create Spark session
            spark = SparkSession.builder.master("local").appName("test").getOrCreate()

            # List
            data = [('Admit', 25), ('Jalopies', 22), ('Sarah', 20), ('Baba', 26)]

            # Create a schema for the data frame
            schema = StructType([StructField('Name', StringType(), True), StructField('Age', IntegerType(), True)])

            # Convert list to RDD
            rdd = spark.sparkContext.parallelize(data)

            # Create data frame
            df = spark.createDataFrame(rdd, schema)

            print(df.printSchema())
            print(df.describe())
            print(df.describe().show())
            df.show()
        except ValueError:
            Exception("Something went wrong")


if __name__ == "__main__":
    DataFrame_Test.data_frame_function()
