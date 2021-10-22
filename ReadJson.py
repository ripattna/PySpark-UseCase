from pyspark.sql import SparkSession


class ReadJson:
    @staticmethod
    def json_read():
        # Creating Spark Session
        spark = SparkSession.builder.appName("ReadJson").master("local[*]").getOrCreate()

        # Read JSON file into dataframe
        read_json_date = spark.read.json('C:\\Project\\Files\\Input\\json\\office.json').cache()
        # read_json_date = spark.read.json.path('C:\Project\Files\Input\json\Sample-JSON.json')

        print(read_json_date.show())


if __name__ == "__main__":
    ReadJson.json_read()
