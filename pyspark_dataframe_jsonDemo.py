from pyspark.sql import SparkSession


class Json_demo:
    @staticmethod
    def json_test():
        spark = SparkSession.builder.appName("test").getOrCreate()

        path = "resources/Sample-JSON.json"
        data_df = spark.read.option("multiline", "true").json(path)

        print(data_df.show())
        print(data_df.printSchema())
        print(data_df.describe())
        print(data_df.describe().show())
        print(data_df['City'])
        print(type(data_df['City']))
        print(data_df.select('City'))
        print(data_df.select('City').show())
        print(data_df.select(['City']).show())
        print(data_df.head(2))
        print(data_df.select('City', 'ZipCodeType').show())
        print(data_df.select(['City', 'ZipCodeType']).show())

        data_df.withColumnRenamed('RecordNumber', 'Us_Record_number').show()

        data_df.withColumn('newRecordNumber', data_df['RecordNumber'] + 1).show()
        print(type(data_df))

        # Creating a Temp View from the DataFrame
        data_df.createOrReplaceTempView("records")

        sql_df = spark.sql("select * from records")
        print(sql_df.show())


if __name__ == "__main__":
    Json_demo.json_test()
