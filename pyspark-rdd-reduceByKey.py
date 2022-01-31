from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # Creating Spark Spark Session
    spark = SparkSession.builder.appName('Test').getOrCreate()


