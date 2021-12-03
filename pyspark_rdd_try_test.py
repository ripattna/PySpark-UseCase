"""
Connect to SQl and Convert the data to SparkDF and then load to HIVE table
"""

from pyspark.sql import SparkSession
import sys
import logging


def sql_to_spark():
    """This function will Convert Sql data to Spark DataFrame"""

    try:
        logging.info("Creating spark session")
        spark = SparkSession.builder.appName('Test').master("local").getOrCreate()

    except Exception as e:

        logging.error("could not create spark session due to exception")
        logging.exception(e)
        sys.exit(1)

    print("Hello World!!")


sql_to_spark()





