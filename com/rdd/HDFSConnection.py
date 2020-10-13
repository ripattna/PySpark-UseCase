from pyspark import SparkContext, SparkConf
import os

if __name__ == "__main__":

    # Create Spark context with necessary configuration
    conf = SparkConf().setAppName("appName").setMaster("local")
    sc = SparkContext(conf=conf)
    import os
    os.system('ls')
