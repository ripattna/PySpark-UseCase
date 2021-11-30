"""
This is Rdd Transformation Example
"""
from pyspark import SparkContext

if __name__ == "__main__":

    # Create Spark context with necessary configuration
    # conf = SparkConf().setAppName("appName").setMaster("local")
    # sc = SparkContext(conf=conf)

    sc = SparkContext("local", "PySpark")

    readRDD = sc.textFile("resources/Input.txt")

    nonempty_lines = readRDD.filter(lambda x: len(x) > 0)

    # print(nonempty_lines.collect())
    words = nonempty_lines.flatMap(lambda x: x.split(" "))

    # print(words.collect())
    wordcount = words.map(lambda x: (x, 1))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[1], x[0]))\
        .sortByKey(False)

    print(wordcount.collect())
