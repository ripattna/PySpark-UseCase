"""
This is Word Count program
Instead of using flatMap we are using map
"""
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # Create Spark context with necessary configuration
    conf = SparkConf().setAppName("appName").setMaster("local")
    sc = SparkContext(conf=conf)

    # Read the file
    readRDD = sc.textFile("resources/WordCount_Sample.txt")
    # Applying filter transformation
    nonempty_lines = readRDD.filter(lambda x: len(x) > 0)

    # print(nonempty_lines.collect())
    words = nonempty_lines.flatMap(lambda x: x.split(" "))

    # print(words.collect())
    wordcount = words.map(lambda x: (x, 1))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[1], x[0]))\
        .sortByKey(False)

    print(wordcount.collect())
    print("The word count of the file is:", wordcount.count())

