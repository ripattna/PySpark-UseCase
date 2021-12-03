"""
This is a word count program in PySpark
"""
from pyspark import SparkContext

if __name__ == "__main__":

    # Create Spark context with necessary configuration
    sc = SparkContext("local", "PySpark Word Count Example")

    # Read data from text file and split each line into words
    text_file = sc.textFile("resources/WordCount_Sample.txt")

    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: x + y)

    print(counts.collect())
    print("The word count of the file is:", counts.count())
