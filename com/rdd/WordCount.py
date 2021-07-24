from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    # Create Spark context with necessary configuration
    sc = SparkContext("local", "PySpark Word Count Example")

    # conf = SparkConf().setAppName("appName").setMaster("local")
    # sc = SparkContext(conf=conf)

    # Read data from text file and split each line into words
    text_file = sc.textFile("C:\\Project\\Files\\Input\\text\\Input.txt")
    # print(text_file.collect())
    # print(type(text_file))
    counts = text_file.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: x + y)
    print(counts.collect())
    print("The word count of the file is:", counts.count())
