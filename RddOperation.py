from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("appName").setMaster("local")
sc = SparkContext(conf=conf)

read_rdd = sc.textFile("C:\\Project\\Files\\Input\\text\\Input.txt")
print("The element of the RDD is:", read_rdd.collect())

# Creating a RDD by transforming a existing RDD
newRDD = read_rdd.filter(lambda x: 'spark' in x)
filtered = newRDD.collect()
print("This is the filter Spark contains word:", newRDD.collect())
sc.stop()
