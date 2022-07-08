from pyspark import SparkContext, SparkConf

# Creating SparkContext
# conf = SparkConf().setAppName("appName").setMaster("local")
# sc = SparkContext(conf)

sc = SparkContext("local", "PySpark-UseCase")

read_rdd = sc.textFile("resources/Input.txt")
print("The element of the RDD is:", read_rdd.collect())

# Creating a RDD by transforming existing RDD
newRDD = read_rdd.filter(lambda x: 'spark' in x)
filtered = newRDD.collect()
print("This is the filter Spark contains word:", newRDD.collect())
sc.stop()
