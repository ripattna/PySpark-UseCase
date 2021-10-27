from pyspark.sql import SparkSession

# Creating SparkSession
spark = SparkSession.builder.appName("PySpark-UseCase").master("local").getOrCreate()

# Create RDD using sparkContext.parallelize()
language = ["scala", "java", "python", "ruby", "ansible", "yml", "unix"]
rdd1 = spark.sparkContext.parallelize(language)
print("The element of the first RDD is:", rdd1.collect())

# Create RDD from external Data source(Create RDD using sparkContext.textFile())
rdd2 = spark.sparkContext.textFile("resources/Input.txt")
print("The element of the second RDD is:", rdd2.collect())

# Create RDD using sparkContext.wholeTextFiles())
rdd3 = spark.sparkContext.wholeTextFiles("resources/Input.txt")
print("The element of the third RDD is:", rdd3.collect())

# Creating a RDD by transforming a existing RDD
rdd4 = rdd3.filter(lambda x: 'Spark' in x)
print("This is the filter Spark contains word:", rdd4.count())
print("This is the filter Spark contains word:", rdd4.collect())

# Create Empty RDD using sparkContext.emptyRDD
# Creates Empty RDD with no partition
rdd5 = spark.sparkContext.emptyRDD()
print("Value of RDD5 is:", rdd5.collect())
print("Type of RDD5 is:", type(rdd5))
print("RDD6 Partition Count:" + str(rdd5.getNumPartitions()))

# Creates Empty RDD using parallelize
rdd6 = spark.sparkContext.parallelize([])
print(rdd6)
print("Value of RDD6 is:", rdd6.collect())
print("Type of RDD6 is:", type(rdd6))
print("RDD6 Partition Count:" + str(rdd6.getNumPartitions()))

# Creating Empty RDD with partition
rdd7 = spark.sparkContext.parallelize([], 10)
print(rdd7)
print("Value of RDD7 is:", rdd7.collect())
print("Type of RDD7 is:", type(rdd7))
print("RDD7 Partition Count:" + str(rdd7.getNumPartitions()))
