from pyspark.sql import SparkSession

if __name__ == "__main__":

    # Creating SparkSession
    spark = SparkSession.builder.appName("RddExercise").getOrCreate()

    data = [[1, "Ram"], [2, "Sam"], [3, "Hari"], [4, "Rabi"]]
    d2 = [x[1] for x in data]
    print(d2)
    newRDD = spark.sparkContext.parallelize(data, 3)
    print("The element of the first RDD is:", newRDD.collect())
    print("No of Partition:", newRDD.getNumPartitions())

