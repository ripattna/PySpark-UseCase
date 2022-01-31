"""

Rdd assignments RDD basics

"""
from pyspark.sql import SparkSession


def rdd_basic():
    # Creating Spark Session
    spark = SparkSession.builder.appName("test").getOrCreate()

    # Create a RDD for the above array
    rdd = spark.sparkContext.parallelize(["34", "29", "34", "29", "49", "78"])

    # Display the array
    print("The RDD elements are:", rdd.collect())

    # Display the first element of the array
    print("The first element of the RDD:", rdd.first())

    # Display the sorted output (ascending and descending) through an RDD
    sort_asc_rdd = rdd.sortByKey("asc")
    sort_desc_rdd = rdd.sortByKey(ascending=False)
    print("The ascending order of the RDD:", sort_asc_rdd.collect())
    print("The descending order of the RDD:", sort_desc_rdd.collect())

    # Display the distinct elements of the array using an RDD
    distinct_rdd = rdd.distinct()
    print(distinct_rdd)
    print("The distinct RDD list:", distinct_rdd)

    # Display distinct elements without using a new RDD.
    print("The distinct element are:", rdd.distinct())
    print(rdd.distinct())

    # Display maximum and minimum of given array using RDD.
    print("The maximum value of the RDD element is:", rdd.max())
    print("The minimum value of the RDD element is:", rdd.min())

    # Display top 5 list elements using RDD
    print("The top 5 element of the RDD are :", rdd.top(5))

    # Combine above array with a new array { 30,35,45,60,75,85} and display output.
    new_rdd = spark.sparkContext.parallelize(["30", "35", "45", "60", "75", "85"])
    # print(newRdd.collect())
    print("After join the new elements:", rdd.union(new_rdd).collect())

    # Provide the sum of the array elements using reduce with distinct values

    # Provide the sum of the array elements using reduce
    # print("The sum of the RDD:", rdd.union(newRdd).sum())


rdd_basic()
