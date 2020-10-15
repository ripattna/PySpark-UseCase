from pyspark import SparkContext, SparkConf


class CreteRDD:
    @staticmethod
    def rdd_test():
        try:
            conf = SparkConf().setAppName("appName").setMaster("local")
            sc = SparkContext(conf=conf)

            # Creating a RDD by parallelize a collection
            first_rdd = sc.parallelize(["scala", "java", "python", "ruby", "ansible", "yml", "unix"])
            # print(type(first_rdd))
            print("The element of the first RDD is:", first_rdd.collect())
            # print("#######################################################################################")

            # Creating a RDD referencing a data set from a external storage system
            read_rdd = sc.textFile("C:\\Project\\Files\\Input\\text\\Input.txt")
            # print(type(read_rdd))
            print("The element of the second RDD is:", read_rdd.collect())
            # print("#######################################################################################")

            # Creating a RDD by transforming a existing RDD
            new_rdd = read_rdd.filter(lambda x: 'Spark' in x)
            # print(type(new_rdd))
            # filter_rdd = new_rdd.collect()
            print("This is the filter Spark contains word:", new_rdd.count())
            sc.stop()

        except ValueError:
            print("Enable to create the RDD!")


if __name__ == "__main__":
    CreteRDD.rdd_test()
