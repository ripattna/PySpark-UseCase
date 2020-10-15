# Configure spark variables
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession

conf = SparkConf().setAppName("appName").setMaster("local")
sc = SparkContext(conf=conf)

readRdd = sc.textFile("C:\\Project\\Files\\Input\\csv\\order_detail.csv")
# print(readRdd.collect())

header = readRdd.first()
print(header)

orderDetail = readRdd.filter(lambda x: x != header)
# print(orderDetail.collect())

orderToken = orderDetail.map(lambda x: x.split(","))
# print(orderToken.collect())

# Find the unique product code in the RDD
product_code = orderToken.map(lambda x: x[1]).distinct()
print(product_code.collect())

# Pair RDD Function
# Find the total sales amount for each product
productSalesPairRDD = orderToken.map(lambda x: (x[1], int(x[2]) * float(x[3])))
print(productSalesPairRDD.count())
productSalesRDD = productSalesPairRDD.reduceByKey(lambda acc, value: acc + value)
print(productSalesRDD.collect())

# Archive the same result using groupByKey()
productSalesRDD_1 = productSalesPairRDD.groupByKey().mapValues(lambda x: sum(x))
print(productSalesRDD_1.collect())
