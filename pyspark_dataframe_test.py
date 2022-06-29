"""
Dataframe basics examples
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType


# Create Spark session
spark = SparkSession.builder.appName("test").master("local").getOrCreate()

# List
data = [('Category A', 100, "This is category A"),
        ('Category B', 120, "This is category B"),
        ('Category C', 150, "This is category C")]

# Create a schema for the data frame
schema = StructType([
    StructField('Category', StringType(), True),
    StructField('Count', IntegerType(), True),
    StructField('Description', StringType(), True)])

# Convert list to RDD
rdd = spark.sparkContext.parallelize(data)

# Create data frame
df = spark.createDataFrame(rdd, schema)
print(df.schema)
df.show()
