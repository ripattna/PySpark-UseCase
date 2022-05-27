from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

appName = "PySpark Example - Python Array/List to Spark Data Frame"
master = "local"

# Create Spark session
spark = SparkSession.builder \
    .appName(appName) \
    .master(master) \
    .getOrCreate()

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

