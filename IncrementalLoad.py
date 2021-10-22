from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

list1 = [(1, 'abc'), (2, 'def')]

old_df = spark.createDataFrame(list1, ['id', 'value'])
old_df.show()

list2 = [(2, 'cde'), (3, 'xyz')]
new_df = spark.createDataFrame(list2, ['id', 'value'])
new_df.show()

df = old_df.join(new_df, old_df.id == new_df.id, 'full_outer')\
    .select(coalesce(old_df.id, new_df.id).alias("id"), coalesce(new_df.value, old_df.value).alias("value"))

# Printing the DataSet
df.show()
