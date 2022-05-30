from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col, mean, ntile, count
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# SparkSession
spark = SparkSession.builder.appName("scores_article_level").master("local[*]").getOrCreate()

# Read the file
scores_article_level = spark.read.format("csv").load("resources/scores_article_level.csv", inferSchema=True,header=True)
scores_article_level.show()

emp_RDD = spark.sparkContext.emptyRDD()    # Empty RDD
columns = StructType([StructField('article', StringType(), False),
                      StructField('purchases', IntegerType(), False),
                      StructField('purchases_bundle', IntegerType(), False),
                      StructField('pairs', IntegerType(), False),
                      StructField('pairs_quantile', IntegerType(), False),
                      StructField('ratio_pairs_bundle', IntegerType(), False),
                      StructField('complementarity_score', IntegerType(), False),
                      StructField('market', StringType(), False),
                      StructField('dt', StringType(), False),
                      StructField('agg', StringType(), False)])

# Create an empty RDD with empty schema
df1 = spark.createDataFrame(data=emp_RDD, schema=columns)
df2 = spark.createDataFrame(data=emp_RDD, schema=columns)

window = [14, 28]

for i in window:

    if i == 14:
        scores_article_level = scores_article_level.withColumn("aggregation", lit(i))
        df1 = df1.union(scores_article_level)

    else:
        scores_article_level = scores_article_level.withColumn("aggregation", lit(i))
        df2 = df2.union(scores_article_level)

df2 = df1.union(df2)

df2.show()
