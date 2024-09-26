from pyspark.context import SparkContext
from pyspark.sql import SparkSession

sc = SparkSession.builder.appName('FirstProg').getOrCreate()
data = sc.sparkContext.parallelize([1,2,3,4,5,6,7,8,9,10])

sumdata = data.reduce(lambda i,j:(i+j))
print(sumdata)