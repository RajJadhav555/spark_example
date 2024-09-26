import findspark
findspark.init()
from pyspark.sql import SparkSession
sc  = SparkSession.builder.appName('FileReadProg').getOrCreate()

file = sc.sparkContext.textFile('file:///C:\\Users\\Intel\\OneDrive\\Desktop\\Ganesha.txt')
trans1 = file.flatMap(lambda x: x.split(" "))
keybyvalue = trans1.map(lambda a:(a,1))
agg = keybyvalue.reduceByKey(lambda  x, y:(x+y))
print(agg.collect())