from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

sc = SparkSession.builder.appName('FileReadProg').getOrCreate()
sqlCon = SQLContext(sc)

file = sqlCon.read.csv("file:///C:\\Prwatech\\Chennai.csv", header=True, inferSchema=True)

print("This is total count ", file.count())

file.registerTempTable("ChennaiRealEstate")

query2 = sqlCon.sql("select Location, count(Location) as LocationCount "
                    "from ChennaiRealEstate "
                    "group by Location having LocationCount>100 order "
                    "by LocationCount DESC")

print(query2.show())

