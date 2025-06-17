from pyspark import *
from pyspark.sql import *
var1=SparkSession.builder.appName('app1').getOrCreate()
a=[1,1,7,5]
var2=var1.sparkContext.parallelize(a).collect()
print(var2)