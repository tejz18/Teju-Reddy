import time

from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

Schema=StructType([
    StructField("name",StringType(),nullable=True),
    StructField("age",IntegerType(),nullable=True),
    StructField("DOB",StringType(),nullable=True)
])
Data=[("Aravind",24,"13-5-2000"),("Teja",23,"18-05-2001")]
var1=SparkSession.builder.appName("app2").config("spark.ui.enabled","true").getOrCreate()
var2=var1.createDataFrame(Data,Schema)
var2.show()
import time
time.sleep(300)