from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import pandas as pd
# Set Python path for PySpark
os.environ["PYSPARK_PYTHON"] = r"C:\Users\HP\PycharmProjects\PythonProject\.venv\Scripts\python.exe"

# Initialize Spark session
spark = SparkSession.builder.appName("app1").getOrCreate()

# Schema and Data for df1
schema1 = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("Age", IntegerType(), nullable=True),
    StructField("DOB", StringType(), nullable=True),
])

data1 = [("Teja", 23, "05-18-2001"),
         ("Bharath", 25, "08-05-1999"),
         ("Aravind", 24, "05-13-2000")]

df1 = spark.createDataFrame(data1, schema1)

# Schema and Data for df2
schema2 = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("Qualification", StringType(), nullable=True),
    StructField("College", StringType(), nullable=True),
    StructField("Organization", StringType(), nullable=True)
])

data2 = [("Teja", "Masters", "EIU", "Amazon"),
         ("Bharath", "Masters", "EIU", "Tesla"),
         ("Aravind", "Masters", "EIU", "Deloitte")]

df2 = spark.createDataFrame(data2, schema2)

# Convert to pandas DataFrames
pddf1 = df1.toPandas()
pddf2 = df2.toPandas()

# Print the pandas DataFrames
df3=pd.merge(pddf1,pddf2,on="name",how="inner")
print (df3)





