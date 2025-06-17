from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import collect_list,udf,col

#definig schema
Schema=StructType([
    StructField("Name",StringType(),nullable=True),
    StructField("Age",IntegerType(),nullable=True),
    StructField("Department",StringType(),nullable=True),
    StructField("Salary",IntegerType(),nullable=True)
])
#providing data
Data=[("Aravind",24,"CSE",10000),("Teja",23,"IT",25000),("Bharath",25,"IT",50000),("Hari",25,"CSE",55000)]
#intializing the spark session
ss=SparkSession.builder.appName("app2").getOrCreate()
#creating dataframe
df=ss.createDataFrame(Data,Schema)
#used to print the data
df.show()

Grouped_departments=df.groupBy("Department").agg(collect_list("Salary").alias("Salaries"))
Grouped_departments.show()

#define median function
def find_median(salaries):
    Sorted_salaries=sorted(salaries)
    n=len(Sorted_salaries)
    if n%2==0:
        return (Sorted_salaries[n//2-1]+Sorted_salaries[n//2])/2.0 #float value
    else:
        return float(Sorted_salaries[n//2])

#udf(function call,return datatype)

median_salary=udf(find_median,FloatType())
updated_df=Grouped_departments.withColumn("median_salary",median_salary(col("salaries")))
updated_df.select("Department","salaries","median_salary").show()



