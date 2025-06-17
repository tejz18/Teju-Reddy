from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize a Spark session
spark = SparkSession.builder.appName("SampleDataFrame").getOrCreate()

# Define schema
schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("City", StringType(), True)
])

# Create sample data
data = [
    (1, "Alice", 25, "New York"),
    (2, "Bob", 30, "San Francisco"),
    (3, "Charlie", 35, "Los Angeles"),
    (4, "David", 40, "Chicago")
]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show DataFrame
df.show()

# Stop Spark session
spark.stop()
