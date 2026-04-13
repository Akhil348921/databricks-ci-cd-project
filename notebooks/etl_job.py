# Databricks ETL Sample (Final Version)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("Job Started")

# Initialize Spark
spark = SparkSession.builder.appName("ETL Job Example").getOrCreate()

# Step 1: Create sample data
data = [
    (1, "Akhil", 1000),
    (2, "Ravi", 2000),
    (3, "Sita", 3000)
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)

print("=== Raw Data ===")
df.show()

# Step 2: Transformation
df_filtered = df.filter(df.salary > 1500)

# Add new column
df_updated = df_filtered.withColumn("bonus", col("salary") * 0.1)

print("=== Transformed Data ===")
df_updated.show()

print("Job Completed")
