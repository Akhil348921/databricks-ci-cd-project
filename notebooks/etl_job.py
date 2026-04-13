# Databricks ETL Sample (PySpark)

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Step 1: Create sample data
data = [
    (1, "Akhil", 1000),
    (2, "Ravi", 2000),
    (3, "Sita", 3000)
]

columns = ["id", "name", "salary"]

df = spark.createDataFrame(data, columns)

# Step 2: Transformation
df_filtered = df.filter(df.salary > 1500)

# Step 3: Output
df_filtered.show()

# Optional: Write to DBFS
df_filtered.write.mode("overwrite").csv("/dbfs/FileStore/output_data")
