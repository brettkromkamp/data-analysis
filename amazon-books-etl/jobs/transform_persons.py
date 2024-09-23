from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("TransformPersons").getOrCreate()

# Sample data frame transformation
df = spark.createDataFrame(
    [
        ("John Doe", 40),
        ("Jane Doe", 35),
        ("Alice Smith", 45),
        ("Bob Brown", 50),
    ],
    ["name", "age"],
)

df.show()

# Perform transformations
df_filtered = df.filter(df["age"] > 40)
df_filtered.show()

# Stop Spark session
spark.stop()
