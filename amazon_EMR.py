from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, window

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TrafficDataProcessing") \
    .getOrCreate()

try:
    # Read data from S3
    df = spark.read.json("s3://your-bucket/your-input-path")

    # Data Exploration and Cleaning
    # Handle missing or null values
    df = df.dropna()
    # Convert timestamp to a proper format
    df = df.withColumn("timestamp", to_timestamp(df["timestamp"], "yyyy-MM-dd'T'HH:mm:ss"))
    # Filter data based on specific conditions
    df = df.filter((col("traffic_status") == "moderate") & (col("timestamp") > "2024-01-01"))
    # Data Aggregation and Transformation
    # Aggregate traffic data by location and time window
    processed_data = df.groupBy("location", window("timestamp", "1 hour")).agg({"traffic_status": "count"})
    # Calculate average traffic speed per location
    processed_data = df.groupBy("location").agg({"traffic_speed": "avg"})
    # Join with external datasets for enriched analysis
    external_data = spark.read.csv("s3://your-bucket/external-data.csv")
    joined_data = df.join(external_data, on="location")
    # Perform more complex transformations or calculations
    processed_data = df.withColumn("new_column", df["existing_column"] * 2)
    # Write processed data to S3 or another destination
    # For example, write processed data to Parquet format
    processed_data.write.mode("overwrite").parquet("s3://your-bucket/your-output-path")
    # For example, write processed data to a relational database
    processed_data.write.mode("append").jdbc(url="jdbc:postgresql://your-host:your-port/your-database",
                                             table="your_table",
                                             properties={"user": "your_user", "password": "your_password"})
finally:
    # Stop SparkSession
    spark.stop()
