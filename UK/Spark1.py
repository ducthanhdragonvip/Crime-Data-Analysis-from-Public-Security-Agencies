import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit, struct, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Set environment variables for Java and Spark
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"] = "/usr/lib/spark"

# Initialize SparkSession with Kafka configurations
spark = SparkSession.builder \
    .appName("CSV Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

# List of Kafka topics to process
topics = [
    "2022-03","2022-04","2022-05","2022-06","2022-07","2022-08",
    "2022-09","2022-10","2022-11","2022-12","2023-01","2023-02",
    "2023-03","2023-04","2023-05","2023-06","2023-07","2023-08",
    "2023-09","2023-10","2023-11","2023-12","2024-02",
    "2024-03","2024-04","2024-05","2024-06","2024-07","2024-08",
    "2024-09","2024-10","2024-11","2024-12","2025-01","2025-02", 
]

# Define JSON schema for Kafka data
json_schema = StructType([
    StructField("category", StringType(), True),
    StructField("location_type", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", StringType(), True),
        StructField("street", StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("longitude", StringType(), True)
    ]), True),
    StructField("context", StringType(), True),
    StructField("outcome_status", StructType([
        StructField("category", StringType(), True),
        StructField("date", StringType(), True)
    ]), True),
    StructField("persistent_id", StringType(), True),
    StructField("id", StringType(), True),
    StructField("location_subtype", StringType(), True),
    StructField("month", StringType(), True)
])

# Initialize an empty DataFrame to collect all topics' data
df_all_topics = None

# Process each topic
for topic in topics:
    # Read data from Kafka topic
    df_kafka = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Cast Kafka value to string
    df_raw = df_kafka.select(col("value").cast("string").alias("raw_json"))

    # Parse JSON data
    df_parsed = df_raw.withColumn("parsed_json", from_json(col("raw_json"), json_schema))

    # Transform data
    df_transformed = df_parsed.select(
        to_timestamp(col("parsed_json.month"), "yyyy-MM").alias("timestamp"),
        col("parsed_json.category").alias("crime_type"),
        col("parsed_json.location.street.name").alias("district"),
        when(
            col("parsed_json.category").isin([
                "violent-crime", 
                "robbery", 
                "possession-of-weapons"
            ]), 
            lit("high")
        ).when(
            col("parsed_json.category").isin([
                "burglary", 
                "vehicle-crime", 
                "theft-from-the-person"
            ]), 
            lit("medium")
        ).otherwise(
            lit("low")
        ).alias("severity"),
        col("parsed_json.location.latitude").cast("double").alias("latitude"),
        col("parsed_json.location.longitude").cast("double").alias("longitude"),
        col("parsed_json.outcome_status.category").alias("outcome_status")
    )

    # Append the transformed data to the main DataFrame
    if df_all_topics is None:
        df_all_topics = df_transformed
    else:
        df_all_topics = df_all_topics.union(df_transformed)

# Write the combined DataFrame to a local CSV file
output_path = "/home/JT/all_topics_data.csv"
df_all_topics.coalesce(1) \
    .write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(output_path)



# Stop SparkSession
spark.stop()