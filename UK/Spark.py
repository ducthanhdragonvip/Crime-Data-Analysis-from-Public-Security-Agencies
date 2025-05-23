import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, lit, struct, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType,IntegerType

# Set environment variables for Java and Spark
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
os.environ["SPARK_HOME"] = "/usr/lib/spark"

# Initialize SparkSession with Kafka and GCS configurations
spark = SparkSession.builder \
    .appName("GCS Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "test-456807-0581efb4fbc8.json") \
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


csv_schema = StructType([
    StructField("DR_NO", StringType(), True),
    StructField("Date Rptd", StringType(), True),
    StructField("DATE OCC", StringType(), True),
    StructField("TIME OCC", StringType(), True),
    StructField("AREA", StringType(), True),
    StructField("AREA NAME", StringType(), True),
    StructField("Rpt Dist No", StringType(), True),
    StructField("Part 1-2", StringType(), True),
    StructField("Crm Cd", StringType(), True),
    StructField("Crm Cd Desc", StringType(), True),
    StructField("Mocodes", StringType(), True),
    StructField("Vict Age", IntegerType(), True),
    StructField("Vict Sex", StringType(), True),
    StructField("Vict Descent", StringType(), True),
    StructField("Premis Cd", StringType(), True),
    StructField("Premis Desc", StringType(), True),
    StructField("Weapon Used Cd", StringType(), True),
    StructField("Weapon Desc", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Status Desc", StringType(), True),
    StructField("Crm Cd 1", StringType(), True),
    StructField("Crm Cd 2", StringType(), True),
    StructField("Crm Cd 3", StringType(), True),
    StructField("Crm Cd 4", StringType(), True),
    StructField("LOCATION", StringType(), True),
    StructField("Cross Street", StringType(), True),
    StructField("Lat", DoubleType(), True),
    StructField("Lon", DoubleType(), True)
])

df_csv_kafka = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "crime_csv_topic") \
                            .option("startingOffsets", "earliest").load() 

df_csv_raw = df_csv_kafka.select(col("value").cast("string").alias("raw_json"))
df_csv_parsed = df_csv_raw.withColumn("parsed_json", from_json(col("raw_json"), csv_schema))

def normalize_csv_data(df):
    df_clean = df.withColumn("timestamp", to_timestamp(col("parsed_json.DATE OCC"), "MM/dd/yyyy hh:mm:ss a")) \
                 .withColumn("month", col("timestamp").substr(6, 2).cast("int")) \
                 .withColumn("year", col("timestamp").substr(1, 4).cast("int"))
    
    df_clean = df_clean.withColumn("severity",
        when(col("parsed_json.Crm Cd Desc").isin(["VEHICLE - STOLEN", "BURGLARY FROM VEHICLE", "ROBBERY", "RESISTING ARREST"]), "medium")
        .when(col("parsed_json.Crm Cd Desc").isin(["BIKE - STOLEN", "SHOPLIFTING-GRAND THEFT ($950.01 & OVER)", "PANDERING"]), "low")
        .otherwise("high")
    )

    df_normalized = df_clean.select(
        "timestamp",
        "month",
        "year",
        col("parsed_json.Crm Cd Desc").alias("crime_type"),
        col("parsed_json.AREA").alias("AREA"),
        "severity",
        col("parsed_json.Lat").alias("latitude"),
        col("parsed_json.Lon").alias("longitude"),
        col("parsed_json.Premis Desc").alias("location"),
        col("parsed_json.Status").alias("outcome_status")
    )
    return df_normalized

df_csv_normalized = normalize_csv_data(df_csv_parsed)
    
    
# Base GCS bucket
gcs_bucket = 'crime-data-group8'

kafka_dfs = []
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
        # Create timestamp from month column (format: yyyy-MM)
        to_timestamp(col("parsed_json.month"), "yyyy-MM").alias("timestamp"),
        
        # Crime type (use category as crime_type)
        col("parsed_json.category").alias("crime_type"),
        
        # District (use street name as a proxy for district since LSOA name is not available)
        col("parsed_json.location.street.name").alias("AREA"),
        
        # Severity mapping based on crime type
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
        # Location struct (lat/lon)
        col("parsed_json.location.latitude").cast("double").alias("latitude"),
        col("parsed_json.location.longitude").cast("double").alias("longitude"),
        col("parsed_json.location_type").alias("location"),
        
        # Outcome status (extract category from outcome_status)
        col("parsed_json.outcome_status.category").alias("outcome_status")
    )
    
    df_transformed = df_transformed.withColumn("month", col("timestamp").substr(6, 2).cast("int")) \
                                  .withColumn("year", col("timestamp").substr(1, 4).cast("int"))
    kafka_dfs.append(df_transformed)


df_kafka_unified = kafka_dfs[0].unionAll(*kafka_dfs[1:])
# Union normalized CSV and Kafka data
df_unified = df_csv_normalized.union(df_kafka_unified)


output_csv_path = "/home/JT/all_data_merge/data_merge.csv"

# Write unified data to CSV on GCS
df_unified.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_csv_path)

# Stop SparkSession
spark.stop()