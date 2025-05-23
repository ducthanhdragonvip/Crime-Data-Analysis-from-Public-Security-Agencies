import time
import logging
from retrying import retry
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import UnknownTopicOrPartitionError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg ,lit ,to_timestamp ,struct ,from_json, date_format ,to_json ,coalesce, when
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('stream_to_gcs.log')
    ]
)
logger = logging.getLogger(__name__)

# Configuration variables
GCS_BUCKET = 'crime-data-group8'
OUTPUT_PATH = f"gs://{GCS_BUCKET}/US-chicago/2020To-Present/test/streaming_output"
CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/checkpoints"
KAFKA_BOOTSTRAP_SERVERS = '10.128.0.3:9092'  # Update if Kafka is on a different host
KAFKA_TOPIC = 'chicago_crimes'
SERVICE_ACCOUNT_KEY_PATH = '/home/JT/test-456807-0581efb4fbc8.json'  # Update path
SPARK_MASTER = 'spark://10.128.0.3:7077'

def validate_kafka_topic():
    """Validate that the Kafka topic exists."""
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        topics = admin_client.list_topics()
        admin_client.close()
        if KAFKA_TOPIC not in topics:
            logger.error(f"Kafka topic '{KAFKA_TOPIC}' does not exist.")
            return False
        logger.info(f"Kafka topic '{KAFKA_TOPIC}' validated successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed to validate Kafka topic: {str(e)}")
        return False

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
def initialize_spark():
    """Initialize Spark session with retry logic."""
    logger.info("Initializing Spark session...")
    if not os.path.exists(SERVICE_ACCOUNT_KEY_PATH):
        logger.error(f"Service account key file not found: {SERVICE_ACCOUNT_KEY_PATH}")
        raise FileNotFoundError(f"Service account key file not found: {SERVICE_ACCOUNT_KEY_PATH}")
    
    try:
        spark = SparkSession.builder \
            .appName("KafkaToGCSIntegration") \
            .master(SPARK_MASTER) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", SERVICE_ACCOUNT_KEY_PATH) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        logger.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise

# Schema for Chicago crimes data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("case_number", StringType(), True),
    StructField("date", StringType(), True),
    StructField("block", StringType(), True),
    StructField("iucr", StringType(), True),
    StructField("primary_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("location_description", StringType(), True),
    StructField("arrest", BooleanType(), True),
    StructField("domestic", BooleanType(), True),
    StructField("beat", StringType(), True),
    StructField("district", StringType(), True),
    StructField("ward", StringType(), True),
    StructField("community_area", StringType(), True),
    StructField("fbi_code", StringType(), True),
    StructField("x_coordinate", StringType(), True),  # Changed to StringType
    StructField("y_coordinate", StringType(), True),  # Changed to StringType
    StructField("year", StringType(), True),         # Changed to StringType
    StructField("updated_on", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("location", StructType([
        StructField("latitude", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("human_address", StringType(), True)
    ]), True)
])

def write_batch(batch_df, batch_id):
    """Write batch data to GCS bucket in CSV format."""
    try:
        batch_to_write = batch_df.select(
            "ID", "Case Number", "Date", "Block", "IUCR",
            "Primary Type", "Description", "Location Description",
            "arrest", "domestic", "beat", "district", "ward",
            "Community Area", "FBI Code", "X Coordinate", "Y Coordinate",
            "year", "Updated On", "latitude", "longitude"
        )
        
        batch_to_write.coalesce(1) \
            .write \
            .format("csv") \
            .mode("append") \
            .option("header", "false") \
            .option("compression", "none") \
            .save(OUTPUT_PATH)
        logger.info(f"Batch {batch_id} written to {OUTPUT_PATH}")
    except Exception as e:
        logger.error(f"Error writing batch {batch_id}: {str(e)}")

def main():
    """Main function to run the streaming process continuously."""
    logger.info("Starting Kafka to GCS streaming process.")
    
    while True:
        spark = None
        query = None
        try:
            # Validate Kafka topic
            if not validate_kafka_topic():
                logger.info("Waiting 60 seconds before retrying Kafka topic validation...")
                time.sleep(60)
                continue
            
            # Initialize Spark session
            spark = initialize_spark()
            
            # Read streaming data from Kafka
            df_kafka = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
                .option("subscribe", KAFKA_TOPIC) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            # Parse JSON data and select fields
            streaming_df = df_kafka \
                .selectExpr("CAST(value AS STRING) as raw_json") \
                .select(from_json(col("raw_json"), schema).alias("data")) \
                .select("data.*") \
                .withColumnRenamed("id", "ID") \
                .withColumnRenamed("case_number", "Case Number") \
                .withColumnRenamed("date", "Date") \
                .withColumnRenamed("block", "Block") \
                .withColumnRenamed("iucr", "IUCR") \
                .withColumnRenamed("primary_type", "Primary Type") \
                .withColumnRenamed("description", "Description") \
                .withColumnRenamed("location_description", "Location Description") \
                .withColumnRenamed("community_area", "Community Area") \
                .withColumnRenamed("fbi_code", "FBI Code") \
                .withColumnRenamed("x_coordinate", "X Coordinate") \
                .withColumnRenamed("y_coordinate", "Y Coordinate") \
                .withColumnRenamed("updated_on", "Updated On") \
                .withColumn("Updated On", to_timestamp("Updated On")) \
                .withColumn("Date", to_timestamp("Date"))
            
            # Format the streaming data
            formatted_streaming_df = streaming_df.select(
    col("ID"),
    col("Case Number"),
    date_format(col("Date"), "yyyy-MM-dd HH:mm:ss").alias("Date"),
    col("Block"),
    col("IUCR"),
    col("Primary Type"),
    col("Description"),
    col("Location Description"),
    col("arrest"),
    col("domestic"),
    col("beat"),
    col("district"),
    col("ward"),
    col("Community Area"),
    col("FBI Code"),
    # Handle coordinates without casting since they're already strings
    col("X Coordinate"),
    col("Y Coordinate"),
    col("year"),
    date_format(col("Updated On"), "yyyy-MM-dd HH:mm:ss").alias("Updated On"),
    col("latitude"),
    col("longitude"),
    # Handle location with proper struct type
    when(col("location").isNotNull(), 
        to_json(struct(
            coalesce(col("location.latitude"), col("latitude")).alias("latitude"),
            coalesce(col("location.longitude"), col("longitude")).alias("longitude"),
            coalesce(col("location.human_address"), 
                    lit("{\"address\": \"\", \"city\": \"\", \"state\": \"\", \"zip\": \"\"}"))
                .alias("human_address")
        ))
    ).alias("location")
)
            
            # Start the streaming query
            query = formatted_streaming_df \
                .writeStream \
                .foreachBatch(write_batch) \
                .option("checkpointLocation", CHECKPOINT_PATH) \
                .trigger(processingTime="1 minute") \
                .start()
            
            logger.info("Streaming query started successfully.")
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal, stopping gracefully...")
            if query:
                query.stop()
            if spark:
                spark.stop()
            logger.info("Streaming process stopped.")
            break
        except UnknownTopicOrPartitionError as e:
            logger.error(f"Kafka topic error: {str(e)}")
            logger.info("Retrying in 60 seconds...")
            if query:
                query.stop()
            if spark:
                spark.stop()
            time.sleep(60)
            continue
        except Exception as e:
            logger.error(f"Unexpected error in streaming process: {str(e)}")
            logger.info("Restarting streaming process in 30 seconds...")
            if query:
                query.stop()
            if spark:
                spark.stop()
            time.sleep(30)
            continue

if __name__ == '__main__':
    main()