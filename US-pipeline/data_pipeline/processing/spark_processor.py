import time
import logging
from abc import ABC, abstractmethod
from retrying import retry
from kafka import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_json, struct, coalesce, when, to_timestamp, from_json, lit
from config.settings import settings
from utils.logging import get_logger
from data_pipeline.processing.schemas import chicago_crimes_schema, seattle_crimes_schema

class BaseStreamProcessor(ABC):
    def __init__(self, app_name, kafka_topic, checkpoint_path, output_path, schema):
        self.logger = get_logger(self.__class__.__name__)
        self.kafka_topic = kafka_topic
        self.checkpoint_path = checkpoint_path
        self.output_path = output_path
        self.schema = schema
        self.spark = self._initialize_spark(app_name)

    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000, wait_exponential_max=10000)
    def _initialize_spark(self, app_name):
        """Initialize Spark session with retry logic."""
        self.logger.info(f"Initializing Spark session for {app_name}...")
        try:
            spark = SparkSession.builder \
                .appName(app_name) \
                .master(settings.SPARK_MASTER) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
                .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
                .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
                .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", settings.SERVICE_ACCOUNT_KEY_PATH) \
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
                .config("spark.streaming.stopGracefullyOnShutdown", "true") \
                .config("spark.sql.streaming.checkpointLocation", self.checkpoint_path) \
                .config("spark.sql.adaptive.enabled", "false") \
                .getOrCreate()
            self.logger.info(f"Spark session for {app_name} initialized successfully.")
            return spark
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark session for {app_name}: {str(e)}")
            raise

    def _validate_kafka_topic(self):
        """Validate that the Kafka topic exists."""
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS[0])
            topics = admin_client.list_topics()
            admin_client.close()
            if self.kafka_topic not in topics:
                self.logger.error(f"Kafka topic '{self.kafka_topic}' does not exist.")
                return False
            self.logger.info(f"Kafka topic '{self.kafka_topic}' validated successfully.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to validate Kafka topic '{self.kafka_topic}': {str(e)}")
            return False

    @abstractmethod
    def _process_streaming_df(self, df_kafka):
        """Process the streaming DataFrame (to be implemented by subclasses)."""
        pass

    @abstractmethod
    def _write_batch(self, batch_df, batch_id):
        """Write batch data to GCS bucket (to be implemented by subclasses)."""
        pass

    def process_stream(self):
        """Main method to process the streaming data."""
        self.logger.info(f"Starting streaming processing for {self.__class__.__name__}.")
        
        while True:
            query = None
            try:
                if not self._validate_kafka_topic():
                    self.logger.info("Waiting 60 seconds before retrying Kafka topic validation...")
                    time.sleep(60)
                    continue
                
                df_kafka = self.spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", ",".join(settings.KAFKA_BOOTSTRAP_SERVERS)) \
                    .option("subscribe", self.kafka_topic) \
                    .option("startingOffsets", "latest") \
                    .option("failOnDataLoss", "false") \
                    .load()
                
                streaming_df = self._process_streaming_df(df_kafka)
                
                query = streaming_df \
                    .writeStream \
                    .foreachBatch(self._write_batch) \
                    .option("checkpointLocation", self.checkpoint_path) \
                    .trigger(processingTime="1 minute") \
                    .start()
                
                self.logger.info("Streaming query started successfully.")
                query.awaitTermination()
                
            except KeyboardInterrupt:
                self.logger.info("Received shutdown signal, stopping gracefully...")
                if query:
                    query.stop()
                break
            except UnknownTopicOrPartitionError as e:
                self.logger.error(f"Kafka topic error: {str(e)}")
                self.logger.info("Retrying in 60 seconds...")
                if query:
                    query.stop()
                time.sleep(60)
                continue
            except Exception as e:
                self.logger.error(f"Unexpected error in streaming process: {str(e)}")
                self.logger.info("Restarting streaming process in 30 seconds...")
                if query:
                    query.stop()
                time.sleep(30)
                continue

    def close(self):
        """Close the Spark session."""
        if self.spark:
            self.spark.stop()
            self.logger.info("Spark session closed.")

class ChicagoStreamProcessor(BaseStreamProcessor):
    def __init__(self):
        super().__init__(
            app_name="ChicagoCrimesStreamProcessor",
            kafka_topic=settings.CHICAGO_KAFKA_TOPIC,
            checkpoint_path=settings.CHICAGO_CHECKPOINT_PATH,
            output_path=settings.CHICAGO_OUTPUT_PATH,
            schema=chicago_crimes_schema
        )

    def _process_streaming_df(self, df_kafka):
        """Process the Chicago streaming DataFrame."""
        streaming_df = df_kafka \
            .selectExpr("CAST(value AS STRING) as raw_json") \
            .select(from_json(col("raw_json"), self.schema).alias("data")) \
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
            col("X Coordinate"),
            col("Y Coordinate"),
            col("year"),
            date_format(col("Updated On"), "yyyy-MM-dd HH:mm:ss").alias("Updated On"),
            col("latitude"),
            col("longitude"),
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
        return formatted_streaming_df

    def _write_batch(self, batch_df, batch_id):
        """Write Chicago batch data to GCS bucket in CSV format."""
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
                .save(self.output_path)
            self.logger.info(f"Batch {batch_id} written to {self.output_path} successfully.")
        except Exception as e:
            self.logger.error(f"Error writing batch {batch_id}: {str(e)}")

class SeattleStreamProcessor(BaseStreamProcessor):
    def __init__(self):
        super().__init__(
            app_name="SeattleCrimesStreamProcessor",
            kafka_topic=settings.SEATTLE_KAFKA_TOPIC,
            checkpoint_path=settings.SEATTLE_CHECKPOINT_PATH,
            output_path=settings.SEATTLE_OUTPUT_PATH,
            schema=seattle_crimes_schema
        )

    def _process_streaming_df(self, df_kafka):
        """Process the Seattle streaming DataFrame."""
        streaming_df = df_kafka \
            .selectExpr("CAST(value AS STRING) as raw_json") \
            .select(from_json(col("raw_json"), self.schema).alias("data")) \
            .select("data.*") \
            .withColumnRenamed("report_number", "Report Number") \
            .withColumnRenamed("report_date_time", "Report Date Time") \
            .withColumnRenamed("offense_id", "Offense ID") \
            .withColumnRenamed("offense_date", "Offense Date") \
            .withColumnRenamed("nibrs_group_a_b", "NIBRS Group A/B") \
            .withColumnRenamed("nibrs_crime_against_category", "NIBRS Crime Against Category") \
            .withColumnRenamed("offense_sub_category", "Offense Sub Category") \
            .withColumnRenamed("shooting_type_group", "Shooting Type Group") \
            .withColumnRenamed("block_address", "Block Address") \
            .withColumnRenamed("beat", "Beat") \
            .withColumnRenamed("precinct", "Precinct") \
            .withColumnRenamed("sector", "Sector") \
            .withColumnRenamed("neighborhood", "Neighborhood") \
            .withColumnRenamed("reporting_area", "Reporting Area") \
            .withColumnRenamed("offense_category", "Offense Category") \
            .withColumnRenamed("nibrs_offense_code_description", "NIBRS Offense Code Description") \
            .withColumnRenamed("nibrs_offense_code", "NIBRS Offense Code") \
            .withColumn("latitude", col("latitude").cast("double")) \
            .withColumn("longitude", col("longitude").cast("double"))

        formatted_streaming_df = streaming_df.select(
            col("Report Number"),
            date_format(col("Report Date Time"), "yyyy-MM-dd HH:mm:ss").alias("Report Date Time"),
            col("Offense ID"),
            date_format(col("Offense Date"), "yyyy-MM-dd HH:mm:ss").alias("Offense Date"),
            col("NIBRS Group A/B"),
            col("NIBRS Crime Against Category"),
            col("Offense Sub Category"),
            col("Shooting Type Group"),
            col("Block Address"),
            col("latitude"),
            col("longitude"),
            col("Beat"),
            col("Precinct"),
            col("Sector"),
            col("Neighborhood"),
            col("Reporting Area"),
            col("Offense Category"),
            col("NIBRS Offense Code Description"),
            col("NIBRS Offense Code")
        )
        return formatted_streaming_df

    def _write_batch(self, batch_df, batch_id):
        """Write Seattle batch data to GCS bucket in CSV format."""
        try:
            batch_to_write = batch_df.select(
                "Report Number", "Report Date Time", "Offense ID", "Offense Date",
                "NIBRS Group A/B", "NIBRS Crime Against Category", "Offense Sub Category",
                "Shooting Type Group", "Block Address", "latitude", "longitude",
                "Beat", "Precinct", "Sector", "Neighborhood", "Reporting Area",
                "Offense Category", "NIBRS Offense Code Description", "NIBRS Offense Code"
            )
            
            batch_to_write.coalesce(1) \
                .write \
                .format("csv") \
                .mode("append") \
                .option("header", "false") \
                .option("compression", "none") \
                .save(self.output_path)
            self.logger.info(f"Batch {batch_id} written to {self.output_path} successfully.")
        except Exception as e:
            self.logger.error(f"Error writing batch {batch_id}: {str(e)}")