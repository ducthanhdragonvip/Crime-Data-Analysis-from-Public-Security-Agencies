import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    APP_TOKEN = os.getenv('APP_TOKEN')
    GCS_BUCKET = os.getenv('GCS_BUCKET')

    # Chicago Configuration
    CHICAGO_SOCRATA_DOMAIN = os.getenv('CHICAGO_SOCRATA_DOMAIN')
    CHICAGO_DATASET_ID = os.getenv('CHICAGO_DATASET_ID')
    CHICAGO_KAFKA_TOPIC = os.getenv('CHICAGO_KAFKA_TOPIC')
    CHICAGO_TIMESTAMP_FILE = os.getenv('CHICAGO_TIMESTAMP_FILE')
    CHICAGO_UPDATE_FIELD = os.getenv('CHICAGO_UPDATE_FIELD')
    CHICAGO_ID_FIELD = os.getenv('CHICAGO_ID_FIELD')
    CHICAGO_OUTPUT_PATH = f"gs://{GCS_BUCKET}/US-chicago/2020To-Present/streaming_output"
    CHICAGO_CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/US-chicago/checkpoints"

    # Seattle Configuration
    SEATTLE_SOCRATA_DOMAIN = os.getenv('SEATTLE_SOCRATA_DOMAIN')
    SEATTLE_DATASET_ID = os.getenv('SEATTLE_DATASET_ID')
    SEATTLE_KAFKA_TOPIC = os.getenv('SEATTLE_KAFKA_TOPIC')
    SEATTLE_TIMESTAMP_FILE = os.getenv('SEATTLE_TIMESTAMP_FILE')
    SEATTLE_UPDATE_FIELD = os.getenv('SEATTLE_UPDATE_FIELD')
    SEATTLE_ID_FIELD = os.getenv('SEATTLE_ID_FIELD')
    SEATTLE_OUTPUT_PATH = f"gs://{GCS_BUCKET}/US-seattle/streaming_output"
    SEATTLE_CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/US-seattle/checkpoints"

    # Spark Configuration
    SPARK_MASTER = os.getenv('SPARK_MASTER')
    SERVICE_ACCOUNT_KEY_PATH = os.getenv('SERVICE_ACCOUNT_KEY_PATH')
    
    # Processing Configuration
    POLL_INTERVAL = int(os.getenv('POLL_INTERVAL'))
    API_LIMIT = int(os.getenv('API_LIMIT'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES'))

settings = Settings()