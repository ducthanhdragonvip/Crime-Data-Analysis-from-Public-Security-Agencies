import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
    APP_TOKEN = os.getenv('APP_TOKEN', 'FG5Z82U02Y2QbLeIADvHAGA85') ## evergreen.data.socrata.com

    GCS_BUCKET = os.getenv('GCS_BUCKET', 'crime-data-group8')

    
    # Chicago Configuration
    CHICAGO_SOCRATA_DOMAIN = os.getenv('CHICAGO_SOCRATA_DOMAIN', 'data.cityofchicago.org')
    CHICAGO_DATASET_ID = os.getenv('CHICAGO_DATASET_ID', 'ijzp-q8t2')
    CHICAGO_KAFKA_TOPIC = os.getenv('CHICAGO_KAFKA_TOPIC', 'chicago_crimes')
    CHICAGO_TIMESTAMP_FILE = os.getenv('CHICAGO_TIMESTAMP_FILE', 'data/last_updated.txt')
    CHICAGO_UPDATE_FIELD = os.getenv('CHICAGO_UPDATE_FIELD', 'updated_on')
    CHICAGO_ID_FIELD = os.getenv('CHICAGO_ID_FIELD', 'id')
    CHICAGO_OUTPUT_PATH = f"gs://{GCS_BUCKET}/US-chicago/2020To-Present/streaming_output"
    CHICAGO_CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/US-chicago/checkpoints"


    # Seattle Configuration
    SEATTLE_SOCRATA_DOMAIN = os.getenv('SEATTLE_SOCRATA_DOMAIN', 'cos-data.seattle.gov')
    SEATTLE_DATASET_ID = os.getenv('SEATTLE_DATASET_ID', 'tazs-3rd5')
    SEATTLE_KAFKA_TOPIC = os.getenv('SEATTLE_KAFKA_TOPIC', 'seattle_crimes')
    SEATTLE_TIMESTAMP_FILE = os.getenv('SEATTLE_TIMESTAMP_FILE', 'data/last_updated_seattle.txt')
    SEATTLE_UPDATE_FIELD = os.getenv('SEATTLE_UPDATE_FIELD', 'report_date_time')
    SEATTLE_ID_FIELD = os.getenv('SEATTLE_ID_FIELD', 'report_number')
    SEATTLE_OUTPUT_PATH = f"gs://{GCS_BUCKET}/US-seattle/streaming_output"
    SEATTLE_CHECKPOINT_PATH = f"gs://{GCS_BUCKET}/US-seattle/checkpoints"

    
    # Spark Configuration
    SPARK_MASTER = os.getenv('SPARK_MASTER', 'local[*]')
    SERVICE_ACCOUNT_KEY_PATH = os.getenv('SERVICE_ACCOUNT_KEY_PATH', '/home/JT/test-456807-0581efb4fbc8.json')
    
    # Processing Configuration
    POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', 3600))  # 1 hour
    API_LIMIT = int(os.getenv('API_LIMIT', 1000))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))
    
    

    APP_TOKEN = os.getenv('APP_TOKEN','FG5Z82U02Y2QbLeIADvHAGA85')

settings = Settings()