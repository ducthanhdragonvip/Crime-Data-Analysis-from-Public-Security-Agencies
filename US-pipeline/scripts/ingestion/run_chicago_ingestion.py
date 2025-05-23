import time
from data_pipeline.ingestion.socrata_client import SocrataDataFetcher
from data_pipeline.ingestion.producer import KafkaProducerWrapper
from config.settings import settings
from utils.logging import get_logger
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
logger = get_logger(__name__)

def ingest_crimes():
    """Main ingestion function."""
    data_fetcher = SocrataDataFetcher(
        timestamp_file=settings.CHICAGO_TIMESTAMP_FILE,
        domain=settings.CHICAGO_SOCRATA_DOMAIN,
        dataset_id=settings.CHICAGO_DATASET_ID,
        update_field=settings.CHICAGO_UPDATE_FIELD

    )
    producer = KafkaProducerWrapper()
    
    try:
        records = data_fetcher.get_updated_records()
        logger.info(f"Fetched {len(records)} new records.")
        
        for record in records:
            record_id = record.get('id')
            if record_id:
                producer.send_message(settings.CHICAGO_KAFKA_TOPIC, record_id, record)
        
        producer.flush()
        logger.info("Records sent to Kafka successfully.")
        
    except Exception as e:
        logger.error(f"Error in ingestion process: {e}")
    finally:
        data_fetcher.close()
        producer.close()

def main():
    """Run the ingestion process continuously."""
    logger.info("Starting Chicago Crimes Kafka ingestion service.")
    while True:
        try:
            ingest_crimes()
            logger.info(f"Sleeping for {settings.POLL_INTERVAL} seconds.")
            time.sleep(settings.POLL_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Received shutdown signal, exiting.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            logger.info(f"Retrying after {settings.POLL_INTERVAL} seconds.")
            time.sleep(settings.POLL_INTERVAL)

if __name__ == '__main__':
    main()