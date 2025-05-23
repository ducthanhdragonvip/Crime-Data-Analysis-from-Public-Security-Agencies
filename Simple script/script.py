import json
import os
import time
from datetime import datetime
from sodapy import Socrata
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'chicago_crimes'

# Socrata configuration
DATASET_ID = 'ijzp-q8t2'
SOCRATA_DOMAIN = 'data.cityofchicago.org'
TIMESTAMP_FILE = 'last_updated.txt'

# Polling and API configuration
POLL_INTERVAL = 3600  # 1 hour
API_LIMIT = 1000  # Records per API call
MAX_RETRIES = 3  # For API retry logic

def load_last_updated():
    """Load the last updated timestamp from file."""
    try:
        if os.path.exists(TIMESTAMP_FILE):
            with open(TIMESTAMP_FILE, 'r') as f:
                return f.read().strip()
        return None
    except Exception as e:
        logger.error(f"Error loading last updated timestamp: {e}")
        return None

def save_last_updated(timestamp):
    """Save the last updated timestamp to file."""
    try:
        with open(TIMESTAMP_FILE, 'w') as f:
            f.write(timestamp)
    except Exception as e:
        logger.error(f"Error saving last updated timestamp: {e}")

def initialize_producer():
    """Initialize and return a Kafka producer."""
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            retries=5,
            max_block_ms=10000
        )
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        return None

def fetch_records(client, where_clause, offset, limit):
    """Fetch a batch of records from Socrata with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            return client.get(
                DATASET_ID,
                where=where_clause,
                order='updated_on DESC',
                limit=limit,
                offset=offset
            )
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"API attempt {attempt + 1} failed: {e}. Retrying in {2 ** attempt} seconds.")
                time.sleep(2 ** attempt)
            else:
                logger.error(f"API failed after {MAX_RETRIES} attempts: {e}")
                return []

def ingest_crimes():
    """Ingest Chicago crimes data into Kafka with pagination."""
    client = None
    producer = None
    try:
        # Initialize Socrata client
        client = Socrata(SOCRATA_DOMAIN, None)
        
        # Load last updated timestamp
        last_updated = load_last_updated()
        where_clause = f"updated_on > '{last_updated}'" if last_updated else None
        
        # Initialize Kafka producer
        producer = initialize_producer()
        if not producer:
            logger.error("Producer initialization failed, skipping this cycle.")
            return
        
        # Track the latest 'updated_on' timestamp
        latest_updated = last_updated
        offset = 0
        total_records = 0
        
        while True:
            # Fetch a batch of records
            results = fetch_records(client, where_clause, offset, API_LIMIT)
            if not results:
                logger.info(f"No more records to fetch at offset {offset}.")
                break
            
            # Process and send records to Kafka
            for record in results:
                try:
                    record_id = record.get('id')
                    updated_on = record.get('updated_on')
                    
                    # Update latest timestamp if newer
                    if updated_on and (not latest_updated or updated_on > latest_updated):
                        latest_updated = updated_on
                        
                    # Send to Kafka
                    producer.send(KAFKA_TOPIC, key=record_id, value=record)
                except Exception as e:
                    logger.error(f"Error processing record {record.get('id', 'unknown')}: {e}")
                    continue
            
            total_records += len(results)
            logger.info(f"Processed {len(results)} records at offset {offset}.")
            
            # Move to the next batch
            offset += API_LIMIT
            if len(results) < API_LIMIT:
                break  # No more records to fetch
        
        # Flush producer
        producer.flush()
        
        # Save the latest timestamp
        if latest_updated:
            save_last_updated(latest_updated)
            logger.info(f"Updated last timestamp to {latest_updated}")
        
        logger.info(f"Total processed {total_records} records in this cycle.")
        
    except Exception as e:
        logger.error(f"Error in ingestion process: {e}")
    finally:
        if client:
            client.close()
        if producer:
            producer.flush()

def main():
    """Run the ingestion process continuously."""
    logger.info("Starting 24/7 Chicago Crimes Kafka ingestion.")
    while True:
        try:
            ingest_crimes()
            logger.info(f"Sleeping for {POLL_INTERVAL} seconds.")
            time.sleep(POLL_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Received shutdown signal, exiting.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in main loop: {e}")
            logger.info(f"Retrying after {POLL_INTERVAL} seconds.")
            time.sleep(POLL_INTERVAL)

if __name__ == '__main__':
    main()
