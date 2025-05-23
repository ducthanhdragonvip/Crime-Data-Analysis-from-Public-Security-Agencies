from kafka import KafkaProducer
import json
import logging
from config.settings import settings
from utils.logging import get_logger

logger = get_logger(__name__)

class KafkaProducerWrapper:
    def __init__(self):
        self.producer = self._initialize_producer()

    def _initialize_producer(self):
        """Initialize and return a Kafka producer."""
        try:
            return KafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                retries=5,
                max_block_ms=10000
            )
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            return None

    def send_message(self, topic, key, value):
        """Send a message to Kafka topic."""
        if self.producer is None:
            logger.error("Producer not initialized, cannot send message")
            return False
        
        try:
            self.producer.send(topic, key=key, value=value)
            return True
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            return False

    def flush(self):
        """Flush producer messages."""
        if self.producer:
            self.producer.flush()

    def close(self):
        """Close the producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()