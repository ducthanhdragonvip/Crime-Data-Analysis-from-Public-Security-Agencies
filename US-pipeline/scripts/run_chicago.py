from data_pipeline.processing.spark_processor import ChicagoStreamProcessor
from utils.logging import get_logger
import sys
import os

# Ensure the project root is in sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = get_logger(__name__)

def main():
    """Run the Spark streaming processor for Chicago."""
    logger.info("Starting Chicago Crimes Spark streaming processor.")
    processor = ChicagoStreamProcessor()
    try:
        processor.process_stream()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, exiting.")
    except Exception as e:
        logger.error(f"Error in Chicago processor: {str(e)}")
    finally:
        processor.close()
        logger.info("Chicago processor closed.")

if __name__ == '__main__':
    main()