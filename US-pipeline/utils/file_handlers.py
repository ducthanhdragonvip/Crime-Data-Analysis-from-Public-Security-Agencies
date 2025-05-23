import os
import logging
from config.settings import settings
from utils.logging import get_logger

logger = get_logger(__name__)

def load_last_updated(timestamp_file=None):
    """Load the last updated timestamp from file."""
    file_path = timestamp_file or settings.TIMESTAMP_FILE
    try:
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                return f.read().strip()
        return None
    except Exception as e:
        logger.error(f"Error loading last updated timestamp from {file_path}: {e}")
        return None

def save_last_updated(timestamp, timestamp_file=None):
    """Save the last updated timestamp to file."""
    file_path = timestamp_file or settings.TIMESTAMP_FILE
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            f.write(timestamp)
    except Exception as e:
        logger.error(f"Error saving last updated timestamp to {file_path}: {e}")