from sodapy import Socrata
import time
import logging
from config.settings import settings
from utils.file_handlers import load_last_updated, save_last_updated
from utils.logging import get_logger

logger = get_logger(__name__)

class SocrataDataFetcher:
    def __init__(self, domain=None, dataset_id=None, timestamp_file=None, update_field=None):
        self.domain = domain 
        self.dataset_id = dataset_id 
        self.timestamp_file = timestamp_file
        self.client = Socrata(self.domain ,app_token=settings.APP_TOKEN)
        self.update_field = update_field 
        

    def fetch_records(self, where_clause, offset, limit):
        """Fetch a batch of records from Socrata with retry logic."""
        for attempt in range(settings.MAX_RETRIES):
            try:
                return self.client.get(
                    self.dataset_id,
                    where=where_clause,
                    order=f'{self.update_field} DESC',
                    limit=limit,
                    offset=offset
                )
            except Exception as e:
                if attempt < settings.MAX_RETRIES - 1:
                    logger.warning(f"API attempt {attempt + 1} failed: {e}. Retrying in {2 ** attempt} seconds.")
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"API failed after {settings.MAX_RETRIES} attempts: {e}")
                    return []

    def get_updated_records(self):
        """Get all records updated since last check."""
        last_updated = load_last_updated(self.timestamp_file)
        where_clause = f"{self.update_field} > '{last_updated}'" if last_updated else None
        
        offset = 0
        all_records = []
        latest_updated = last_updated
        
        while True:
            records = self.fetch_records(where_clause, offset, settings.API_LIMIT)
            if not records:
                break
                
            all_records.extend(records)
            
            # Update latest timestamp
            for record in records:
                updated_on = record.get(self.update_field)
                if updated_on and (not latest_updated or updated_on > latest_updated):
                    latest_updated = updated_on
            
            offset += settings.API_LIMIT
            if len(records) < settings.API_LIMIT:
                break
        
        if latest_updated and latest_updated != last_updated:
            save_last_updated(latest_updated, self.timestamp_file)
            
        return all_records

    def close(self):
        """Close the Socrata client."""
        if self.client:
            self.client.close()