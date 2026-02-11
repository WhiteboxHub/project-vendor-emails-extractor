#!/usr/bin/env python3
"""
Batch Position Classifier
-------------------------
Fetches 'new' raw_positions from the database, classifies them using BERT,
and saves valid job positions to the main positions table.
"""

import logging
import sys
import argparse
import time
from pathlib import Path
from typing import List, Dict

from utils.config import get_config
from utils.logging.logger import get_logger
from utils.api_client import get_api_client
from utils.extraction.bert_classifier import BertPositionClassifier

# Setup Logger
logger = get_logger("batch_classifier")

class PositionBatchProcessor:
    def __init__(self, config_path='config/config.yaml', dry_run=False):
        self.dry_run = dry_run
        
        # Load Config
        self.config_loader = get_config()
        self.config_loader.config_path = Path(config_path)
        self.config = self.config_loader.load()
        
        # Initialize API Client
        try:
            self.api_client = get_api_client()
            logger.info("API client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize API client: {str(e)}")
            sys.exit(1)
            
        # Initialize BERT Classifier
        try:
            # Use GPU if available, otherwise CPU
            self.classifier = BertPositionClassifier(
                model_name="valhalla/distilbart-mnli-12-1" 
            )
            logger.info("BERT Classifier initialized")
        except Exception as e:
            logger.error(f"Failed to initialize BERT Classifier: {str(e)}")
            sys.exit(1)

    def run(self, batch_size=50):
        """Main execution loop"""
        logger.info(f"Starting batch processing (Dry Run: {self.dry_run})")
        
        while True:
            # 1. Fetch Batch
            batch = self.fetch_raw_positions(limit=batch_size)
            
            if not batch:
                logger.info("No new raw positions found. Exiting.")
                break
            
            logger.info(f"Processing batch of {len(batch)} records...")
            
            processed_count = 0
            
            # 2. Process Each Record Individually
            for record in batch:
                record_id = record.get('id')
                raw_title = record.get('raw_title')
                
                # Handle missing title
                if not raw_title:
                    logger.warning(f"Record {record_id}: Missing raw_title. Marking processed.")
                    if not self.dry_run:
                        self.update_raw_status(record_id, "processed")
                    continue
                
                # Construct Rich Input for Better Classification
                # "Title: ... \n Company: ... \n Description: ..."
                raw_company = record.get('raw_company', '') or ''
                raw_desc = record.get('raw_description', '') or ''
                
                # Truncate description to avoid token limit issues (500 chars is usually enough for context)
                rich_text = f"Title: {raw_title}\nCompany: {raw_company}\nDescription:\n{raw_desc[:500]}"
                
                # Run Classification on Rich Text
                try:
                    result = self.classifier.classify(rich_text)
                    logger.info(f"Record {record_id}: [{result['label'].upper()}] '{raw_title}' (Score: {result['score']:.2f})")
                    
                    if result['is_valid']:
                        # Valid Position -> Save to positions table
                        position_data = {
                            "job_title": raw_title,
                            "company_name": record.get('raw_company'),
                            "location": record.get('raw_location'),
                            "source": "email_bot_v2",
                            "raw_position_id": record_id,
                            "confidence_score": result['score'],
                            "classification_label": result['label']
                        }
                        
                        if not self.dry_run:
                            saved = self.save_position(position_data)
                            if saved:
                                logger.info(f"Record {record_id}: Saved to positions")
                    
                    # Always mark as processed if we attempted classification
                    if not self.dry_run:
                        self.update_raw_status(record_id, "processed")
                        processed_count += 1
                        
                except Exception as e:
                    logger.error(f"Error processing record {record_id}: {str(e)}")
            
            if self.dry_run:
                logger.info("[DRY RUN] Completed batch processing logic (no DB writes)")
                break # Exit after one batch in dry run
                
            time.sleep(1)

    def fetch_raw_positions(self, limit=50) -> List[Dict]:
        """Fetch raw positions with processing_status='new'"""
        try:
            # GET /api/raw-positions/
            response = self.api_client.get(
                "/api/raw-positions/", 
                params={"status": "new", "limit": limit}
            )
            
            if isinstance(response, list):
                return response
            elif isinstance(response, dict):
                if 'results' in response: return response['results']
                if 'data' in response: return response['data']
            return []
            
        except Exception as e:
            logger.error(f"Error fetching raw positions: {str(e)}")
            return []

    def save_position(self, position_data: Dict) -> bool:
        """Save a single position"""
        try:
            # POST /api/positions/
            self.api_client.post("/api/positions/", position_data)
            return True
        except Exception as e:
            logger.error(f"Failed to save position: {str(e)}")
            return False

    def update_raw_status(self, record_id: int, status: str):
        """Update status of a single raw position"""
        try:
            # PATCH /api/raw-positions/{id}/
            self.api_client.patch(
                f"/api/raw-positions/{record_id}/", 
                {"processing_status": status}
            )
        except Exception as e:
            logger.error(f"Failed to update status for {record_id}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Batch validate job positions using BERT')
    parser.add_argument('--dry-run', action='store_true', help='Run without saving to DB')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size')
    args = parser.parse_args()

    processor = PositionBatchProcessor(dry_run=args.dry_run)
    processor.run(batch_size=args.batch_size)

if __name__ == "__main__":
    main()
