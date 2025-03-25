#!/usr/bin/env python3
"""Main script for processing voter data."""

import os
import sys
import argparse
import logging
import time
import multiprocessing as mp
import pandas as pd

from src.pipeline import PipelineController
from src.processors.data_processor import DataProcessor
from src.utils import Config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def import_to_database(config_path, db_type):
    """Import existing processed files directly to the specified database.
    
    Args:
        config_path: Path to the configuration file
        db_type: Type of database to import to ('sqlite' or 'bigquery')
    """
    # Load config
    config = Config(config_path)
    logger.info(f"Loading configuration from {config_path}")
    
    # Initialize data processor
    processor = DataProcessor(config)
    
    # Set database type
    if db_type:
        logger.info(f"Setting database type to {db_type}")
        config.set("database.type", db_type)
    
    # Process directory with CSV files
    processed_dir = config.get("paths.output_dir", "data/processed")
    
    if not os.path.exists(processed_dir):
        logger.error(f"Processed data directory {processed_dir} not found")
        return 1
    
    # Define the files to process
    files_to_import = [
        "core_voters.csv",
        "addresses.csv",
        "voter_registrations.csv",
        "election_history.csv"
    ]
    
    # Import each file
    import_args = []
    for filename in files_to_import:
        file_path = os.path.join(processed_dir, filename)
        if not os.path.exists(file_path):
            logger.warning(f"File {file_path} not found, skipping")
            continue
            
        logger.info(f"Loading {file_path}")
        df = pd.read_csv(file_path)
        import_args.append((df, filename))
    
    # Import to database
    if import_args:
        logger.info(f"Importing {len(import_args)} tables to {db_type} database")
        processor.save_tables(*import_args)
        logger.info("Import completed successfully")
    else:
        logger.warning("No files found to import")
    
    return 0

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process voter data into normalized tables')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--sources', help='Comma-separated list of sources to process')
    parser.add_argument('--import-only', action='store_true', help='Skip processing and only import to database')
    parser.add_argument('--db-type', choices=['sqlite', 'bigquery'], help='Database type to use')
    args = parser.parse_args()
    
    # Set up paths
    config_path = args.config
    
    # Check if we're only importing to database
    if args.import_only:
        logger.info("Running in import-only mode")
        return import_to_database(config_path, args.db_type)
    
    # Set up sources
    if args.sources:
        sources = args.sources.split(',')
    else:
        sources = ['rockland', 'putnam']  # Default sources
    
    logger.info("Starting voter data processing")
    logger.info(f"Processing sources: {sources}")
    
    start_time = time.time()
    
    # Create and run the pipeline
    try:
        # Initialize the pipeline controller
        controller = PipelineController(config_path)
        
        # Override database type if specified
        if args.db_type:
            logger.info(f"Setting database type to {args.db_type}")
            controller.config.set("database.type", args.db_type)
        
        # Run the pipeline
        controller.run_pipeline(sources)
        
        total_time = time.time() - start_time
        logger.info(f"Total processing time: {total_time:.2f} seconds")
        logger.info("Voter data processing completed successfully")
        
    except Exception as e:
        logger.exception(f"Error processing voter data: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    # Ensure proper multiprocessing on macOS
    mp.set_start_method('spawn', force=True)
    sys.exit(main()) 