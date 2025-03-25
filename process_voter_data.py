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
    
    # Get batch size from config
    chunk_size = config.get("processing.chunk_size", 10000)
    logger.info(f"Using batch size of {chunk_size}")
    
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
        "election_history.csv",
        "committees.csv",
        "candidates.csv",
        "donation_history.csv"
    ]
    
    # Import each file
    import_args = []
    for filename in files_to_import:
        file_path = os.path.join(processed_dir, filename)
        if not os.path.exists(file_path):
            logger.warning(f"File {file_path} not found, skipping")
            continue
            
        logger.info(f"Loading {file_path}")
        
        # Get file size to check if we need to process in chunks
        file_size = os.path.getsize(file_path)
        file_too_large = file_size > 100 * 1024 * 1024  # 100MB threshold
        
        if file_too_large:
            logger.info(f"Large file detected ({file_size/1024/1024:.2f} MB), processing in chunks")
            # Process file in chunks
            chunks = []
            for chunk in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
                chunks.append(chunk)
                logger.info(f"Processed chunk of {len(chunk)} rows")
            
            df = pd.concat(chunks, ignore_index=True)
            logger.info(f"Completed loading {len(df)} rows from {filename}")
        else:
            # Load entire file at once
            df = pd.read_csv(file_path, low_memory=False)
            logger.info(f"Loaded {len(df)} rows from {filename}")
            
        import_args.append((df, filename))
    
    # Import to database
    if import_args:
        logger.info(f"Importing {len(import_args)} tables to {db_type} database")
        processor.save_tables(*import_args)
        logger.info("Import completed successfully")
    else:
        logger.warning("No files found to import")
    
    return 0

def process_chunk(args):
    """Process a chunk of donation data.
    
    This function needs to be at module level for multiprocessing to work.
    
    Args:
        args: Tuple of (chunk_data, config_path, db_type)
        
    Returns:
        Tuple of processed data
    """
    chunk_data, config_path, db_type = args
    
    # Create a new processor instance in each worker
    config = Config(config_path)
    processor = DataProcessor(config)
    
    # Set database type if needed
    if db_type:
        config.set("database.type", db_type)
    
    # Process the chunk
    # Load required data from files in each worker to avoid sharing memory
    processed_dir = config.get("paths.output_dir", "data/processed")
    
    # Load existing data if available
    voters_df = pd.DataFrame()
    addresses_df = pd.DataFrame()
    committees_df = pd.DataFrame()
    candidates_df = pd.DataFrame()
    
    # Try to load existing core voters
    voters_path = os.path.join(processed_dir, "core_voters.csv")
    if os.path.exists(voters_path):
        voters_df = pd.read_csv(voters_path, low_memory=False)
    
    # Try to load existing addresses
    addresses_path = os.path.join(processed_dir, "addresses.csv")
    if os.path.exists(addresses_path):
        addresses_df = pd.read_csv(addresses_path, low_memory=False)
    
    # Try to load existing committees
    committees_path = os.path.join(processed_dir, "committees.csv")
    if os.path.exists(committees_path):
        committees_df = pd.read_csv(committees_path, low_memory=False)
    
    # Try to load existing candidates
    candidates_path = os.path.join(processed_dir, "candidates.csv")
    if os.path.exists(candidates_path):
        candidates_df = pd.read_csv(candidates_path, low_memory=False)
    
    # Process the chunk
    return processor.process_donation_data(
        chunk_data, 
        voters_df, 
        addresses_df, 
        committees_df, 
        candidates_df
    )

def process_donation_data(config_path, donation_files, db_type=None):
    """Process donation data, linking with existing voters, addresses, candidates and committees.
    
    Args:
        config_path: Path to the configuration file
        donation_files: List of donation CSV files to process
        db_type: Type of database to use ('sqlite' or 'bigquery')
    """
    # Load config
    config = Config(config_path)
    logger.info(f"Loading configuration from {config_path}")
    
    # Initialize data processor
    processor = DataProcessor(config)
    
    # Set database type if specified
    if db_type:
        logger.info(f"Setting database type to {db_type}")
        config.set("database.type", db_type)
    
    # Get processing configuration
    chunk_size = config.get("processing.chunk_size", 10000)
    num_workers = config.get("processing.num_workers", 1)
    use_multiprocessing = config.get("processing.use_multiprocessing", True)
    
    logger.info(f"Processing configuration: chunk_size={chunk_size}, workers={num_workers}, multiprocessing={use_multiprocessing}")
    
    # Process directory paths
    processed_dir = config.get("paths.output_dir", "data/processed")
    raw_dir = config.get("paths.raw_dir", "data/raw")
    
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)
        
    # Load existing data if available
    voters_df = pd.DataFrame()
    addresses_df = pd.DataFrame()
    committees_df = pd.DataFrame()
    candidates_df = pd.DataFrame()
    donations_df = pd.DataFrame()
    
    # Try to load existing core voters
    voters_path = os.path.join(processed_dir, "core_voters.csv")
    if os.path.exists(voters_path):
        logger.info(f"Loading existing voters from {voters_path}")
        voters_df = pd.read_csv(voters_path, low_memory=False)
    
    # Try to load existing addresses
    addresses_path = os.path.join(processed_dir, "addresses.csv")
    if os.path.exists(addresses_path):
        logger.info(f"Loading existing addresses from {addresses_path}")
        addresses_df = pd.read_csv(addresses_path, low_memory=False)
    
    # Try to load existing committees
    committees_path = os.path.join(processed_dir, "committees.csv")
    if os.path.exists(committees_path):
        logger.info(f"Loading existing committees from {committees_path}")
        committees_df = pd.read_csv(committees_path, low_memory=False)
    
    # Try to load existing candidates
    candidates_path = os.path.join(processed_dir, "candidates.csv")
    if os.path.exists(candidates_path):
        logger.info(f"Loading existing candidates from {candidates_path}")
        candidates_df = pd.read_csv(candidates_path, low_memory=False)
        
    # Process each donation file
    for donation_file in donation_files:
        file_path = os.path.join(raw_dir, donation_file)
        if not os.path.exists(file_path):
            logger.warning(f"Donation file {file_path} not found, skipping")
            continue
            
        logger.info(f"Processing donation file: {file_path}")
        donation_data = pd.read_csv(file_path, low_memory=False)
        total_records = len(donation_data)
        logger.info(f"Processing {total_records} donation records")
        
        # If multiprocessing is disabled or only 1 worker, process all at once
        if not use_multiprocessing or num_workers <= 1 or total_records <= chunk_size:
            logger.info("Processing donations in single batch")
            donors, addresses, committees, candidates, donations = processor.process_donation_data(
                donation_data, voters_df, addresses_df, committees_df, candidates_df
            )
            
            # Update dataframes with new data
            voters_df = donors
            addresses_df = addresses
            committees_df = committees
            candidates_df = candidates
            
            # Append to donations dataframe
            if donations_df.empty:
                donations_df = donations
            else:
                donations_df = pd.concat([donations_df, donations], ignore_index=True)
        else:
            # Process in chunks using multiprocessing
            logger.info(f"Processing donations in chunks of {chunk_size} using {num_workers} workers")
            
            # Split data into chunks
            chunks = [donation_data.iloc[i:i+chunk_size] for i in range(0, len(donation_data), chunk_size)]
            logger.info(f"Split data into {len(chunks)} chunks")
            
            # Create pool and process chunks
            with mp.Pool(processes=num_workers) as pool:
                results = pool.map(process_chunk, [(chunk, config_path, db_type) for chunk in chunks])
                
            # Combine results
            all_donors = []
            all_addresses = []
            all_committees = []
            all_candidates = []
            all_donations = []
            
            for donors, addresses, committees, candidates, donations in results:
                all_donors.append(donors)
                all_addresses.append(addresses)
                all_committees.append(committees)
                all_candidates.append(candidates)
                all_donations.append(donations)
            
            # Merge results into single dataframes
            logger.info("Merging results from all chunks")
            voters_df = pd.concat(all_donors, ignore_index=True).drop_duplicates(subset=['voter_id'])
            addresses_df = pd.concat(all_addresses, ignore_index=True).drop_duplicates(subset=['address_id'])
            committees_df = pd.concat(all_committees, ignore_index=True).drop_duplicates(subset=['committee_id'])
            candidates_df = pd.concat(all_candidates, ignore_index=True).drop_duplicates(subset=['candidate_id'])
            
            # Append to donations dataframe
            donations_chunk = pd.concat(all_donations, ignore_index=True)
            if donations_df.empty:
                donations_df = donations_chunk
            else:
                donations_df = pd.concat([donations_df, donations_chunk], ignore_index=True)
    
    # Save processed data
    if not voters_df.empty:
        voters_output = os.path.join(processed_dir, "core_voters.csv")
        logger.info(f"Saving updated core voters to {voters_output}")
        voters_df.to_csv(voters_output, index=False)
    
    if not addresses_df.empty:
        addresses_output = os.path.join(processed_dir, "addresses.csv")
        logger.info(f"Saving updated addresses to {addresses_output}")
        addresses_df.to_csv(addresses_output, index=False)
    
    if not committees_df.empty:
        committees_output = os.path.join(processed_dir, "committees.csv")
        logger.info(f"Saving updated committees to {committees_output}")
        committees_df.to_csv(committees_output, index=False)
    
    if not candidates_df.empty:
        candidates_output = os.path.join(processed_dir, "candidates.csv")
        logger.info(f"Saving updated candidates to {candidates_output}")
        candidates_df.to_csv(candidates_output, index=False)
    
    if not donations_df.empty:
        donations_output = os.path.join(processed_dir, "donation_history.csv")
        logger.info(f"Saving donations to {donations_output}")
        donations_df.to_csv(donations_output, index=False)
    
    # Import to database if data exists
    import_args = []
    if not voters_df.empty:
        import_args.append((voters_df, "core_voters.csv"))
    if not addresses_df.empty:
        import_args.append((addresses_df, "addresses.csv"))
    if not committees_df.empty:
        import_args.append((committees_df, "committees.csv"))
    if not candidates_df.empty:
        import_args.append((candidates_df, "candidates.csv"))
    if not donations_df.empty:
        import_args.append((donations_df, "donation_history.csv"))
    
    if import_args:
        logger.info(f"Importing {len(import_args)} tables to database")
        processor.save_tables(*import_args)
        logger.info("Import completed successfully")
    
    return 0

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process voter data into normalized tables')
    parser.add_argument('--config', help='Path to config file')
    parser.add_argument('--sources', help='Comma-separated list of sources to process')
    parser.add_argument('--import-only', action='store_true', help='Skip processing and only import to database')
    parser.add_argument('--db-type', choices=['sqlite', 'bigquery'], help='Database type to use')
    parser.add_argument('--donation-files', help='Comma-separated list of donation CSV files to process')
    parser.add_argument('--workers', type=int, help='Number of worker processes (overrides config)')
    parser.add_argument('--chunk-size', type=int, help='Chunk size for batch processing (overrides config)')
    parser.add_argument('--no-multiprocessing', action='store_true', help='Disable multiprocessing')
    args = parser.parse_args()
    
    # Set up paths
    config_path = args.config
    
    # Check if we're only importing to database
    if args.import_only:
        logger.info("Running in import-only mode")
        return import_to_database(config_path, args.db_type)
    
    # Check if we're processing donation data
    if args.donation_files:
        logger.info("Processing donation data")
        donation_files = args.donation_files.split(',')
        return process_donation_data(config_path, donation_files, args.db_type)
    
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
        
        # Override worker count if specified
        if args.workers:
            logger.info(f"Setting worker count to {args.workers}")
            controller.config.set("processing.num_workers", args.workers)
        
        # Override chunk size if specified
        if args.chunk_size:
            logger.info(f"Setting chunk size to {args.chunk_size}")
            controller.config.set("processing.chunk_size", args.chunk_size)
        
        # Override multiprocessing if specified
        if args.no_multiprocessing:
            logger.info("Disabling multiprocessing")
            controller.config.set("processing.use_multiprocessing", False)
        
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