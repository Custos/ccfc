#!/usr/bin/env python3
"""Script to validate voter data CSV files."""

import os
import sys
import argparse
import pandas as pd
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validate_rockland_file(file_path):
    """Validate Rockland voter data file."""
    logger.info(f"Validating Rockland file: {file_path}")
    
    # Required columns for Rockland voter data
    required_columns = [
        "Voter Id", "First Name", "Middle Name", "Last Name", "Suffix",
        "Street Number", "Street Name", "Apartment", "City", "State", "Zip",
        "Birth date", "Birth Year", "Gender", "Registration Date", 
        "Registration Source", "Party", "Voter Status", "Voter Status Reason"
    ]
    
    # Read the first few rows to get column names
    try:
        sample = pd.read_csv(file_path, nrows=5)
        columns = sample.columns.tolist()
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            logger.error(f"Missing required columns in {file_path}: {missing_columns}")
            return False
            
        # Sample data validation
        logger.info(f"Column count: {len(columns)}")
        logger.info(f"Sample data:\n{sample.head()}")
        
        # Check if Voter History column exists
        if "Voter History" in columns:
            logger.info("Voter History column found")
            has_history = sample["Voter History"].notna().any()
            logger.info(f"Contains election history data: {has_history}")
        else:
            logger.warning("Voter History column not found")
        
        # Count total rows
        total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header row
        logger.info(f"Total rows in file: {total_rows}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating file {file_path}: {e}")
        return False

def validate_putnam_file(file_path):
    """Validate Putnam voter data file."""
    logger.info(f"Validating Putnam file: {file_path}")
    
    # Required columns for Putnam voter data
    required_columns = [
        "Voter Id", "First Name", "Middle Name", "Last Name", "Suffix",
        "Street Number", "Street Name", "Apartment", "City", "State", "Zip",
        "Birth date", "Birth Year", "Gender", "Registration Date", 
        "Registration Source", "Party", "Voter Status", "Voter Status Reason"
    ]
    
    # Read the first few rows to get column names
    try:
        sample = pd.read_csv(file_path, nrows=5)
        columns = sample.columns.tolist()
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in columns]
        
        if missing_columns:
            logger.error(f"Missing required columns in {file_path}: {missing_columns}")
            return False
            
        # Sample data validation
        logger.info(f"Column count: {len(columns)}")
        logger.info(f"Sample data:\n{sample.head()}")
        
        # Check for election history columns (GE*, PE*, etc.)
        election_columns = [col for col in columns if col.startswith(('GE', 'PE', 'PP'))]
        if election_columns:
            logger.info(f"Found {len(election_columns)} election history columns")
        else:
            logger.warning("No election history columns found")
        
        # Count total rows
        total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header row
        logger.info(f"Total rows in file: {total_rows}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating file {file_path}: {e}")
        return False

def validate_donations_file(file_path):
    """Validate donations data file."""
    logger.info(f"Validating donations file: {file_path}")
    
    # Some expected columns for FEC donations
    expected_columns = [
        "committee_id", "contributor_name", "contributor_first_name", 
        "contributor_last_name", "contributor_city", "contributor_state", 
        "contributor_zip", "contribution_receipt_date", "contribution_receipt_amount"
    ]
    
    # Read the first few rows to get column names
    try:
        sample = pd.read_csv(file_path, nrows=5)
        columns = sample.columns.tolist()
        
        # Check for expected columns
        found_columns = [col for col in expected_columns if col in columns]
        
        # Sample data validation
        logger.info(f"Column count: {len(columns)}")
        logger.info(f"Found {len(found_columns)}/{len(expected_columns)} expected columns")
        logger.info(f"Sample data:\n{sample.head()}")
        
        # Count total rows
        total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header row
        logger.info(f"Total rows in file: {total_rows}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating file {file_path}: {e}")
        return False

def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Validate voter data CSV files')
    parser.add_argument('file_paths', nargs='+', help='CSV file paths to validate')
    parser.add_argument('--type', choices=['rockland', 'putnam', 'donations'], 
                        help='Type of data to validate')
    args = parser.parse_args()
    
    success = True
    
    for file_path in args.file_paths:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            success = False
            continue
            
        # Auto-detect type if not specified
        file_type = args.type
        if not file_type:
            if 'rockland' in file_path.lower():
                file_type = 'rockland'
            elif 'putnam' in file_path.lower():
                file_type = 'putnam'
            elif 'donation' in file_path.lower() or 'fec' in file_path.lower():
                file_type = 'donations'
            else:
                logger.warning(f"Could not determine file type for {file_path}, assuming rockland")
                file_type = 'rockland'
        
        # Validate the file
        if file_type == 'rockland':
            file_valid = validate_rockland_file(file_path)
        elif file_type == 'putnam':
            file_valid = validate_putnam_file(file_path)
        elif file_type == 'donations':
            file_valid = validate_donations_file(file_path)
        
        if not file_valid:
            success = False
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 