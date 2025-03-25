#!/usr/bin/env python
"""Process donation data into the database."""

import os
import logging
import argparse
import pandas as pd
from tqdm import tqdm
from typing import Dict, Any, List, Tuple, Optional
import yaml
import uuid

from src.utils.config import Config
from src.utils.db import DatabaseManager
from src.adapters.donation_adapter import FECDonationAdapter
from src.utils.entity_matching import EntityMatcher

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_donations(config_path: str, donation_files: List[str]) -> None:
    """Process donation data into the database.
    
    Args:
        config_path: Path to config file
        donation_files: List of donation files to process
    """
    # Load configuration
    logger.info(f"Loading configuration from {config_path}")
    config = Config(config_path)
    
    # Initialize database connection
    logger.info("Initializing database connection")
    db = DatabaseManager(config)
    
    # Track overall statistics
    total_donations = 0
    total_unique_donors = 0
    total_matched_donors = 0
    total_new_donors = 0
    total_unique_committees = 0
    total_unique_candidates = 0
    total_processed_records = 0
    total_error_count = 0
    
    # Check if core_voters table exists and initialize entity matcher
    try:
        core_voters = db.read_table("core_voters")
        logger.info(f"Found {len(core_voters)} existing core voters")
    except Exception as e:
        logger.warning(f"Could not read core_voters table: {e}")
        logger.info("Creating database tables")
        db.create_tables()
        core_voters = pd.DataFrame()
    
    # Initialize entity matcher if we have existing voters
    if not core_voters.empty:
        logger.info("Initializing entity matcher")
        matcher = EntityMatcher(config)
        matcher.load_entities(core_voters)
    else:
        matcher = None
    
    # Process each donation file
    for file_path in donation_files:
        logger.info(f"Processing donation file: {file_path}")
        
        # Initialize the donation adapter
        adapter = FECDonationAdapter(config, source_name="fec_donations")
        
        # Set the entity matcher
        if matcher:
            adapter.entity_matcher = matcher
        
        # Process the donation file
        donation_data = adapter.process_file(file_path)
        
        if donation_data.empty:
            logger.warning(f"No donation data processed from {file_path}")
            continue
        
        # Match entities
        if matcher:
            adapter.match_entities()
        
        # Get adapter stats
        adapter_report = adapter.report()
        total_processed_records += adapter_report["records_processed"]
        total_error_count += adapter_report["error_count"]
        total_unique_donors += adapter_report["unique_donors"]
        total_matched_donors += adapter_report["matched_voters"]
        total_new_donors += adapter_report["new_voters"]
        total_unique_committees += adapter_report["committee_count"]
        total_unique_candidates += adapter_report["candidate_count"]
        
        file_donations = len(donation_data)
        total_donations += file_donations
        
        # Get core voters, committees, and candidates extracted from donations
        new_core_voters = adapter.get_core_voters()
        committees = adapter.get_committees()
        candidates = adapter.get_candidates()
        
        # Handle committees - deduplicate and save to database
        if not committees.empty:
            # Read existing committees if available
            try:
                existing_committees = db.read_table("committees")
                logger.info(f"Found {len(existing_committees)} existing committees")
            except:
                existing_committees = pd.DataFrame()
            
            # Deduplicate committees
            unique_committees = deduplicate_committees(committees, existing_committees)
            logger.info(f"Found {len(unique_committees)} new unique committees")
            
            # Save unique committees to database
            if not unique_committees.empty:
                logger.info(f"Saving {len(unique_committees)} new committees to database")
                db.save_dataframe(unique_committees, "committees")
        
        # Handle candidates - deduplicate and save to database
        if not candidates.empty:
            # Read existing candidates if available
            try:
                existing_candidates = db.read_table("candidates")
                logger.info(f"Found {len(existing_candidates)} existing candidates")
            except:
                existing_candidates = pd.DataFrame()
            
            # Deduplicate candidates
            unique_candidates = deduplicate_candidates(candidates, existing_candidates)
            logger.info(f"Found {len(unique_candidates)} new unique candidates")
            
            # Save unique candidates to database
            if not unique_candidates.empty:
                logger.info(f"Saving {len(unique_candidates)} new candidates to database")
                db.save_dataframe(unique_candidates, "candidates")
        
        # Handle core voters - deduplicate and save
        if not new_core_voters.empty:
            # Process addresses for new voters
            processed_core_voters, addresses = process_new_voters(new_core_voters)
            
            # Save new core voters to database
            if not processed_core_voters.empty:
                logger.info(f"Saving {len(processed_core_voters)} new core voters to database")
                db.save_dataframe(processed_core_voters, "core_voters")
            
            # Save addresses to database
            if not addresses.empty:
                logger.info(f"Saving {len(addresses)} new addresses to database")
                db.save_dataframe(addresses, "addresses")
        
        # Check for existing transaction IDs to avoid constraint violations
        try:
            existing_donations = db.read_table("donation_history")
            if not existing_donations.empty:
                logger.info(f"Found {len(existing_donations)} existing donations")
                
                # Get set of existing transaction IDs
                existing_ids = set(existing_donations['transaction_id'])
                
                # Find any duplicate IDs
                duplicate_ids = set(donation_data['transaction_id']).intersection(existing_ids)
                
                if duplicate_ids:
                    logger.warning(f"Found {len(duplicate_ids)} duplicate transaction IDs")
                    
                    # Make duplicate transaction IDs unique
                    for idx, row in donation_data.iterrows():
                        if row['transaction_id'] in duplicate_ids:
                            # Append a unique suffix to make it unique
                            new_id = f"{row['transaction_id']}_{uuid.uuid4().hex[:8]}"
                            donation_data.at[idx, 'transaction_id'] = new_id
                            logger.debug(f"Changed transaction ID {row['transaction_id']} to {new_id}")
        except Exception as e:
            logger.warning(f"Could not check for duplicate transaction IDs: {e}")
        
        # Save donation data to database
        logger.info(f"Saving {len(donation_data)} donation records to database")
        db.save_dataframe(donation_data, "donation_history")
    
    # Print overall summary
    logger.info("=== Processing Summary ===")
    logger.info(f"Total records processed: {total_processed_records}")
    logger.info(f"Total records with errors: {total_error_count}")
    logger.info(f"Total donation records processed: {total_donations}")
    logger.info(f"Total unique donors identified: {total_unique_donors}")
    logger.info(f"Total donors matched to existing voters: {total_matched_donors}")
    logger.info(f"Total new voters created: {total_new_donors}")
    logger.info(f"Total unique committees added: {total_unique_committees}")
    logger.info(f"Total unique candidates added: {total_unique_candidates}")
    logger.info("========================")
    
    # Close database connection
    db.close()
    logger.info("Done processing donation data")

def deduplicate_committees(new_committees: pd.DataFrame, existing_committees: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate committees against existing data.
    
    Args:
        new_committees: DataFrame of new committees
        existing_committees: DataFrame of existing committees
        
    Returns:
        DataFrame of unique new committees
    """
    if existing_committees.empty:
        # No existing committees, so just deduplicate among new committees
        return new_committees.drop_duplicates(subset=['committee_id'])
    
    # Get set of existing committee IDs
    existing_ids = set(existing_committees['committee_id'])
    
    # Filter out committees that already exist
    unique_committees = new_committees[~new_committees['committee_id'].isin(existing_ids)]
    
    # Deduplicate remaining new committees
    return unique_committees.drop_duplicates(subset=['committee_id'])

def deduplicate_candidates(new_candidates: pd.DataFrame, existing_candidates: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate candidates against existing data.
    
    Args:
        new_candidates: DataFrame of new candidates
        existing_candidates: DataFrame of existing candidates
        
    Returns:
        DataFrame of unique new candidates
    """
    if existing_candidates.empty:
        # No existing candidates, so just deduplicate among new candidates
        return new_candidates.drop_duplicates(subset=['candidate_id'])
    
    # Get set of existing candidate IDs
    existing_ids = set(existing_candidates['candidate_id'])
    
    # Filter out candidates that already exist
    unique_candidates = new_candidates[~new_candidates['candidate_id'].isin(existing_ids)]
    
    # Deduplicate remaining new candidates
    return unique_candidates.drop_duplicates(subset=['candidate_id'])

def match_donors_to_voters(donors: pd.DataFrame, matcher: EntityMatcher) -> Dict[str, str]:
    """Match donors to existing core voters.
    
    Args:
        donors: DataFrame of donors
        matcher: Entity matcher instance
        
    Returns:
        Dictionary mapping donation voter_id to matched existing voter_id
    """
    matches = {}
    match_counts = {"attempted": 0, "matched": 0, "failed": 0}
    
    logger.info(f"Attempting to match {len(donors)} donors to existing voters")
    
    for _, donor in donors.iterrows():
        match_counts["attempted"] += 1
        
        # Create match record with donor information
        match_record = {
            'first_name': donor['first_name'],
            'last_name': donor['last_name'],
            'middle_name': donor.get('middle_name', ''),
            'city': donor.get('_city', ''),
            'state': donor.get('_state', ''),
            'zip': donor.get('_zip', '')
        }
        
        # Diagnostic logging for every 100th record
        if match_counts["attempted"] % 100 == 0:
            logger.debug(f"Matching donor: {match_record}")
        
        # Try to match
        match_results = matcher.match_entity(match_record)
        
        if match_results and match_results.get('match_found', False):
            # Store mapping from donor voter_id to matched voter_id
            matches[donor['voter_id']] = match_results['entity_id']
            match_counts["matched"] += 1
            
            # Diagnostic logging for successful matches
            if match_counts["matched"] % 10 == 0:
                logger.debug(f"Matched donor: {match_record} to voter ID: {match_results['entity_id']} with score: {match_results['match_score']}")
        else:
            match_counts["failed"] += 1
    
    # Log matching statistics
    logger.info(f"Donor matching stats: {match_counts['matched']} matched, {match_counts['failed']} unmatched out of {match_counts['attempted']} attempts")
    
    return matches

def process_new_voters(voters: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Process new core voters and create addresses.
    
    Args:
        voters: DataFrame of voters
        
    Returns:
        Tuple of (core_voters DataFrame, addresses DataFrame)
    """
    if voters.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Create addresses for the voters
    addresses = []
    now = pd.Timestamp.now().isoformat()
    
    # Process each voter to extract address information
    for _, voter in voters.iterrows():
        if not voter.get('_street_1') or not voter.get('_city') or not voter.get('_state'):
            continue
            
        # Generate address ID
        address_id = str(pd.util.hash_pandas_object(voter[['_street_1', '_street_2', '_city', '_state', '_zip']]).sum())
        
        addresses.append({
            'address_id': address_id,
            'voter_id': voter['voter_id'],
            'street_address': f"{voter.get('_street_1', '')} {voter.get('_street_2', '')}".strip(),
            'city': voter.get('_city', ''),
            'state': voter.get('_state', ''),
            'zip_code': voter.get('_zip', ''),
            'county': '',  # Not available in donation data
            'is_current': True,
            'created_at': now,
            'source_system': 'fec_donations'
        })
        
        # Update current_address_id in the voter record
        voters.loc[voters['voter_id'] == voter['voter_id'], 'current_address_id'] = address_id
    
    # Clean up temporary columns used for processing
    processed_voters = voters.copy()
    if '_street_1' in processed_voters.columns:
        columns_to_remove = [
            col for col in processed_voters.columns 
            if col.startswith('_') and col in processed_voters.columns
        ]
        processed_voters = processed_voters.drop(columns=columns_to_remove)
    
    return processed_voters, pd.DataFrame(addresses)

def update_donations_with_voter_ids(donations: pd.DataFrame, matches: Dict[str, str]) -> pd.DataFrame:
    """Update donation records with matched voter IDs.
    
    Args:
        donations: DataFrame of donations
        matches: Dictionary mapping donation voter_id to matched existing voter_id
        
    Returns:
        Updated donations DataFrame
    """
    # Make a copy to avoid modifying the original
    updated_donations = donations.copy()
    
    # Update voter_id for matched donors
    for donation_voter_id, matched_voter_id in matches.items():
        updated_donations.loc[updated_donations['voter_id'] == donation_voter_id, 'voter_id'] = matched_voter_id
    
    return updated_donations

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Process donation data into database')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--files', nargs='+', required=True, help='Donation files to process')
    args = parser.parse_args()
    
    process_donations(args.config, args.files)

if __name__ == '__main__':
    main() 