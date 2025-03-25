import pandas as pd
import re
from datetime import datetime, timezone
import os
from tqdm import tqdm
import time
import logging
import csv
import math
import multiprocessing as mp
from functools import partial
import numpy as np

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def clean_date(val):
    try:
        return pd.to_datetime(val).date()
    except:
        return None

def clean_birth_year(val):
    """Convert various birth year formats to integer, handling floating point values."""
    if not val or val == '':
        return None
    try:
        # First try to convert directly to int if possible
        if isinstance(val, int):
            return val
        
        # If it's a string that can be parsed as a float (like "1972.0")
        if isinstance(val, str) and '.' in val:
            return int(float(val))
            
        # If it's already a float
        if isinstance(val, float):
            if math.isnan(val):
                return None
            return int(val)
            
        # Any other string or numeric format
        return int(val)
    except:
        logger.warning(f"Unable to parse birth year: {val}")
        return None

def get_current_utc():
    """Return current UTC time in ISO format using timezone-aware method"""
    return datetime.now(timezone.utc).isoformat()

def process_rockland_chunk(chunk):
    """Process a single chunk of Rockland data"""
    chunk = chunk.fillna("")
    
    # Filter out records with missing critical address information
    valid_records = chunk[(chunk['Street Name'].str.strip() != '') & 
                         (chunk['City'].str.strip() != '')]
    
    if len(valid_records) < len(chunk):
        logger.debug(f"Filtered out {len(chunk) - len(valid_records)} records with missing address information")
        chunk = valid_records
        
    if len(chunk) == 0:
        return pd.DataFrame()
    
    # Vectorized operations where possible
    birth_years = chunk['Birth Year'].apply(clean_birth_year)
    dobs = chunk['Date of Birth'].apply(clean_date)
    reg_dates = chunk['Registration Date'].apply(clean_date)
    
    # Create the raw_source_row column more efficiently
    raw_source = chunk.astype(str).agg('|'.join, axis=1)
    
    df = pd.DataFrame({
        'county': 'Rockland',
        'voter_id': chunk['Voter Id'],
        'first_name': chunk['First Name'],
        'middle_name': chunk['Middle Name'],
        'last_name': chunk['Last Name'],
        'suffix': chunk['Suffix'],
        'street_number': chunk['Street Number'],
        'street_name': chunk['Street Name'],
        'apartment': chunk['Apartment'],
        'full_address': chunk['Residence Address'],
        'city': chunk['City'],
        'state': chunk['State'],
        'zip': chunk['Zip'],
        'zip_plus_4': '',
        'dob': dobs,
        'birth_year': birth_years,
        'gender': chunk['Gender'],
        'registration_date': reg_dates,
        'registration_source': chunk['Registration Source'],
        'party': chunk['Party'],
        'voter_status': chunk['Status'],
        'voter_status_reason': chunk['Status Reason'],
        'phone_number': chunk['Phone'],
        'email': chunk['E-Mail'],
        'mailing_address': chunk['Mailing Address 1'],
        'polling_place_name': chunk['Polling Place Name 1'],
        'polling_place_address': chunk['Polling Place Address 1'],
        'ingestion_timestamp': get_current_utc(),
        'raw_source_row': raw_source
    })
    return df

def normalize_rockland(path, chunksize=10000, num_workers=None):
    """Process Rockland data with multiprocessing"""
    if num_workers is None:
        num_workers = max(1, mp.cpu_count() - 1)  # Leave one CPU free
    
    logger.info(f"Starting to process Rockland data with {num_workers} workers in chunks of {chunksize} records")
    chunks = pd.read_csv(path, dtype=str, chunksize=chunksize)
    
    # Convert chunks to a list so we can count them
    chunk_list = list(chunks)
    total_records = sum(len(chunk) for chunk in chunk_list)
    logger.info(f"Found {len(chunk_list)} chunks with {total_records} total records")
    
    # Process chunks in parallel
    with mp.Pool(processes=num_workers) as pool:
        dfs = list(tqdm(
            pool.imap(process_rockland_chunk, chunk_list),
            total=len(chunk_list),
            desc="Processing Rockland chunks"
        ))
    
    logger.info(f"Completed Rockland processing: {total_records} total records")
    return pd.concat(dfs, ignore_index=True)

def process_putnam_chunk(chunk):
    """Process a single chunk of Putnam data"""
    chunk = chunk.fillna("")
    
    # Filter out records with missing critical address information
    valid_records = chunk[(chunk['Street Name'].str.strip() != '') & 
                         (chunk['City'].str.strip() != '')]
    
    if len(valid_records) < len(chunk):
        logger.debug(f"Filtered out {len(chunk) - len(valid_records)} records with missing address information")
        chunk = valid_records
        
    if len(chunk) == 0:
        return pd.DataFrame(), pd.DataFrame()
    
    # Vectorized operations where possible
    birth_years = chunk['Birth Year'].apply(clean_birth_year)
    dobs = chunk['Birth date'].apply(clean_date)
    reg_dates = chunk['Registration Date'].apply(clean_date)
    
    # Create the raw_source_row column more efficiently
    raw_source = chunk.astype(str).agg('|'.join, axis=1)
    
    df = pd.DataFrame({
        'county': 'Putnam',
        'voter_id': chunk['Voter Id'],
        'first_name': chunk['First Name'],
        'middle_name': chunk['Middle Name'],
        'last_name': chunk['Last Name'],
        'suffix': chunk['Suffix'],
        'street_number': chunk['Street Number'],
        'street_name': chunk['Street Name'],
        'apartment': chunk['Apartment'],
        'full_address': chunk['Address Line 2'],
        'city': chunk['City'],
        'state': chunk['State'],
        'zip': chunk['Zip'],
        'zip_plus_4': chunk['Zip+4'],
        'dob': dobs,
        'birth_year': birth_years,
        'gender': chunk['Gender'],
        'registration_date': reg_dates,
        'registration_source': chunk['Registration Source'],
        'party': chunk['Party'],
        'voter_status': chunk['Voter Status'],
        'voter_status_reason': chunk['Voter Status Reason'],
        'phone_number': chunk['Phone Number'],
        'email': '',
        'mailing_address': chunk['Mailing Address'],
        'polling_place_name': '',
        'polling_place_address': '',
        'ingestion_timestamp': get_current_utc(),
        'raw_source_row': raw_source
    })
    return df, chunk

def normalize_putnam(path, chunksize=10000, num_workers=None):
    """Process Putnam data with multiprocessing"""
    if num_workers is None:
        num_workers = max(1, mp.cpu_count() - 1)  # Leave one CPU free
    
    logger.info(f"Starting to process Putnam data with {num_workers} workers in chunks of {chunksize} records")
    chunks = pd.read_csv(path, dtype=str, chunksize=chunksize)
    
    # Convert chunks to a list so we can count them
    chunk_list = list(chunks)
    total_records = sum(len(chunk) for chunk in chunk_list)
    logger.info(f"Found {len(chunk_list)} chunks with {total_records} total records")
    
    # Process chunks in parallel
    with mp.Pool(processes=num_workers) as pool:
        results = list(tqdm(
            pool.imap(process_putnam_chunk, chunk_list),
            total=len(chunk_list),
            desc="Processing Putnam chunks"
        ))
    
    # Separate the results
    dfs = [result[0] for result in results]
    raw_dfs = [result[1] for result in results]
    
    logger.info(f"Completed Putnam processing: {total_records} total records")
    return pd.concat(dfs, ignore_index=True), pd.concat(raw_dfs, ignore_index=True)

def process_election_batch(args):
    """Process a batch of election history data"""
    batch_df, columns, voter_id_col = args
    history = []
    pattern = re.compile(r'([A-Z]{2,3})(\d{2}):Voting Method')
    valid_count = 0
    skipped_count = 0
    
    for col in columns:
        match = pattern.match(col)
        election_type, year = match.groups()
        base = f"{election_type}{year}:"
        
        for _, row in batch_df.iterrows():
            # Get the voting method and make sure it's not empty
            voting_method_val = row.get(base + 'Voting Method', '')
            
            # Handle potential NaN/float values safely
            if pd.isna(voting_method_val):
                voting_method = ''
            else:
                voting_method = str(voting_method_val).strip()
                
            # Only include records with actual participation
            if voting_method and voting_method.lower() not in ['none', 'n/a', '-', 'unknown', '']:
                # Handle party and absentee type values safely
                party_val = row.get(base + 'Party', '')
                party = '' if pd.isna(party_val) else str(party_val).strip()
                
                absentee_val = row.get(base + 'Absentee Type', '')
                absentee_type = '' if pd.isna(absentee_val) else str(absentee_val).strip()
                
                history.append({
                    'voter_id': row[voter_id_col],
                    'county': 'Putnam',
                    'election_code': f"{election_type}{year}",
                    'election_type': election_type,
                    'election_year': int('20' + year) if int(year) < 50 else int('19' + year),
                    'voting_method': voting_method,
                    'party': party,
                    'absentee_type': absentee_type,
                    'source_column': col
                })
                valid_count += 1
            else:
                skipped_count += 1
    
    return history, valid_count, skipped_count

def extract_election_history(df, voter_id_col='Voter Id', num_workers=None):
    """Extract election history using multiprocessing"""
    if num_workers is None:
        num_workers = max(1, mp.cpu_count() - 1)  # Leave one CPU free
        
    logger.info(f"Starting election history extraction with {num_workers} workers")
    pattern = re.compile(r'([A-Z]{2,3})(\d{2}):Voting Method')
    
    # Get all election columns at once
    election_cols = [col for col in df.columns if pattern.match(col)]
    logger.info(f"Found {len(election_cols)} election columns to process")
    
    # Split the dataframe into batches
    batches = []
    batch_size = max(100, len(df) // (num_workers * 4))  # Ensure enough batches for good parallelism
    
    for i in range(0, len(df), batch_size):
        end = min(i + batch_size, len(df))
        batch_df = df.iloc[i:end]
        batches.append((batch_df, election_cols, voter_id_col))
    
    logger.info(f"Split data into {len(batches)} batches for parallel processing")
    
    # Process batches in parallel
    with mp.Pool(processes=num_workers) as pool:
        results = list(tqdm(
            pool.imap(process_election_batch, batches),
            total=len(batches),
            desc="Processing election batches"
        ))
    
    # Extract results and counts
    history_results = [item[0] for item in results]
    valid_total = sum(item[1] for item in results)
    skipped_total = sum(item[2] for item in results)
    
    # Flatten the list of lists
    history = [item for sublist in history_results for item in sublist]
    logger.info(f"Completed election history extraction: {len(history)} valid records (skipped {skipped_total} empty or invalid records)")
    
    return pd.DataFrame(history)

def main():
    start_time = time.time()
    rockland_file = "rockland_voters.csv"
    putnam_file = "putnam_voters.csv"
    
    # Use all but one CPU core
    num_workers = max(1, mp.cpu_count() - 1)
    logger.info(f"Using {num_workers} CPU cores for processing")

    if not os.path.exists(rockland_file) or not os.path.exists(putnam_file):
        logger.error("❌ Missing 'rockland_voters.csv' or 'putnam_voters.csv' in the current directory.")
        return

    logger.info("🔄 Starting data normalization process")
    
    # Process Rockland
    rockland_start = time.time()
    rockland = normalize_rockland(rockland_file, num_workers=num_workers)
    rockland_time = time.time() - rockland_start
    logger.info(f"✅ Rockland processing completed in {rockland_time:.2f} seconds ({len(rockland):,} records, {len(rockland)/rockland_time:.2f} records/sec)")

    # Process Putnam
    putnam_start = time.time()
    putnam, raw_putnam = normalize_putnam(putnam_file, num_workers=num_workers)
    putnam_time = time.time() - putnam_start
    logger.info(f"✅ Putnam processing completed in {putnam_time:.2f} seconds ({len(putnam):,} records, {len(putnam)/putnam_time:.2f} records/sec)")

    # Extract election history
    history_start = time.time()
    election_history = extract_election_history(raw_putnam, num_workers=num_workers)
    history_time = time.time() - history_start
    logger.info(f"✅ Election history extraction completed in {history_time:.2f} seconds ({len(election_history):,} records, {len(election_history)/history_time:.2f} records/sec)")

    # Write output files
    logger.info("💾 Writing output files...")
    write_start = time.time()
    full = pd.concat([rockland, putnam])
    
    # Write with proper data types - increase performance with higher chunksize
    logger.info(f"Writing {len(full):,} voter records to CSV...")
    full['birth_year'] = full['birth_year'].apply(lambda x: int(x) if pd.notnull(x) else "")
    full.to_csv("voters.csv", index=False, quoting=csv.QUOTE_ALL, chunksize=50000)
    logger.info(f"Writing {len(election_history):,} election history records to CSV...")
    election_history.to_csv("election_history.csv", index=False, quoting=csv.QUOTE_ALL, chunksize=50000)
    write_time = time.time() - write_start
    logger.info(f"✅ Files written in {write_time:.2f} seconds")
    
    total_time = time.time() - start_time
    logger.info(f"✨ Total processing time: {total_time:.2f} seconds")
    logger.info(f"📊 Total records processed: {len(full):,}")
    logger.info(f"📊 Total election history records: {len(election_history):,}")
    logger.info(f"📊 Overall processing speed: {(len(full) + len(election_history))/total_time:.2f} records/sec")

if __name__ == "__main__":
    # Ensure proper multiprocessing on macOS
    mp.set_start_method('spawn', force=True)
    main()
