"""Adapters for voter registration data sources."""

import pandas as pd
import logging
from typing import Dict, Any, Optional
from tqdm import tqdm

from .base_adapter import BaseAdapter

logger = logging.getLogger(__name__)

class VoterRegistrationAdapter(BaseAdapter):
    """Base adapter for voter registration data."""
    
    def clean_date(self, val):
        """Convert a string to a date.
        
        Args:
            val: Input value
            
        Returns:
            Cleaned date string (YYYY-MM-DD) or None
        """
        if not val or val == '':
            return None
        try:
            return pd.to_datetime(val).strftime('%Y-%m-%d')
        except:
            logger.debug(f"Unable to parse date: {val}")
            return None
    
    def clean_birth_year(self, val):
        """Convert various birth year formats to integer.
        
        Args:
            val: Input value
            
        Returns:
            Integer birth year or None
        """
        if not val or val == '':
            return None
        try:
            import math
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
            logger.debug(f"Unable to parse birth year: {val}")
            return None
    
    def create_raw_source_row(self, row: pd.Series) -> str:
        """Create a raw source row string from a row of data.
        
        Args:
            row: Row of data
            
        Returns:
            String representation of the raw row
        """
        return "|".join(str(val) for val in row)


class RocklandVoterAdapter(VoterRegistrationAdapter):
    """Adapter for Rockland County voter registration data."""
    
    def _process_chunk_impl(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of Rockland voter data.
        
        Args:
            chunk: Raw data chunk
            
        Returns:
            Processed data in standardized format
        """
        chunk = chunk.fillna("")
        
        # Filter out records with missing critical address information
        valid_records = chunk[(chunk['Street Name'].str.strip() != '') & 
                             (chunk['City'].str.strip() != '')]
        
        if len(valid_records) < len(chunk):
            logger.info(f"Filtered out {len(chunk) - len(valid_records)} records with missing address information")
            chunk = valid_records
            
        if len(chunk) == 0:
            logger.warning("No valid records found after filtering")
            return pd.DataFrame()
        
        # Create the outputs with proper type conversions
        registration_ids = [self.generate_id() for _ in range(len(chunk))]
        birth_years = chunk['Birth Year'].apply(self.clean_birth_year)
        dobs = chunk['Birth date'].apply(self.clean_date)
        reg_dates = chunk['Registration Date'].apply(self.clean_date)
        
        # Create the raw_source_row
        raw_sources = chunk.apply(self.create_raw_source_row, axis=1)
        
        # Timestamps
        now = self.get_current_utc()
        
        # Create the output DataFrame in one go
        df = pd.DataFrame({
            'registration_id': registration_ids,
            'voter_id': chunk['Voter Id'],  # This will be replaced in deduplication
            'county': 'Rockland',
            'registration_date': reg_dates,
            'registration_source': chunk['Registration Source'],
            'party': chunk['Party'],
            'voter_status': chunk['Voter Status'],
            'voter_status_reason': chunk['Voter Status Reason'],
            'polling_place_name': chunk.get('Polling Place Name 1', ''),
            'polling_place_address': chunk.get('Polling Place Address 1', ''),
            'source_system': 'rockland_voters',
            'source_record_id': chunk['Voter Id'],
            'created_at': now,
            'updated_at': now,
            
            # Fields for core_voter
            '_first_name': chunk['First Name'],
            '_middle_name': chunk['Middle Name'],
            '_last_name': chunk['Last Name'],
            '_suffix': chunk['Suffix'],
            '_dob': dobs,
            '_birth_year': birth_years,
            '_gender': chunk['Gender'],
            '_phone_number': chunk.get('Phone Number', ''),
            '_email': chunk.get('Email', ''),
            
            # Fields for address
            '_street_number': chunk['Street Number'],
            '_street_name': chunk['Street Name'],
            '_apartment': chunk['Apartment'],
            '_full_address': '',
            '_city': chunk['City'],
            '_state': chunk['State'],
            '_zip': chunk['Zip'],
            '_zip_plus_4': chunk['Zip+4'],
            '_mailing_address': chunk.get('Mailing Address', ''),
            
            # Raw source for reference
            '_raw_source_row': raw_sources,
            
            # Additional field for election history processing
            '_voter_history': chunk.get('Voter History', '')
        })
        
        return df

    def extract_election_history(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Extract election history from Rockland voter data.
        
        In Rockland data, the election history is stored in a comma-separated string
        in the 'Voter History' column.
        
        Args:
            raw_data: Raw voter data
            
        Returns:
            Election history data in standardized format
        """
        logger.info("Extracting election history from Rockland voter data")
        
        # Check if we're working with processed data or raw data
        voter_id_col = 'voter_id' if 'voter_id' in raw_data.columns else 'Voter Id'
        history_col = '_voter_history' if '_voter_history' in raw_data.columns else 'Voter History'
        
        if voter_id_col not in raw_data.columns:
            logger.error(f"Voter ID column '{voter_id_col}' not found in data. Available columns: {raw_data.columns.tolist()}")
            return pd.DataFrame()
            
        if history_col not in raw_data.columns:
            logger.error(f"Voter History column '{history_col}' not found in data. Available columns: {raw_data.columns.tolist()}")
            return pd.DataFrame()
            
        logger.info(f"Using '{voter_id_col}' as voter ID column and '{history_col}' as history column")
        
        # Initialize empty lists for the data
        history = []
        now = self.get_current_utc()
        total_rows = len(raw_data)
        skipped_empty_address = 0
        
        # Process each voter with progress bar
        for _, row in tqdm(raw_data.iterrows(), total=total_rows, desc="Extracting Rockland election history"):
            # Skip records with no address information
            street_num_col = '_street_number' if '_street_number' in raw_data.columns else 'Street Number'
            street_name_col = '_street_name' if '_street_name' in raw_data.columns else 'Street Name'
            city_col = '_city' if '_city' in raw_data.columns else 'City'
            
            street_num = row.get(street_num_col, '')
            street_name = row.get(street_name_col, '')
            city = row.get(city_col, '')
            
            # Skip if we don't have basic address information
            if not street_name or not city:
                skipped_empty_address += 1
                continue
                
            voter_id = row[voter_id_col]
            voter_history = row.get(history_col, '')
            
            if not voter_history or pd.isna(voter_history):
                continue
                
            # Split the history string by commas
            history_entries = voter_history.split(',')
            
            for entry in history_entries:
                entry = entry.strip()
                if not entry:
                    continue
                
                # Parse the election code - typically like "GE00" (General Election 2000)
                # First 2-3 characters are election type (GE, PE, etc.)
                # Last 2 digits are the year (00 for 2000)
                if len(entry) >= 4:
                    if entry[-2:].isdigit():
                        year = entry[-2:]
                        election_type = entry[:-2]
                        
                        record_id = self.generate_id()
                        
                        history.append({
                            'election_record_id': record_id,
                            'voter_id': voter_id,
                            'county': 'Rockland',
                            'election_code': entry,
                            'election_type': election_type,
                            'election_year': int('20' + year) if int(year) < 50 else int('19' + year),
                            'voting_method': 'P',  # Default to 'P' for "Polling Place" since specific method isn't provided
                            'party': '',  # Not provided in Rockland data
                            'absentee_type': '',  # Not provided in Rockland data
                            'source_system': 'rockland_voters',
                            'source_record_id': f"{voter_id}_{entry}",
                            'created_at': now,
                            'updated_at': now
                        })
        
        # Create the DataFrame
        if not history:
            logger.warning("No election history found in Rockland data")
            return pd.DataFrame()
            
        logger.info(f"Extracted {len(history)} valid election history records from Rockland data")
        logger.info(f"Skipped {skipped_empty_address} records with missing address information")
        return pd.DataFrame(history)


class PutnamVoterAdapter(VoterRegistrationAdapter):
    """Adapter for Putnam County voter registration data."""
    
    def _process_chunk_impl(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Process a chunk of Putnam voter data.
        
        Args:
            chunk: Raw data chunk
            
        Returns:
            Processed data in standardized format
        """
        chunk = chunk.fillna("")
        
        # Filter out records with missing critical address information
        valid_records = chunk[(chunk['Street Name'].str.strip() != '') & 
                             (chunk['City'].str.strip() != '')]
        
        if len(valid_records) < len(chunk):
            logger.info(f"Filtered out {len(chunk) - len(valid_records)} records with missing address information")
            chunk = valid_records
            
        if len(chunk) == 0:
            logger.warning("No valid records found after filtering")
            return pd.DataFrame()
        
        # Create the outputs with proper type conversions
        registration_ids = [self.generate_id() for _ in range(len(chunk))]
        birth_years = chunk['Birth Year'].apply(self.clean_birth_year)
        dobs = chunk['Birth date'].apply(self.clean_date)
        reg_dates = chunk['Registration Date'].apply(self.clean_date)
        
        # Create the raw_source_row
        raw_sources = chunk.apply(self.create_raw_source_row, axis=1)
        
        # Timestamps
        now = self.get_current_utc()
        
        # Create the output DataFrame in one go
        df = pd.DataFrame({
            'registration_id': registration_ids,
            'voter_id': chunk['Voter Id'],  # This will be replaced in deduplication
            'county': 'Putnam',
            'registration_date': reg_dates,
            'registration_source': chunk['Registration Source'],
            'party': chunk['Party'],
            'voter_status': chunk['Voter Status'],
            'voter_status_reason': chunk['Voter Status Reason'],
            'polling_place_name': '',
            'polling_place_address': '',
            'source_system': 'putnam_voters',
            'source_record_id': chunk['Voter Id'],
            'created_at': now,
            'updated_at': now,
            
            # Fields for core_voter
            '_first_name': chunk['First Name'],
            '_middle_name': chunk['Middle Name'],
            '_last_name': chunk['Last Name'],
            '_suffix': chunk['Suffix'],
            '_dob': dobs,
            '_birth_year': birth_years,
            '_gender': chunk['Gender'],
            '_phone_number': chunk['Phone Number'],
            '_email': '',
            
            # Fields for address
            '_street_number': chunk['Street Number'],
            '_street_name': chunk['Street Name'],
            '_apartment': chunk['Apartment'],
            '_full_address': chunk.get('Address Line 2', ''),
            '_city': chunk['City'],
            '_state': chunk['State'],
            '_zip': chunk['Zip'],
            '_zip_plus_4': chunk['Zip+4'],
            '_mailing_address': chunk.get('Mailing Address', ''),
            
            # Raw source for reference
            '_raw_source_row': raw_sources
        })
        
        return df
    
    def extract_election_history(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract election history from Putnam voter data.
        
        Args:
            df: Raw voter data
            
        Returns:
            Election history data
        """
        import re
        
        logger.info("Extracting election history from Putnam data")
        history = []
        pattern = re.compile(r'([A-Z]{2,3})(\d{2}):Voting Method')
        
        # Get election columns
        election_cols = [col for col in df.columns if pattern.match(col)]
        logger.info(f"Found {len(election_cols)} election columns")
        valid_records = 0
        skipped_records = 0
        
        # Process each row and extract election history
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Extracting Putnam election history"):
            voter_id = row['Voter Id']
            
            for col in election_cols:
                match = pattern.match(col)
                election_type, year = match.groups()
                base = f"{election_type}{year}:"
                
                # Check if there's an actual voting method value
                voting_method_val = row.get(base + 'Voting Method', '')
                
                # Handle potential NaN/float values 
                if pd.isna(voting_method_val):
                    voting_method = ''
                else:
                    voting_method = str(voting_method_val).strip()
                    
                # Only add if there's a non-empty voting method with actual data
                if voting_method and voting_method.lower() not in ['none', 'n/a', '-', 'unknown']:
                    # Get party value safely
                    party_val = row.get(base + 'Party', '')
                    party = '' if pd.isna(party_val) else str(party_val).strip()
                    
                    # Get absentee type safely
                    absentee_val = row.get(base + 'Absentee Type', '')
                    absentee_type = '' if pd.isna(absentee_val) else str(absentee_val).strip()
                    
                    record_id = self.generate_id()
                    now = self.get_current_utc()
                    valid_records += 1
                    
                    history.append({
                        'election_record_id': record_id,
                        'voter_id': voter_id,  # This will be replaced in deduplication
                        'county': 'Putnam',
                        'election_code': f"{election_type}{year}",
                        'election_type': election_type,
                        'election_year': int('20' + year) if int(year) < 50 else int('19' + year),
                        'voting_method': voting_method,
                        'party': party,
                        'absentee_type': absentee_type,
                        'source_system': 'putnam_voters',
                        'source_record_id': f"{voter_id}_{election_type}{year}",
                        'created_at': now,
                        'updated_at': now
                    })
                else:
                    skipped_records += 1
        
        logger.info(f"Extracted {len(history)} valid election history records (skipped {skipped_records} invalid records)")
        return pd.DataFrame(history) if history else pd.DataFrame()