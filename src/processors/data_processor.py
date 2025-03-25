"""Data processor for normalized entity tables."""

import pandas as pd
import numpy as np
import logging
import json
from typing import Dict, List, Tuple, Optional, Set, Any
import uuid
import os

logger = logging.getLogger(__name__)

class DataProcessor:
    """Process data into normalized entity tables."""
    
    def __init__(self, config):
        """Initialize the data processor.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.output_dir = config.get("paths.output_dir", "data/processed")
        
        # Ensure output directory exists
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def process_registration_data(self, data: pd.DataFrame, 
                                 matches: Dict[Tuple[str, str], Dict[str, Any]]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Process voter registration data into normalized tables.
        
        Args:
            data: Registration data
            matches: Entity match information
            
        Returns:
            Tuple of (core_voters, addresses, registrations) DataFrames
        """
        logger.info(f"Processing {len(data)} registration records into normalized tables")
        
        # Replace the voter_id with the matched entity_id
        for idx, row in data.iterrows():
            source_system = row.get('source_system', 'unknown')
            source_id = row.get('source_record_id', 'unknown')
            key = (source_system, source_id)
            
            if key in matches:
                entity_id = matches[key]['entity_id']
                data.at[idx, 'voter_id'] = entity_id
        
        # Create core voters dataframe
        core_voters = self._extract_core_voters(data)
        
        # Create addresses dataframe
        addresses = self._extract_addresses(data)
        
        # Create registrations dataframe
        registrations = self._extract_registrations(data, addresses)
        
        logger.info(f"Created {len(core_voters)} core voters, {len(addresses)} addresses, {len(registrations)} registrations")
        return core_voters, addresses, registrations
    
    def process_election_history(self, data: pd.DataFrame, 
                             matches: Dict[Tuple[str, str], Dict[str, Any]]) -> pd.DataFrame:
        """Process election history data.
        
        Args:
            data: Election history data
            matches: Entity match information
            
        Returns:
            Processed election history DataFrame
        """
        logger.info(f"Processing {len(data)} election history records")
        
        # Replace the voter_id with the matched entity_id
        for idx, row in data.iterrows():
            source_system = row.get('source_system', 'unknown')
            
            # Handle the source_record_id safely regardless of type
            raw_source_id = row.get('source_record_id', 'unknown')
            if isinstance(raw_source_id, str) and '_' in raw_source_id:
                source_id = raw_source_id.split('_')[0]  # Extract the voter ID part if it's a compound ID
            else:
                source_id = str(raw_source_id)  # Convert to string if it's a number or other type
                
            key = (source_system, source_id)
            
            if key in matches:
                entity_id = matches[key]['entity_id']
                data.at[idx, 'voter_id'] = entity_id
        
        return data
    
    def process_donation_data(self, data: pd.DataFrame, 
                             voters_df: pd.DataFrame,
                             addresses_df: pd.DataFrame,
                             committees_df: pd.DataFrame,
                             candidates_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Process donation data into normalized tables.
        
        Links donors to existing core_voters where possible, adds new voters if necessary,
        links to existing addresses or adds new ones, and maintains unique committees and candidates.
        
        Args:
            data: Donation data DataFrame
            voters_df: Existing core voters DataFrame
            addresses_df: Existing addresses DataFrame
            committees_df: Existing committees DataFrame
            candidates_df: Existing candidates DataFrame
            
        Returns:
            Tuple of (updated_voters, updated_addresses, updated_committees, updated_candidates, donation_history)
        """
        logger.info(f"Processing {len(data)} donation records")
        
        # Create empty DataFrames if none provided
        if voters_df.empty:
            voters_df = pd.DataFrame(columns=[
                'voter_id', 'first_name', 'middle_name', 'last_name', 'suffix', 
                'dob', 'gender', 'email', 'phone_number', 'current_address_id',
                'created_at', 'updated_at', 'confidence_score', 'source_systems'
            ])
        
        if addresses_df.empty:
            addresses_df = pd.DataFrame(columns=[
                'address_id', 'street_number', 'street_name', 'apartment', 'city',
                'state', 'zip', 'zip_plus_4', 'is_mailing_address', 'valid_from',
                'valid_to', 'source_system', 'source_record_id', 'created_at', 'updated_at'
            ])
            
        if committees_df.empty:
            committees_df = pd.DataFrame(columns=[
                'committee_id', 'committee_name', 'committee_type', 'committee_designation',
                'committee_org_type', 'committee_street_1', 'committee_street_2', 'committee_city',
                'committee_state', 'committee_zip', 'committee_treasurer_name', 'committee_email',
                'committee_phone', 'committee_website', 'filing_frequency', 'party_affiliation',
                'organization_type', 'connected_organization_name', 'candidate_id',
                'committee_registration_date', 'committee_termination_date', 'committee_status',
                'is_national_committee', 'has_nonfederal_account', 'last_file_date', 'last_report_year'
            ])
            
        if candidates_df.empty:
            candidates_df = pd.DataFrame(columns=[
                'candidate_id', 'candidate_name', 'candidate_first_name', 'candidate_last_name',
                'candidate_middle_name', 'candidate_prefix', 'candidate_suffix', 'candidate_street_1',
                'candidate_street_2', 'candidate_city', 'candidate_state', 'candidate_zip',
                'candidate_party_affiliation', 'candidate_office', 'candidate_office_state',
                'candidate_office_district', 'candidate_status', 'incumbent_challenger_status',
                'principal_campaign_committee_id', 'election_year', 'load_date',
                'candidate_inactive_date', 'candidate_registration_date',
                'candidate_last_file_date', 'candidate_last_report_year'
            ])
            
        # Process committees (avoid duplicates based on committee_id)
        updated_committees = self._process_committees(data, committees_df)
        
        # Process candidates (avoid duplicates based on candidate_id)
        updated_candidates = self._process_candidates(data, candidates_df)
        
        # Process donor addresses
        donor_addresses, address_map = self._process_donor_addresses(data, addresses_df)
        
        # Process donors and link to core voters
        updated_voters, voter_map = self._process_donors(data, voters_df, address_map)
        
        # Process donations
        donation_history = self._process_donation_history(data, voter_map)
        
        logger.info(f"Processed {len(donation_history)} donation records")
        logger.info(f"Found/created {len(updated_committees.loc[~updated_committees.duplicated('committee_id')])} committees")
        logger.info(f"Found/created {len(updated_candidates.loc[~updated_candidates.duplicated('candidate_id')])} candidates")
        logger.info(f"Found/created {len(donor_addresses.loc[~donor_addresses.duplicated('address_id')])} addresses")
        logger.info(f"Found/created {len(updated_voters.loc[~updated_voters.duplicated('voter_id')])} voters")
        
        return updated_voters, donor_addresses, updated_committees, updated_candidates, donation_history
    
    def _extract_core_voters(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract core voter information from registration data.
        
        Args:
            data: Registration data
            
        Returns:
            Core voter DataFrame
        """
        # Group by voter_id to get one record per voter
        grouped = data.groupby('voter_id').first().reset_index()
        
        # Extract core voter fields
        core_voters = pd.DataFrame({
            'voter_id': grouped['voter_id'],
            'first_name': grouped['_first_name'],
            'middle_name': grouped['_middle_name'],
            'last_name': grouped['_last_name'],
            'suffix': grouped['_suffix'],
            'dob': grouped['_dob'],
            'gender': grouped['_gender'],
            'email': grouped['_email'],
            'phone_number': grouped['_phone_number'],
            'created_at': grouped['created_at'],
            'updated_at': grouped['updated_at'],
            'confidence_score': 1.0,  # Default confidence
            'source_systems': grouped['source_system'].apply(lambda x: [x])
        })
        
        return core_voters
    
    def _extract_addresses(self, data: pd.DataFrame) -> pd.DataFrame:
        """Extract address information from registration data.
        
        Args:
            data: Registration data
            
        Returns:
            Address DataFrame
        """
        # Generate address IDs
        address_ids = [str(uuid.uuid4()) for _ in range(len(data))]
        
        # Extract address fields
        addresses = pd.DataFrame({
            'address_id': address_ids,
            'street_number': data['_street_number'],
            'street_name': data['_street_name'],
            'apartment': data['_apartment'],
            'city': data['_city'],
            'state': data['_state'],
            'zip': data['_zip'],
            'zip_plus_4': data['_zip_plus_4'],
            'is_mailing_address': False,  # Default to residential
            'valid_from': data['registration_date'],
            'valid_to': None,  # Still valid
            'source_system': data['source_system'],
            'source_record_id': data['source_record_id'],
            'created_at': data['created_at'],
            'updated_at': data['updated_at']
        })
        
        return addresses
    
    def _extract_registrations(self, data: pd.DataFrame, addresses: pd.DataFrame) -> pd.DataFrame:
        """Extract registration information and link to addresses.
        
        Args:
            data: Registration data
            addresses: Address DataFrame
            
        Returns:
            Registration DataFrame
        """
        # Create registrations DataFrame
        registrations = pd.DataFrame({
            'registration_id': data['registration_id'],
            'voter_id': data['voter_id'],
            'county': data['county'],
            'registration_date': data['registration_date'],
            'registration_source': data['registration_source'],
            'party': data['party'],
            'voter_status': data['voter_status'],
            'voter_status_reason': data['voter_status_reason'],
            'polling_place_name': data['polling_place_name'],
            'polling_place_address': data['polling_place_address'],
            'address_id': addresses['address_id'],  # Link to address
            'source_system': data['source_system'],
            'source_record_id': data['source_record_id'],
            'created_at': data['created_at'],
            'updated_at': data['updated_at']
        })
        
        return registrations
    
    def save_tables(self, *args) -> None:
        """Save DataFrames to CSV files.
        
        Args:
            *args: Tuple of (dataframe, filename) pairs
        """
        for df, filename in args:
            output_path = os.path.join(self.output_dir, filename)
            logger.info(f"Saving {len(df)} records to {output_path}")
            df.to_csv(output_path, index=False)
            
        # Check database type and save accordingly
        db_type = self.config.get("database.type")
        if db_type == "sqlite":
            self.save_to_sqlite(*args)
        elif db_type == "bigquery":
            self.save_to_bigquery(*args)
            
    def save_to_sqlite(self, *args) -> None:
        """Save DataFrames to SQLite database.
        
        Args:
            *args: Tuple of (dataframe, filename) pairs
        """
        import sqlite3
        
        # Try to get the SQLite database path from different possible config keys
        db_path = self.config.get("database.sqlite_path")
        if not db_path:
            db_path = self.config.get("database.db_path")
            
        if not db_path:
            logger.warning("No SQLite database path specified in config, skipping database save")
            return
            
        logger.info(f"Saving data to SQLite database at {db_path}")
        
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
            
            # Connect to the database
            conn = sqlite3.connect(db_path)
            
            # Save each dataframe
            for df, filename in args:
                # Convert filename to table name (remove .csv and use snake_case)
                table_name = os.path.splitext(filename)[0]
                
                # Save to database, replacing existing table
                logger.info(f"Saving {len(df)} records to SQLite table {table_name}")
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                
            conn.close()
            logger.info("Successfully saved all data to SQLite database")
            
        except Exception as e:
            logger.error(f"Error saving to SQLite database: {e}")
            
    def save_to_bigquery(self, *args) -> None:
        """Save DataFrames to BigQuery.
        
        Args:
            *args: Tuple of (dataframe, filename) pairs
        """
        try:
            # Import BigQuery libraries
            from google.cloud import bigquery
            from google.oauth2 import service_account
            import pandas_gbq
            
            # Get BigQuery configuration
            project_id = self.config.get("database.bigquery.project_id")
            dataset_id = self.config.get("database.bigquery.dataset_id")
            location = self.config.get("database.bigquery.location", "us")
            service_account_path = self.config.get("database.bigquery.service_account_key")
            
            if not project_id or not dataset_id:
                logger.error("Missing required BigQuery configuration (project_id or dataset_id)")
                return
                
            logger.info(f"Saving data to BigQuery dataset {project_id}.{dataset_id}")
            
            # Set up authentication if service account provided
            credentials = None
            if service_account_path:
                try:
                    credentials = service_account.Credentials.from_service_account_file(
                        service_account_path,
                        scopes=["https://www.googleapis.com/auth/cloud-platform"],
                    )
                    logger.info(f"Using service account from {service_account_path}")
                except Exception as e:
                    logger.error(f"Error loading service account credentials: {e}")
                    return
            
            # Create BigQuery client
            client = bigquery.Client(
                project=project_id,
                credentials=credentials,
                location=location
            )
            
            # Make sure dataset exists
            try:
                dataset_ref = client.dataset(dataset_id)
                client.get_dataset(dataset_ref)
            except Exception:
                logger.info(f"Creating dataset {dataset_id}")
                dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
                dataset.location = location
                client.create_dataset(dataset, exists_ok=True)
            
            # Save each dataframe
            for df, filename in args:
                # Convert filename to table name (remove .csv)
                table_name = os.path.splitext(filename)[0]
                full_table_id = f"{project_id}.{dataset_id}.{table_name}"
                
                # Upload dataframe to BigQuery
                logger.info(f"Saving {len(df)} records to BigQuery table {full_table_id}")
                
                # Configure pandas_gbq with credentials if available
                if credentials:
                    pandas_gbq.context.credentials = credentials
                pandas_gbq.context.project = project_id
                
                # Write to BigQuery with schema autodetection
                df.to_gbq(
                    destination_table=f"{dataset_id}.{table_name}",
                    project_id=project_id,
                    if_exists='replace',
                    credentials=credentials
                )
                
            logger.info("Successfully saved all data to BigQuery")
            
        except ImportError:
            logger.error("BigQuery modules not installed. Run: pip install google-cloud-bigquery pandas-gbq")
        except Exception as e:
            logger.error(f"Error saving to BigQuery: {e}")
            
    def update_core_voters_with_current_address(self, core_voters: pd.DataFrame, 
                                              registrations: pd.DataFrame) -> pd.DataFrame:
        """Update core voters with current address IDs.
        
        Args:
            core_voters: Core voter DataFrame
            registrations: Registration DataFrame
            
        Returns:
            Updated core voters DataFrame
        """
        # Get the most recent registration (and thus address) for each voter
        latest_regs = registrations.sort_values('registration_date', ascending=False).groupby('voter_id').first().reset_index()
        
        # Create a mapping of voter_id to address_id
        addr_map = dict(zip(latest_regs['voter_id'], latest_regs['address_id']))
        
        # Update the core_voters DataFrame
        core_voters['current_address_id'] = core_voters['voter_id'].map(addr_map)
        
        return core_voters
    
    def _process_committees(self, data: pd.DataFrame, existing_committees: pd.DataFrame) -> pd.DataFrame:
        """Process and deduplicate committee data.
        
        Args:
            data: Donation data
            existing_committees: Existing committee DataFrame
            
        Returns:
            Updated committee DataFrame
        """
        # Extract unique committees from donation data
        if 'committee_id' not in data.columns or 'committee_name' not in data.columns:
            logger.warning("Committee data not found in donation data")
            return existing_committees
            
        # Get unique committees from donation data
        unique_committees = data[['committee_id', 'committee_name']].drop_duplicates()
        unique_committees = unique_committees[unique_committees['committee_id'].notna()]
        
        if len(unique_committees) == 0:
            logger.info("No valid committee data found in donation records")
            return existing_committees
            
        # If existing committees is empty, create new DataFrame
        if len(existing_committees) == 0:
            # Create new committees DataFrame with minimal info
            new_committees = pd.DataFrame({
                'committee_id': unique_committees['committee_id'],
                'committee_name': unique_committees['committee_name'],
                # Add any additional default values for required fields
                'committee_type': None,
                'committee_designation': None,
                'committee_org_type': None,
                'committee_status': 'ACTIVE',
                'created_at': pd.Timestamp.now(),
                'updated_at': pd.Timestamp.now()
            })
            return new_committees
            
        # Merge with existing committees, keeping existing data where available
        existing_committee_ids = set(existing_committees['committee_id'])
        new_committees_df = pd.DataFrame()
        
        # Filter for only new committees
        new_committee_data = unique_committees[~unique_committees['committee_id'].isin(existing_committee_ids)]
        
        if len(new_committee_data) > 0:
            # Create new committees with minimal info
            new_committees_df = pd.DataFrame({
                'committee_id': new_committee_data['committee_id'],
                'committee_name': new_committee_data['committee_name'],
                # Add any additional default values for required fields
                'committee_type': None,
                'committee_designation': None,
                'committee_org_type': None,
                'committee_status': 'ACTIVE',
                'created_at': pd.Timestamp.now(),
                'updated_at': pd.Timestamp.now()
            })
        
        # Combine existing and new committees
        if len(new_committees_df) > 0:
            updated_committees = pd.concat([existing_committees, new_committees_df], ignore_index=True)
        else:
            updated_committees = existing_committees.copy()
            
        return updated_committees
        
    def _process_candidates(self, data: pd.DataFrame, existing_candidates: pd.DataFrame) -> pd.DataFrame:
        """Process and deduplicate candidate data.
        
        Args:
            data: Donation data
            existing_candidates: Existing candidate DataFrame
            
        Returns:
            Updated candidate DataFrame
        """
        # Some donation files may not have candidate IDs or may have them in the 'candidate_id' column
        if 'candidate_id' not in data.columns or data['candidate_id'].isna().all():
            logger.info("No valid candidate data found in donation records")
            return existing_candidates
            
        # Extract candidate information from donation data
        candidate_cols = [col for col in data.columns if col.startswith('candidate_')]
        if not candidate_cols:
            logger.warning("Candidate data fields not found in donation data")
            return existing_candidates
            
        # Get unique candidates from donation data
        candidate_fields = ['candidate_id', 'candidate_name', 'candidate_first_name', 
                           'candidate_last_name', 'candidate_middle_name', 'candidate_prefix', 
                           'candidate_suffix', 'candidate_office', 'candidate_office_state', 
                           'candidate_office_district']
        
        # Select only columns that exist in the donation data
        available_fields = [f for f in candidate_fields if f in data.columns]
        
        if not available_fields:
            logger.warning("No candidate fields found in donation data")
            return existing_candidates
            
        unique_candidates = data[available_fields].drop_duplicates()
        unique_candidates = unique_candidates[unique_candidates['candidate_id'].notna()]
        
        if len(unique_candidates) == 0:
            logger.info("No valid candidate data found in donation records")
            return existing_candidates
            
        # If existing candidates is empty, create new DataFrame
        if len(existing_candidates) == 0:
            # Create new candidates DataFrame with available info
            new_candidates = pd.DataFrame()
            for field in candidate_fields:
                if field in unique_candidates.columns:
                    new_candidates[field] = unique_candidates[field]
                else:
                    new_candidates[field] = None
                    
            # Add timestamps
            new_candidates['created_at'] = pd.Timestamp.now()
            new_candidates['updated_at'] = pd.Timestamp.now()
            
            return new_candidates
            
        # Merge with existing candidates, keeping existing data where available
        existing_candidate_ids = set(existing_candidates['candidate_id'])
        new_candidates_df = pd.DataFrame()
        
        # Filter for only new candidates
        new_candidate_data = unique_candidates[~unique_candidates['candidate_id'].isin(existing_candidate_ids)]
        
        if len(new_candidate_data) > 0:
            # Create new candidates with available info
            new_candidates_df = pd.DataFrame()
            for field in candidate_fields:
                if field in new_candidate_data.columns:
                    new_candidates_df[field] = new_candidate_data[field]
                else:
                    new_candidates_df[field] = None
                    
            # Add timestamps
            new_candidates_df['created_at'] = pd.Timestamp.now()
            new_candidates_df['updated_at'] = pd.Timestamp.now()
        
        # Combine existing and new candidates
        if len(new_candidates_df) > 0:
            updated_candidates = pd.concat([existing_candidates, new_candidates_df], ignore_index=True)
        else:
            updated_candidates = existing_candidates.copy()
            
        return updated_candidates 
    
    def _process_donor_addresses(self, data: pd.DataFrame, existing_addresses: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[int, str]]:
        """Process donor addresses, linking to existing addresses where possible.
        
        Args:
            data: Donation data
            existing_addresses: Existing address DataFrame
            
        Returns:
            Tuple of (updated addresses DataFrame, mapping of donation row index to address_id)
        """
        # Extract address information from donation data
        address_fields = ['contributor_street_1', 'contributor_street_2', 'contributor_city', 
                         'contributor_state', 'contributor_zip']
        
        # Check if required address fields exist
        if not all(field in data.columns for field in ['contributor_street_1', 'contributor_city', 'contributor_state']):
            logger.warning("Required address fields not found in donation data")
            return existing_addresses, {}
            
        # Create a copy to avoid modifying the original
        donation_data = data.copy()
        
        # Generate address IDs for matching
        address_map = {}  # Maps donation row index to address_id
        new_addresses = []
        now = pd.Timestamp.now()
        
        # Helper function to normalize addresses for comparison
        def normalize_address(street1, street2, city, state, zip_code):
            street1 = str(street1).upper() if pd.notna(street1) else ""
            street2 = str(street2).upper() if pd.notna(street2) else ""
            city = str(city).upper() if pd.notna(city) else ""
            state = str(state).upper() if pd.notna(state) else ""
            zip_code = str(zip_code).split('-')[0] if pd.notna(zip_code) else ""  # Just the base ZIP
            return f"{street1}|{street2}|{city}|{state}|{zip_code}"
            
        # Create a lookup of existing addresses
        existing_address_lookup = {}
        if not existing_addresses.empty:
            for _, addr in existing_addresses.iterrows():
                street_num = str(addr.get('street_number', '')) if pd.notna(addr.get('street_number', '')) else ""
                street_name = str(addr.get('street_name', '')) if pd.notna(addr.get('street_name', '')) else ""
                street1 = f"{street_num} {street_name}".strip()
                
                key = normalize_address(
                    street1,
                    addr.get('apartment', ''),
                    addr.get('city', ''),
                    addr.get('state', ''),
                    addr.get('zip', '')
                )
                existing_address_lookup[key] = addr.get('address_id')
        
        # Process each donation record
        for idx, row in donation_data.iterrows():
            # Skip if required fields are missing
            if pd.isna(row.get('contributor_street_1')) or pd.isna(row.get('contributor_city')) or pd.isna(row.get('contributor_state')):
                continue
                
            # Create normalized address key
            address_key = normalize_address(
                row.get('contributor_street_1', ''),
                row.get('contributor_street_2', ''),
                row.get('contributor_city', ''),
                row.get('contributor_state', ''),
                row.get('contributor_zip', '')
            )
            
            # Check if address exists
            if address_key in existing_address_lookup:
                address_map[idx] = existing_address_lookup[address_key]
            else:
                # Create new address
                address_id = str(uuid.uuid4())
                address_map[idx] = address_id
                existing_address_lookup[address_key] = address_id
                
                # Parse street number and name from street1
                street_parts = str(row.get('contributor_street_1', '')).split(' ', 1)
                street_number = street_parts[0] if len(street_parts) > 0 else None
                street_name = street_parts[1] if len(street_parts) > 1 else None
                
                # Add to new addresses list
                new_addresses.append({
                    'address_id': address_id,
                    'street_number': street_number,
                    'street_name': street_name,
                    'apartment': row.get('contributor_street_2', None),
                    'city': row.get('contributor_city', None),
                    'state': row.get('contributor_state', None),
                    'zip': str(row.get('contributor_zip', None)).split('-')[0] if pd.notna(row.get('contributor_zip', None)) else None,
                    'zip_plus_4': str(row.get('contributor_zip', '')).split('-')[1] if pd.notna(row.get('contributor_zip', '')) and '-' in str(row.get('contributor_zip', '')) else None,
                    'is_mailing_address': True,  # Assume donor address is mailing address
                    'valid_from': self._safe_parse_date(row.get('contribution_receipt_date')) or now.date(),
                    'valid_to': None,  # Still valid
                    'source_system': 'fec',
                    'source_record_id': row.get('transaction_id', None),
                    'created_at': now,
                    'updated_at': now
                })
        
        # Create DataFrame from new addresses
        if new_addresses:
            new_addresses_df = pd.DataFrame(new_addresses)
            # Combine with existing addresses
            updated_addresses = pd.concat([existing_addresses, new_addresses_df], ignore_index=True)
        else:
            updated_addresses = existing_addresses.copy()
            
        return updated_addresses, address_map
        
    def _process_donors(self, data: pd.DataFrame, existing_voters: pd.DataFrame, address_map: Dict[int, str]) -> Tuple[pd.DataFrame, Dict[int, str]]:
        """Process donors, linking to existing voters where possible.
        
        Args:
            data: Donation data
            existing_voters: Existing core voters DataFrame
            address_map: Mapping of donation row index to address_id
            
        Returns:
            Tuple of (updated voters DataFrame, mapping of donation row index to voter_id)
        """
        # Check if required donor fields exist
        donor_fields = ['contributor_first_name', 'contributor_last_name']
        if not all(field in data.columns for field in donor_fields):
            logger.warning("Required donor fields not found in donation data")
            return existing_voters, {}
            
        # Create a copy to avoid modifying the original
        donation_data = data.copy()
        
        # Create a lookup of existing voters by name
        name_voter_lookup = {}
        if not existing_voters.empty:
            for _, voter in existing_voters.iterrows():
                first = str(voter.get('first_name', '')).upper() if pd.notna(voter.get('first_name', '')) else ""
                middle = str(voter.get('middle_name', '')).upper() if pd.notna(voter.get('middle_name', '')) else ""
                last = str(voter.get('last_name', '')).upper() if pd.notna(voter.get('last_name', '')) else ""
                suffix = str(voter.get('suffix', '')).upper() if pd.notna(voter.get('suffix', '')) else ""
                
                # Create name keys with variations
                key1 = f"{first}|{last}"  # First + Last
                key2 = f"{first}|{middle}|{last}"  # First + Middle + Last
                key3 = f"{first}|{last}|{suffix}"  # First + Last + Suffix
                key4 = f"{first}|{middle}|{last}|{suffix}"  # All components
                
                for key in [key4, key3, key2, key1]:
                    if key not in name_voter_lookup:
                        name_voter_lookup[key] = voter.get('voter_id')
        
        # Maps donation row index to voter_id
        voter_map = {}
        new_voters = []
        now = pd.Timestamp.now()
        
        # Process each donation record
        for idx, row in donation_data.iterrows():
            # Skip if name fields are missing
            if pd.isna(row.get('contributor_first_name')) or pd.isna(row.get('contributor_last_name')):
                continue
                
            # Normalize name fields
            first = str(row.get('contributor_first_name', '')).upper() if pd.notna(row.get('contributor_first_name', '')) else ""
            middle = str(row.get('contributor_middle_name', '')).upper() if pd.notna(row.get('contributor_middle_name', '')) else ""
            last = str(row.get('contributor_last_name', '')).upper() if pd.notna(row.get('contributor_last_name', '')) else ""
            suffix = str(row.get('contributor_suffix', '')).upper() if pd.notna(row.get('contributor_suffix', '')) else ""
            
            # Create name keys with variations
            key1 = f"{first}|{last}"  # First + Last
            key2 = f"{first}|{middle}|{last}"  # First + Middle + Last
            key3 = f"{first}|{last}|{suffix}"  # First + Last + Suffix
            key4 = f"{first}|{middle}|{last}|{suffix}"  # All components
            
            # Try to find matching voter
            voter_id = None
            for key in [key4, key3, key2, key1]:
                if key in name_voter_lookup:
                    voter_id = name_voter_lookup[key]
                    break
                    
            # If no match found, create new voter
            if voter_id is None:
                voter_id = str(uuid.uuid4())
                
                # Get address_id from map
                address_id = address_map.get(idx)
                
                # Add to new voters list
                new_voters.append({
                    'voter_id': voter_id,
                    'first_name': row.get('contributor_first_name'),
                    'middle_name': row.get('contributor_middle_name'),
                    'last_name': row.get('contributor_last_name'),
                    'suffix': row.get('contributor_suffix'),
                    'dob': None,  # Not available in donation data
                    'gender': None,  # Not available in donation data
                    'email': None,  # Not available in donation data
                    'phone_number': None,  # Not available in donation data
                    'current_address_id': address_id,
                    'created_at': now,
                    'updated_at': now,
                    'confidence_score': 0.7,  # Lower confidence for donors
                    'source_systems': ['fec']
                })
                
                # Add to lookup
                for key in [key4, key3, key2, key1]:
                    if key and key not in name_voter_lookup:
                        name_voter_lookup[key] = voter_id
            
            # Map donation index to voter_id
            voter_map[idx] = voter_id
        
        # Create DataFrame from new voters
        if new_voters:
            # Convert source_systems to string for each voter (required for pandas)
            for voter in new_voters:
                voter['source_systems'] = json.dumps(voter['source_systems'])
                
            new_voters_df = pd.DataFrame(new_voters)
            
            # Convert existing voters source_systems to string if needed
            if not existing_voters.empty and 'source_systems' in existing_voters.columns:
                existing_voters['source_systems'] = existing_voters['source_systems'].apply(
                    lambda x: json.dumps(x) if isinstance(x, list) else 
                            (x if isinstance(x, str) else json.dumps(['unknown']))
                )
            
            # Combine with existing voters
            updated_voters = pd.concat([existing_voters, new_voters_df], ignore_index=True)
        else:
            updated_voters = existing_voters.copy()
            
        return updated_voters, voter_map 
    
    def _process_donation_history(self, data: pd.DataFrame, voter_map: Dict[int, str]) -> pd.DataFrame:
        """Process donation history, linking with voters, committees and candidates.
        
        Args:
            data: Donation data
            voter_map: Mapping of donation row index to voter_id
            
        Returns:
            Donation history DataFrame
        """
        # Create a copy to avoid modifying the original
        donation_data = data.copy()
        
        # Check if required donation fields exist
        required_fields = ['transaction_id', 'committee_id', 'contribution_receipt_date', 'contribution_receipt_amount']
        if not all(field in donation_data.columns for field in required_fields):
            logger.warning("Required donation fields not found in donation data")
            return pd.DataFrame()
            
        # Create donation history records
        donation_records = []
        now = pd.Timestamp.now()
        
        # Process each donation record
        for idx, row in donation_data.iterrows():
            # Get voter_id from map
            voter_id = voter_map.get(idx)
            
            # Skip if no voter_id (couldn't match or create donor)
            if voter_id is None:
                continue
                
            # Create donation record
            donation = {
                'transaction_id': row.get('transaction_id'),
                'voter_id': voter_id,
                'committee_id': row.get('committee_id'),
                'report_year': row.get('report_year'),
                'report_type': row.get('report_type'),
                'image_number': row.get('image_number'),
                'line_number': row.get('line_number'),
                'file_number': row.get('file_number'),
                'entity_type': row.get('entity_type'),
                'entity_type_desc': row.get('entity_type_desc'),
                'contributor_id': row.get('contributor_id'),
                'contributor_prefix': row.get('contributor_prefix'),
                'contributor_first_name': row.get('contributor_first_name'),
                'contributor_middle_name': row.get('contributor_middle_name'),
                'contributor_last_name': row.get('contributor_last_name'),
                'contributor_suffix': row.get('contributor_suffix'),
                'contributor_street_1': row.get('contributor_street_1'),
                'contributor_street_2': row.get('contributor_street_2'),
                'contributor_city': row.get('contributor_city'),
                'contributor_state': row.get('contributor_state'),
                'contributor_zip': row.get('contributor_zip'),
                'contributor_employer': row.get('contributor_employer'),
                'contributor_occupation': row.get('contributor_occupation'),
                'receipt_type': row.get('receipt_type'),
                'receipt_type_desc': row.get('receipt_type_desc'),
                'memo_code': row.get('memo_code'),
                'memo_text': row.get('memo_text'),
                'contribution_receipt_date': self._safe_parse_date(row.get('contribution_receipt_date')),
                'contribution_receipt_amount': row.get('contribution_receipt_amount'),
                'contributor_aggregate_ytd': row.get('contributor_aggregate_ytd'),
                'candidate_id': row.get('candidate_id'),
                'election_type': row.get('election_type'),
                'election_type_full': row.get('election_type_full'),
                'fec_election_type_desc': row.get('fec_election_type_desc'),
                'fec_election_year': row.get('fec_election_year'),
                'amendment_indicator': row.get('amendment_indicator'),
                'amendment_indicator_desc': row.get('amendment_indicator_desc'),
                'schedule_type': row.get('schedule_type'),
                'schedule_type_full': row.get('schedule_type_full'),
                'load_date': self._safe_parse_date(row.get('load_date')),
                'original_sub_id': row.get('original_sub_id'),
                'back_reference_transaction_id': row.get('back_reference_transaction_id'),
                'back_reference_schedule_name': row.get('back_reference_schedule_name'),
                'filing_form': row.get('filing_form'),
                'sub_id': row.get('sub_id'),
                'pdf_url': row.get('pdf_url'),
                'line_number_label': row.get('line_number_label'),
                'is_individual': row.get('is_individual') == 't' or row.get('entity_type') == 'IND',
                'increased_limit': row.get('increased_limit') == 't',
                'two_year_transaction_period': row.get('two_year_transaction_period'),
                'source_system': 'fec',
                'source_record_id': row.get('transaction_id'),
                'created_at': now,
                'updated_at': now
            }
            
            # Add to donation records
            donation_records.append(donation)
        
        # Create DataFrame from donation records
        if donation_records:
            donations_df = pd.DataFrame(donation_records)
        else:
            # Create empty DataFrame with proper columns
            donations_df = pd.DataFrame(columns=[
                'transaction_id', 'voter_id', 'committee_id', 'report_year', 'report_type',
                'image_number', 'line_number', 'file_number', 'entity_type', 'entity_type_desc',
                'contributor_id', 'contributor_prefix', 'contributor_first_name', 
                'contributor_middle_name', 'contributor_last_name', 'contributor_suffix',
                'contributor_street_1', 'contributor_street_2', 'contributor_city',
                'contributor_state', 'contributor_zip', 'contributor_employer',
                'contributor_occupation', 'receipt_type', 'receipt_type_desc',
                'memo_code', 'memo_text', 'contribution_receipt_date', 'contribution_receipt_amount',
                'contributor_aggregate_ytd', 'candidate_id', 'election_type',
                'election_type_full', 'fec_election_type_desc', 'fec_election_year',
                'amendment_indicator', 'amendment_indicator_desc', 'schedule_type',
                'schedule_type_full', 'load_date', 'original_sub_id',
                'back_reference_transaction_id', 'back_reference_schedule_name',
                'filing_form', 'sub_id', 'pdf_url', 'line_number_label',
                'is_individual', 'increased_limit', 'two_year_transaction_period',
                'source_system', 'source_record_id', 'created_at', 'updated_at'
            ])
            
        return donations_df 

    def _safe_parse_date(self, date_str):
        """Safely parse a date string.
        
        Args:
            date_str: Date string to parse
            
        Returns:
            Parsed date or None if parsing fails
        """
        if pd.notna(date_str):
            try:
                return pd.to_datetime(date_str).date()
            except ValueError:
                logger.warning(f"Error parsing date: {date_str}")
        return None 