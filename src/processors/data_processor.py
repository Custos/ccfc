"""Data processor for normalized entity tables."""

import pandas as pd
import numpy as np
import logging
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
        
        db_path = self.config.get("database.sqlite_path")
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