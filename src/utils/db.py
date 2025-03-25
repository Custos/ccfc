"""Database utilities for the CCFC Voter Data Platform."""

import os
import logging
from typing import Optional, Dict, Any
import pandas as pd
import sqlite3
from pathlib import Path

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database manager for handling both SQLite and BigQuery connections."""
    
    def __init__(self, config):
        """Initialize the database manager.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.db_type = config.get("database.type", "sqlite")
        
        # Support both db_path and sqlite_path for backward compatibility
        self.db_path = config.get("database.db_path")
        if not self.db_path:
            self.db_path = config.get("database.sqlite_path", "data/ccfc.db")
        
        logger.info(f"Initializing database with type {self.db_type} at path {self.db_path}")
        
        if self.db_type == "bigquery":
            try:
                from google.cloud import bigquery
                self.client = bigquery.Client()
                self.dataset_id = config.get("database.bigquery.dataset_id", "ccfc")
                self.project_id = config.get("database.bigquery.project_id", "caitfc")
                self.location = config.get("database.bigquery.location", "us")
                logger.info(f"Connected to BigQuery project: {self.project_id}, dataset: {self.dataset_id}")
            except ImportError:
                logger.warning("BigQuery dependencies not installed. Falling back to SQLite.")
                self.db_type = "sqlite"
        
        if self.db_type == "sqlite":
            # Ensure SQLite database directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Connected to SQLite database at {self.db_path}")
        
        # Create tables if they don't exist
        self.create_tables()
    
    def create_tables(self):
        """Create the necessary database tables."""
        if self.db_type == "sqlite":
            self._create_sqlite_tables()
        else:
            self._create_bigquery_tables()
    
    def _create_sqlite_tables(self):
        """Create SQLite tables."""
        logger.info("Starting SQLite table creation...")
        cursor = self.conn.cursor()
        
        try:
            # Create core_voters table
            logger.info("Creating core_voters table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS core_voters (
                    voter_id TEXT PRIMARY KEY,
                    first_name TEXT,
                    middle_name TEXT,
                    last_name TEXT,
                    suffix TEXT,
                    dob DATE,
                    gender TEXT,
                    email TEXT,
                    phone_number TEXT,
                    current_address_id TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    confidence_score FLOAT,
                    source_systems TEXT
                )
            """)
            logger.info("core_voters table created")
            
            # Create addresses table
            logger.info("Creating addresses table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS addresses (
                    address_id TEXT PRIMARY KEY,
                    voter_id TEXT,
                    street_address TEXT,
                    city TEXT,
                    state TEXT,
                    zip_code TEXT,
                    county TEXT,
                    is_current BOOLEAN,
                    created_at TIMESTAMP,
                    source_system TEXT,
                    FOREIGN KEY (voter_id) REFERENCES core_voters(voter_id)
                )
            """)
            logger.info("addresses table created")
            
            # Create voter_registrations table
            logger.info("Creating voter_registrations table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS voter_registrations (
                    registration_id TEXT PRIMARY KEY,
                    voter_id TEXT,
                    registration_date DATE,
                    party TEXT,
                    county TEXT,
                    source_system TEXT,
                    source_record_id TEXT,
                    created_at TIMESTAMP,
                    FOREIGN KEY (voter_id) REFERENCES core_voters(voter_id)
                )
            """)
            logger.info("voter_registrations table created")
            
            # Create election_history table
            logger.info("Creating election_history table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS election_history (
                    election_id TEXT PRIMARY KEY,
                    voter_id TEXT,
                    election_date DATE,
                    election_type TEXT,
                    voted BOOLEAN,
                    party TEXT,
                    county TEXT,
                    source_system TEXT,
                    source_record_id TEXT,
                    created_at TIMESTAMP,
                    FOREIGN KEY (voter_id) REFERENCES core_voters(voter_id)
                )
            """)
            logger.info("election_history table created")
            
            # Create donation_history table
            logger.info("Creating donation_history table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS donation_history (
                    transaction_id TEXT PRIMARY KEY,
                    voter_id TEXT,
                    committee_id TEXT,
                    report_year INTEGER,
                    report_type TEXT,
                    image_number TEXT,
                    line_number TEXT,
                    file_number INTEGER,
                    entity_type TEXT,
                    entity_type_desc TEXT,
                    contributor_id TEXT,
                    contributor_prefix TEXT,
                    contributor_first_name TEXT,
                    contributor_middle_name TEXT,
                    contributor_last_name TEXT,
                    contributor_suffix TEXT,
                    contributor_street_1 TEXT,
                    contributor_street_2 TEXT,
                    contributor_city TEXT,
                    contributor_state TEXT,
                    contributor_zip TEXT,
                    contributor_employer TEXT,
                    contributor_occupation TEXT,
                    receipt_type TEXT,
                    receipt_type_desc TEXT,
                    memo_code TEXT,
                    memo_text TEXT,
                    contribution_receipt_date DATE,
                    contribution_receipt_amount DECIMAL(10,2),
                    contributor_aggregate_ytd DECIMAL(10,2),
                    candidate_id TEXT,
                    election_type TEXT,
                    election_type_full TEXT,
                    fec_election_type_desc TEXT,
                    fec_election_year INTEGER,
                    amendment_indicator TEXT,
                    amendment_indicator_desc TEXT,
                    schedule_type TEXT,
                    schedule_type_full TEXT,
                    load_date DATE,
                    original_sub_id TEXT,
                    back_reference_transaction_id TEXT,
                    back_reference_schedule_name TEXT,
                    filing_form TEXT,
                    sub_id TEXT,
                    pdf_url TEXT,
                    line_number_label TEXT,
                    is_individual BOOLEAN,
                    increased_limit BOOLEAN,
                    two_year_transaction_period INTEGER,
                    source_system TEXT,
                    source_record_id TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    FOREIGN KEY (voter_id) REFERENCES core_voters(voter_id)
                )
            """)
            logger.info("donation_history table created")
            
            # Create committees table
            logger.info("Creating committees table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS committees (
                    committee_id TEXT PRIMARY KEY,
                    committee_name TEXT,
                    committee_type TEXT,
                    committee_designation TEXT,
                    committee_org_type TEXT,
                    committee_street_1 TEXT,
                    committee_street_2 TEXT,
                    committee_city TEXT,
                    committee_state TEXT,
                    committee_zip TEXT,
                    committee_treasurer_name TEXT,
                    committee_email TEXT,
                    committee_phone TEXT,
                    committee_website TEXT,
                    filing_frequency TEXT,
                    party_affiliation TEXT,
                    organization_type TEXT,
                    connected_organization_name TEXT,
                    candidate_id TEXT,
                    committee_registration_date DATE,
                    committee_termination_date DATE,
                    committee_status TEXT,
                    is_national_committee BOOLEAN,
                    has_nonfederal_account BOOLEAN,
                    last_file_date DATE,
                    last_report_year INTEGER,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)
            logger.info("committees table created")
            
            # Create candidates table
            logger.info("Creating candidates table...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS candidates (
                    candidate_id TEXT PRIMARY KEY,
                    candidate_name TEXT,
                    candidate_first_name TEXT,
                    candidate_last_name TEXT,
                    candidate_middle_name TEXT,
                    candidate_prefix TEXT,
                    candidate_suffix TEXT,
                    candidate_street_1 TEXT,
                    candidate_street_2 TEXT,
                    candidate_city TEXT,
                    candidate_state TEXT,
                    candidate_zip TEXT,
                    candidate_party_affiliation TEXT,
                    candidate_office TEXT,
                    candidate_office_state TEXT,
                    candidate_office_district TEXT,
                    candidate_status TEXT,
                    incumbent_challenger_status TEXT,
                    principal_campaign_committee_id TEXT,
                    election_year INTEGER,
                    load_date DATE,
                    candidate_inactive_date DATE,
                    candidate_registration_date DATE,
                    candidate_last_file_date DATE,
                    candidate_last_report_year INTEGER,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)
            logger.info("candidates table created")
            
            # Commit the changes
            self.conn.commit()
            logger.info("Successfully created all database tables")
            
            # Verify tables were created
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            logger.info("Created tables:")
            for table in tables:
                logger.info(f"- {table[0]}")
            
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            self.conn.rollback()
            raise
        finally:
            cursor.close()
    
    def _create_bigquery_tables(self):
        """Create BigQuery tables."""
        # Implementation for BigQuery table creation
        pass
    
    def save_dataframe(self, df: pd.DataFrame, table_name: str, 
                      if_exists: str = 'append') -> None:
        """Save a DataFrame to the database.
        
        Args:
            df: DataFrame to save
            table_name: Name of the table to save to
            if_exists: How to handle existing data ('append' or 'replace')
        """
        if self.db_type == "sqlite":
            logger.info(f"Saving DataFrame to table {table_name}...")
            
            # Convert datetime columns to string format for SQLite
            for col in df.select_dtypes(include=['datetime64[ns]']).columns:
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            
            # Convert boolean columns to integer for SQLite
            for col in df.select_dtypes(include=['bool']).columns:
                df[col] = df[col].astype(int)
            
            # Save to SQLite
            df.to_sql(table_name, self.conn, if_exists=if_exists, index=False)
            self.conn.commit()  # Ensure changes are committed
            logger.info(f"Successfully saved {len(df)} rows to {table_name}")
        else:
            # Implementation for BigQuery
            pass
    
    def read_table(self, table_name: str) -> pd.DataFrame:
        """Read a table from the database.
        
        Args:
            table_name: Name of the table to read
            
        Returns:
            DataFrame containing the table data
        """
        if self.db_type == "sqlite":
            df = pd.read_sql(f"SELECT * FROM {table_name}", self.conn)
            
            # Convert string dates back to datetime
            date_columns = ['created_at', 'updated_at', 'dob', 'registration_date', 'election_date', 'contribution_receipt_date']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Convert integer boolean columns back to boolean
            bool_columns = ['is_current', 'voted', 'is_individual', 'increased_limit', 'is_national_committee', 'has_nonfederal_account']
            for col in bool_columns:
                if col in df.columns:
                    df[col] = df[col].astype(bool)
            
            return df
        else:
            # Implementation for BigQuery
            pass
    
    def close(self):
        """Close the database connection."""
        if self.db_type == "sqlite":
            self.conn.close()
        else:
            # Implementation for BigQuery
            pass 