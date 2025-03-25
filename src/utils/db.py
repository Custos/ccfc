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
        self.db_path = config.get("database.sqlite_path", "data/ccfc.db")
        
        if self.db_type == "bigquery":
            try:
                from google.cloud import bigquery
                self.client = bigquery.Client()
                self.dataset_id = config.get("database.bigquery.dataset_id", "ccfc")
            except ImportError:
                logger.warning("BigQuery dependencies not installed. Falling back to SQLite.")
                self.db_type = "sqlite"
        else:
            # Ensure SQLite database directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
    
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
                    last_name TEXT,
                    dob DATE,
                    gender TEXT,
                    current_address_id TEXT,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
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
                    donation_id TEXT PRIMARY KEY,
                    voter_id TEXT,
                    committee_id TEXT,
                    amount DECIMAL(10,2),
                    donation_date DATE,
                    source_system TEXT,
                    source_record_id TEXT,
                    created_at TIMESTAMP,
                    FOREIGN KEY (voter_id) REFERENCES core_voters(voter_id)
                )
            """)
            logger.info("donation_history table created")
            
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
            date_columns = ['created_at', 'updated_at', 'dob', 'registration_date', 'election_date', 'donation_date']
            for col in date_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Convert integer boolean columns back to boolean
            bool_columns = ['is_current', 'voted']
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