"""Test script for database functionality."""

import logging
import sqlite3
import os
from src.utils.config import Config
from src.utils.db import DatabaseManager

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_sqlite_tables(db_path):
    """Verify tables exist in SQLite database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get list of tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    logger.info("\nTables in database:")
    for table in tables:
        logger.info(f"- {table[0]}")
        # Show table schema
        cursor.execute(f"PRAGMA table_info({table[0]});")
        columns = cursor.fetchall()
        logger.info("  Columns:")
        for col in columns:
            logger.info(f"    {col[1]} ({col[2]})")
    
    conn.close()

def test_database():
    """Test database functionality."""
    # Load configuration
    config = Config()
    
    # Initialize database manager
    logger.info("Initializing database manager...")
    db = DatabaseManager(config)
    logger.info(f"Database path: {db.db_path}")
    
    # Ensure database directory exists
    os.makedirs(os.path.dirname(db.db_path), exist_ok=True)
    
    try:
        # Create tables
        logger.info("Creating database tables...")
        db.create_tables()
        
        # Verify tables were created
        logger.info("Verifying tables...")
        verify_sqlite_tables(db.db_path)
        
        # Test data
        import pandas as pd
        from datetime import datetime
        
        # Create test core voter
        logger.info("Creating test data...")
        core_voter = pd.DataFrame([{
            'voter_id': 'test_voter_1',
            'first_name': 'John',
            'last_name': 'Doe',
            'dob': datetime(1980, 1, 1),
            'gender': 'M',
            'current_address_id': 'test_addr_1',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }])
        
        # Create test address
        address = pd.DataFrame([{
            'address_id': 'test_addr_1',
            'voter_id': 'test_voter_1',
            'street_address': '123 Main St',
            'city': 'New York',
            'state': 'NY',
            'zip_code': '10001',
            'county': 'New York',
            'is_current': True,
            'created_at': datetime.now()
        }])
        
        # Save test data
        logger.info("\nSaving test data...")
        db.save_dataframe(core_voter, 'core_voters')
        db.save_dataframe(address, 'addresses')
        
        # Read back data
        logger.info("\nReading back test data...")
        core_voters = db.read_table('core_voters')
        addresses = db.read_table('addresses')
        
        # Print results
        logger.info("\nCore Voters:")
        print(core_voters)
        logger.info("\nAddresses:")
        print(addresses)
        
        # Verify data in SQLite directly
        logger.info("\nVerifying data in SQLite directly...")
        conn = sqlite3.connect(db.db_path)
        cursor = conn.cursor()
        
        # First verify tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        logger.info("Existing tables:")
        for table in tables:
            logger.info(f"- {table[0]}")
        
        cursor.execute("SELECT * FROM core_voters;")
        logger.info("Core Voters in SQLite:")
        for row in cursor.fetchall():
            logger.info(row)
            
        cursor.execute("SELECT * FROM addresses;")
        logger.info("Addresses in SQLite:")
        for row in cursor.fetchall():
            logger.info(row)
            
        conn.close()
        
        logger.info("\nDatabase test completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during database test: {e}")
        raise
    finally:
        # Close database connection
        logger.info("Closing database connection...")
        db.close()

if __name__ == "__main__":
    test_database() 