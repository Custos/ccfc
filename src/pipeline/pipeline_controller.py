"""Pipeline controller for the CCFC Voter Data Platform."""

import pandas as pd
import logging
import importlib
import time
from datetime import datetime
import multiprocessing as mp
from typing import Dict, Any, List, Optional, Tuple
import os
from tqdm import tqdm  # Add tqdm for progress indicators

from ..adapters import BaseAdapter
from ..matchers import EntityMatcher
from ..processors import DataProcessor
from ..utils import Config

logger = logging.getLogger(__name__)

class PipelineController:
    """Controls the data processing pipeline."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize the pipeline controller.
        
        Args:
            config_path: Path to the configuration file
        """
        # Set up configuration
        self.config = Config(config_path)
        logger.info("Initialized pipeline controller")
        
        # Initialize components
        self.matcher = EntityMatcher(self.config)
        self.processor = DataProcessor(self.config)
        
        # Set up data storage
        self.registration_data = []
        self.election_history_data = []
        self.entity_matches = {}
    
    def _get_adapter(self, source_name: str) -> BaseAdapter:
        """Get an adapter for a source.
        
        Args:
            source_name: Name of the source
            
        Returns:
            Adapter instance
        """
        source_config = self.config.get_source_config(source_name)
        if not source_config:
            raise ValueError(f"Source {source_name} not found in configuration")
        
        adapter_name = source_config.get("adapter")
        if not adapter_name:
            raise ValueError(f"No adapter specified for source {source_name}")
        
        # Import the adapter module
        try:
            module = importlib.import_module("..adapters.voter_registration_adapter", __package__)
            adapter_class = getattr(module, adapter_name)
            return adapter_class(self.config, source_name)
        except (ImportError, AttributeError) as e:
            logger.error(f"Error loading adapter {adapter_name}: {e}")
            raise ValueError(f"Could not load adapter {adapter_name}")
    
    def process_voter_registrations(self, source_names: List[str]) -> None:
        """Process voter registration data from multiple sources.
        
        Args:
            source_names: List of source names to process
        """
        logger.info(f"Processing voter registration data from {len(source_names)} sources")
        start_time = time.time()
        
        # Process each source
        for source_name in tqdm(source_names, desc="Processing sources"):
            source_start = time.time()
            logger.info(f"Processing source: {source_name}")
            
            # Get the adapter for this source
            adapter = self._get_adapter(source_name)
            
            # Process the source data
            data = adapter.process_all_files()
            
            # Extract election history if the source has it
            election_history = None
            source_config = self.config.get_source_config(source_name)
            if source_config.get("has_election_history", False):
                logger.info(f"Extracting election history for {source_name}")
                
                # For election history, we need to read the raw data files directly
                files = adapter.find_source_files()
                if files:
                    logger.info(f"Reading raw data from {len(files)} files for election history extraction")
                    
                    # Get the chunk size from config
                    chunk_size = self.config.get("processing.chunk_size", 10000)
                    
                    # Read raw data in chunks with progress monitoring
                    chunks = []
                    for f in tqdm(files, desc=f"Reading {source_name} files"):
                        file_chunks = pd.read_csv(f, dtype=str, chunksize=chunk_size)
                        for i, chunk in enumerate(tqdm(file_chunks, 
                                                      desc=f"Reading chunks from {os.path.basename(f)}")):
                            chunks.append(chunk)
                    
                    raw_data = pd.concat(chunks)
                    
                    # Extract election history using adapter method
                    if hasattr(adapter, "extract_election_history"):
                        logger.info(f"Extracting election history from raw data for {source_name}")
                        extraction_start = time.time()
                        election_history = adapter.extract_election_history(raw_data)
                        extraction_time = time.time() - extraction_start
                        logger.info(f"Extracted {len(election_history)} election history records in {extraction_time:.2f} seconds")
            
            # Add to the collected data
            if not data.empty:
                self.registration_data.append(data)
                logger.info(f"Added {len(data)} records from {source_name}")
            
            if election_history is not None and not election_history.empty:
                self.election_history_data.append(election_history)
                logger.info(f"Added {len(election_history)} election history records from {source_name}")
            
            source_time = time.time() - source_start
            logger.info(f"Completed processing {source_name} in {source_time:.2f} seconds")
        
        # Combine all data with progress indicator
        if self.registration_data:
            logger.info(f"Combining registration data from {len(self.registration_data)} sources")
            self.combined_registration_data = pd.concat(self.registration_data, ignore_index=True)
            logger.info(f"Combined {len(self.combined_registration_data)} voter registration records")
        
        if self.election_history_data:
            logger.info(f"Combining election history data from {len(self.election_history_data)} sources")
            self.combined_election_history = pd.concat(self.election_history_data, ignore_index=True)
            logger.info(f"Combined {len(self.combined_election_history)} election history records")
        
        total_time = time.time() - start_time
        records_per_sec = len(self.combined_registration_data) / total_time if hasattr(self, 'combined_registration_data') else 0
        logger.info(f"Completed registration data processing in {total_time:.2f} seconds ({records_per_sec:.2f} records/sec)")
    
    def match_entities(self) -> None:
        """Match entities across all data sources."""
        logger.info("Starting entity matching")
        start_time = time.time()
        
        # Match registration data
        if hasattr(self, 'combined_registration_data'):
            self.entity_matches = self.matcher.match_records(self.combined_registration_data)
            logger.info(f"Matched {len(self.entity_matches)} registration records")
        
        total_time = time.time() - start_time
        logger.info(f"Completed entity matching in {total_time:.2f} seconds")
    
    def process_and_save_data(self) -> None:
        """Process and save the data into normalized tables."""
        logger.info("Processing data into normalized tables")
        start_time = time.time()
        
        # Process voter registration data
        if hasattr(self, 'combined_registration_data'):
            # Process registrations into normalized tables
            core_voters, addresses, registrations = self.processor.process_registration_data(
                self.combined_registration_data, self.entity_matches)
            
            # Update core voters with current address
            core_voters = self.processor.update_core_voters_with_current_address(core_voters, registrations)
            
            # Save the tables
            self.processor.save_tables(
                (core_voters, "core_voters.csv"),
                (addresses, "addresses.csv"),
                (registrations, "voter_registrations.csv")
            )
        
        # Process election history
        if hasattr(self, 'combined_election_history'):
            election_history = self.processor.process_election_history(
                self.combined_election_history, self.entity_matches)
            
            # Save the table
            self.processor.save_tables(
                (election_history, "election_history.csv")
            )
        
        total_time = time.time() - start_time
        logger.info(f"Completed data processing and saving in {total_time:.2f} seconds")
    
    def run_pipeline(self, sources: List[str]) -> None:
        """Run the full data pipeline.
        
        Args:
            sources: List of source names to process
        """
        logger.info(f"Starting full pipeline for sources: {sources}")
        pipeline_start = time.time()
        
        # Step 1: Process voter registrations (and election history if available)
        self.process_voter_registrations(sources)
        
        # Step 2: Match entities
        self.match_entities()
        
        # Step 3: Process and save normalized data
        self.process_and_save_data()
        
        pipeline_time = time.time() - pipeline_start
        logger.info(f"Completed full pipeline in {pipeline_time:.2f} seconds") 