"""Base adapter for data sources."""

import os
import pandas as pd
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Generator, Tuple, TYPE_CHECKING
import uuid
import glob
import multiprocessing as mp
from abc import ABC, abstractmethod
from tqdm import tqdm

# Use TYPE_CHECKING to avoid circular imports
if TYPE_CHECKING:
    from ..utils.config import Config

logger = logging.getLogger(__name__)

class BaseAdapter(ABC):
    """Base adapter for all data sources."""
    
    def __init__(self, config: 'Config', source_name: str):
        """Initialize the adapter.
        
        Args:
            config: Application configuration
            source_name: Name of the source in the configuration
        """
        self.config = config
        self.source_name = source_name
        self.source_config = config.get_source_config(source_name)
        
        if self.source_config is None:
            raise ValueError(f"Source {source_name} not found in configuration")
        
        self.input_dir = config.get("paths.input_dir")
        self.output_dir = config.get("paths.output_dir")
        self.chunk_size = config.get("processing.chunk_size", 10000)
        
        # Schema validation will be based on the type of data in the source
        self.data_type = self.source_config.get("type")
        self.schema = config.get_schema(self.data_type)
        
        if self.schema is None:
            logger.warning(f"Schema for {self.data_type} not found")
    
    def get_current_utc(self) -> str:
        """Return current UTC time in ISO format."""
        return datetime.now(timezone.utc).isoformat()
    
    def generate_id(self) -> str:
        """Generate a unique ID."""
        return str(uuid.uuid4())
    
    def find_source_files(self) -> List[str]:
        """Find all files matching the source pattern.
        
        Returns:
            List of full paths to source files
        """
        # First check if files are directly specified in config
        configured_files = self.source_config.get("files", [])
        if configured_files:
            files = [os.path.abspath(f) for f in configured_files]
            logger.info(f"Found {len(files)} files for source {self.source_name} from configuration")
            return files
        
        # Otherwise use file pattern matching
        pattern = self.source_config.get("file_pattern", "*.csv")
        files = []
        
        if self.input_dir is None:
            logger.warning(f"No input directory specified for source {self.source_name}")
            return []
            
        try:
            for filename in os.listdir(self.input_dir):
                import fnmatch
                if fnmatch.fnmatch(filename, pattern):
                    files.append(os.path.join(self.input_dir, filename))
        except FileNotFoundError:
            logger.warning(f"Input directory {self.input_dir} not found")
            return []
        
        logger.info(f"Found {len(files)} files for source {self.source_name}")
        return files
    
    def validate_schema(self, df: pd.DataFrame) -> bool:
        """Validate a dataframe against the schema.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            True if valid, False if not
        """
        if self.schema is None:
            logger.warning("No schema available for validation")
            return True
        
        # Get expected fields
        expected_fields = [field["name"] for field in self.schema]
        actual_fields = df.columns.tolist()
        
        # Check if all expected fields are present
        missing_fields = set(expected_fields) - set(actual_fields)
        if missing_fields:
            logger.warning(f"Missing fields in output: {missing_fields}")
            return False
        
        return True
    
    def read_in_chunks(self, file_path: str) -> Generator[pd.DataFrame, None, None]:
        """Read a file in chunks.
        
        Args:
            file_path: Path to file to read
            
        Yields:
            DataFrame chunks
        """
        logger.info(f"Reading {file_path} in chunks of {self.chunk_size}")
        for chunk in pd.read_csv(file_path, dtype=str, chunksize=self.chunk_size):
            yield chunk
    
    def process_chunk(self, chunk_and_index=None):
        """Process a chunk of data in multiprocessing-compatible way.
        
        Args:
            chunk_and_index: Tuple of (chunk, index) or just chunk if no index

        Returns:
            Processed data in standardized format
        """
        # Handle multiprocessing input format
        if isinstance(chunk_and_index, tuple) and len(chunk_and_index) == 2:
            chunk, index = chunk_and_index
        else:
            chunk = chunk_and_index
            index = None
            
        try:
            result = self._process_chunk_impl(chunk)
            if index is not None and index % 5 == 0:
                logger.debug(f"Processed chunk {index}")
            return result
        except Exception as e:
            logger.error(f"Error processing chunk {index if index is not None else 'unknown'}: {e}")
            raise
    
    def _process_chunk_impl(self, chunk):
        """Actual implementation of chunk processing to be overridden by subclasses."""
        # This should be overridden by subclasses
        raise NotImplementedError("Subclasses must implement _process_chunk_impl")
    
    def process_file(self, file_path: str) -> pd.DataFrame:
        """Process a file into the standardized format.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Processed data in standardized format
        """
        logger.info(f"Processing file: {file_path}")
        
        # Get the chunk size from config
        chunk_size = self.config.get("processing.chunk_size", 10000)
        logger.info(f"Reading {file_path} in chunks of {chunk_size}")
        
        # Get the number of workers from config
        num_workers = self.config.get("processing.num_workers")
        if num_workers is None:
            num_workers = max(1, mp.cpu_count() - 1)  # Use all but one core
        
        # Force single-threaded processing for now until multiprocessing is fixed
        num_workers = 1
        
        logger.info(f"Using {num_workers} workers for processing")
        
        # Read the file in chunks with flexible encoding and line termination
        try:
            chunks = pd.read_csv(
                file_path, 
                dtype=str, 
                chunksize=chunk_size,
                on_bad_lines='warn',
                encoding='utf-8',
                engine='python'
            )
        except UnicodeDecodeError:
            # Try with different encoding if UTF-8 fails
            logger.warning(f"UTF-8 encoding failed, trying with latin-1")
            chunks = pd.read_csv(
                file_path, 
                dtype=str, 
                chunksize=chunk_size,
                on_bad_lines='warn',
                encoding='latin-1',
                engine='python'
            )
        
        # Convert to list for progress tracking
        chunk_list = list(chunks)
        total_chunks = len(chunk_list)
        logger.info(f"File contains {total_chunks} chunks")
        
        if num_workers <= 1 or total_chunks <= 1:
            # Serial processing for single-core or small files
            processed_chunks = []
            for i, chunk in enumerate(tqdm(chunk_list, desc=f"Processing {os.path.basename(file_path)}")):
                try:
                    # Forward to subclass implementation
                    processed_chunk = self._process_chunk_impl(chunk)
                    processed_chunks.append(processed_chunk)
                    
                    if (i+1) % 5 == 0:
                        logger.debug(f"Processed {i+1}/{total_chunks} chunks")
                except Exception as e:
                    logger.error(f"Error processing chunk {i} of {file_path}: {e}")
                    raise
        else:
            # Parallel processing for multi-core
            # Prepare data with indices for tracking
            indexed_chunks = [(chunk, i) for i, chunk in enumerate(chunk_list)]
            
            # Process chunks in parallel
            with mp.Pool(processes=num_workers) as pool:
                processed_chunks = list(tqdm(
                    pool.imap(self.process_chunk, indexed_chunks),
                    total=total_chunks,
                    desc=f"Processing {os.path.basename(file_path)}"
                ))
        
        # Combine all chunks
        if not processed_chunks:
            logger.warning(f"No data processed from {file_path}")
            return pd.DataFrame()
            
        logger.info(f"Combining {len(processed_chunks)} chunks from {file_path}")
        combined = pd.concat(processed_chunks, ignore_index=True)
        logger.info(f"Processed {len(combined)} records from {file_path}")
        return combined
    
    def process_all_files(self) -> pd.DataFrame:
        """Process all files for this data source.
        
        Returns:
            Combined processed data
        """
        # Find files
        files = self.find_source_files()
        if not files:
            logger.warning(f"No files found for source {self.source_name}")
            return pd.DataFrame()
        
        logger.info(f"Found {len(files)} files for source {self.source_name}")
        
        # Process each file
        start_time = datetime.now()
        processed_files = []
        
        # Use tqdm to show progress for file processing
        for file_path in tqdm(files, desc=f"Processing files for {self.source_name}"):
            try:
                file_data = self.process_file(file_path)
                if not file_data.empty:
                    processed_files.append(file_data)
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                raise
        
        # Combine all files
        if not processed_files:
            logger.warning(f"No data processed for source {self.source_name}")
            return pd.DataFrame()
            
        logger.info(f"Combining data from {len(processed_files)} files for {self.source_name}")
        result = pd.concat(processed_files, ignore_index=True)
        
        # Log performance metrics
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Processed {len(result):,} records in {processing_time:.2f} seconds ({len(result)/processing_time:.2f} records/sec)")
        
        return result 