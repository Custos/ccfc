"""Entity matching and deduplication for voter records."""

import pandas as pd
import numpy as np
import logging
import recordlinkage
from recordlinkage.preprocessing import clean as rl_clean
from recordlinkage.index import Full, Block, SortedNeighbourhood
from typing import Dict, List, Tuple, Optional, Set, Any
import uuid
import re

logger = logging.getLogger(__name__)

class EntityMatcher:
    """Entity matcher for voter records."""
    
    def __init__(self, config):
        """Initialize the entity matcher.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.matching_config = config.get_matching_config()
        
        # Extract matching thresholds
        self.match_threshold = self.matching_config.get("match_threshold", 0.8)
        self.high_confidence_threshold = self.matching_config.get("high_confidence_threshold", 0.95)
        
        # Get the fields to use for matching
        self.exact_match_fields = self.matching_config.get("exact_match_fields", [])
        self.prob_match_fields = self.matching_config.get("probabilistic_match_fields", [])
        
        # Store the entity mapping
        self.entity_map = {}  # Map from source_id to entity_id
        
        # Count of new and matched entities
        self.new_entity_count = 0
        self.matched_entity_count = 0
    
    def _clean_phone_number(self, phone: str) -> str:
        """Clean and standardize phone number format.
        
        Args:
            phone: Phone number string
            
        Returns:
            Standardized phone number or empty string if invalid
        """
        if pd.isna(phone):
            return ""
            
        # Convert to string and remove all non-numeric characters
        phone_str = str(phone)
        digits = re.sub(r'\D', '', phone_str)
        
        # Handle different formats
        if len(digits) == 10:
            # Standard 10-digit format
            return digits
        elif len(digits) == 11 and digits.startswith('1'):
            # 11-digit format starting with 1
            return digits[1:]
        elif len(digits) == 7:
            # 7-digit format (area code missing)
            return ""
        else:
            # Invalid format
            return ""
    
    def _preprocess_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Preprocess a dataframe for matching.
        
        Args:
            df: Input dataframe
            
        Returns:
            Preprocessed dataframe
        """
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Clean string fields for better matching
        string_columns = df.select_dtypes(include=['object']).columns
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).apply(lambda x: x.lower() if isinstance(x, str) else x)
                df[col] = rl_clean(df[col])
        
        # Clean phone numbers
        if 'phone_number' in df.columns:
            df['_phone_number'] = df['phone_number'].apply(self._clean_phone_number)
        
        # For date fields, ensure they are in a standard format
        date_columns = ['_dob']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        return df
    
    def _add_new_entity(self, record_data: pd.Series) -> str:
        """Add a new entity to the system.
        
        Args:
            record_data: Record data
            
        Returns:
            New entity ID
        """
        # Generate a new entity ID
        entity_id = str(uuid.uuid4())
        
        # Get the source record ID
        source_system = record_data.get('source_system', 'unknown')
        source_id = record_data.get('source_record_id', 'unknown')
        
        # Add to the entity map
        self.entity_map[(source_system, source_id)] = entity_id
        
        # Increment the new entity counter
        self.new_entity_count += 1
        
        return entity_id
    
    def _match_exact(self, df: pd.DataFrame) -> Tuple[Dict[str, str], pd.DataFrame]:
        """Perform exact matching on the records.
        
        Args:
            df: Input dataframe
            
        Returns:
            Mapping from source IDs to entity IDs and remaining unmatched records
        """
        logger.info("Performing exact matching")
        matched_ids = {}
        
        # For each set of fields for exact matching
        for field_set in self.exact_match_fields:
            logger.info(f"Exact matching on fields: {field_set}")
            
            # Skip if any field is missing
            if not all(f"_{f}" in df.columns for f in field_set):
                logger.warning(f"Skipping exact match on {field_set} - some fields missing")
                continue
            
            # Group by the exact match fields
            if len(field_set) == 1:
                # Single field match
                field = f"_{field_set[0]}"
                groups = df.groupby(field)
            else:
                # Multi-field match
                group_fields = [f"_{f}" for f in field_set]
                groups = df.groupby(group_fields)
            
            # Process each group
            for _, group in groups:
                if len(group) <= 1:
                    continue  # No duplicates
                
                # Get the first record as the canonical one
                canonical = group.iloc[0]
                entity_id = None
                
                # Check if any record in the group already has an entity ID
                for _, record in group.iterrows():
                    source_system = record.get('source_system', 'unknown')
                    source_id = record.get('source_record_id', 'unknown')
                    
                    if (source_system, source_id) in self.entity_map:
                        entity_id = self.entity_map[(source_system, source_id)]
                        break
                
                # If no entity ID found, create a new one
                if entity_id is None:
                    entity_id = self._add_new_entity(canonical)
                
                # Assign the entity ID to all records in the group
                for _, record in group.iterrows():
                    source_system = record.get('source_system', 'unknown')
                    source_id = record.get('source_record_id', 'unknown')
                    
                    matched_ids[(source_system, source_id)] = {
                        'entity_id': entity_id,
                        'confidence': 1.0,
                        'match_type': 'exact',
                        'match_fields': ','.join(field_set)
                    }
        
        # Filter out matched records
        unmatched = df[~df.apply(
            lambda row: (row.get('source_system', 'unknown'), row.get('source_record_id', 'unknown')) in matched_ids,
            axis=1
        )]
        
        logger.info(f"Exact matching found {len(matched_ids)} matches, {len(unmatched)} records remaining")
        return matched_ids, unmatched
    
    def _match_probabilistic(self, df: pd.DataFrame, 
                         batch_size: int = 1000) -> Dict[str, Dict[str, Any]]:
        """Perform probabilistic matching on the records.
        
        Args:
            df: Input dataframe
            batch_size: Size of batches for processing
            
        Returns:
            Mapping from source IDs to entity IDs
        """
        if len(df) == 0:
            return {}
        
        logger.info(f"Performing probabilistic matching on {len(df)} records")
        matched_ids = {}
        
        # Check for the presence of blocking fields
        has_last_name = "_last_name" in df.columns
        has_zip = "_zip" in df.columns
        has_birth_year = "_birth_year" in df.columns
        
        # Create a record linkage comparison object
        indexer = recordlinkage.Index()
        
        # Add blocking for efficiency - only compare records that share certain characteristics
        if has_last_name:
            logger.info("Using last name blocking")
            # Block on the first 2 characters of last name
            indexer.add(Block("_last_name", 2))
        elif has_zip:
            logger.info("Using ZIP code blocking")
            # Block on zip code if last name not available
            indexer.add(Block("_zip"))
        elif has_birth_year:
            logger.info("Using birth year blocking")
            # Block on birth year as a fallback
            indexer.add(Block("_birth_year"))
        else:
            # Full comparison if no good blocking fields available
            logger.warning("No good blocking fields available, using SortedNeighbourhood indexing")
            # Use a sorted neighborhood approach on voter_id as a last resort
            if 'voter_id' in df.columns:
                indexer.add(SortedNeighbourhood('voter_id', window=3))
            else:
                logger.warning("No suitable fields for blocking, creating a small full index")
                # Limit the number of records to avoid excessive comparisons
                if len(df) > 1000:
                    logger.warning(f"Too many records ({len(df)}) for full comparison, sampling 1000")
                    sample_df = df.sample(1000)
                    return self._match_probabilistic(sample_df, batch_size)
                else:
                    indexer.add(Full())
        
        # Create the candidate pairs
        try:
            pairs = indexer.index(df)
            logger.info(f"Generated {len(pairs)} candidate pairs for comparison")
        except KeyError as e:
            logger.error(f"Error with indexing: {e}")
            logger.info(f"Available columns: {df.columns.tolist()}")
            return {}
        
        # Define the comparison
        compare = recordlinkage.Compare()
        
        # Add comparisons based on configuration
        for field_config in self.prob_match_fields:
            field_name = field_config.get("name")
            weight = field_config.get("weight", 1.0)
            method = field_config.get("method", "exact")
            threshold = field_config.get("threshold", 0.85)
            
            if f"_{field_name}" not in df.columns:
                logger.warning(f"Field {field_name} not in dataframe, skipping")
                continue
            
            if method == "exact":
                compare.exact(f"_{field_name}", f"_{field_name}", label=field_name, missing_value=0)
            elif method == "string":
                compare.string(f"_{field_name}", f"_{field_name}", threshold=threshold, label=field_name, missing_value=0)
            elif method == "levenshtein":
                compare.string(f"_{field_name}", f"_{field_name}", method="levenshtein", threshold=threshold, 
                                label=field_name, missing_value=0)
            elif method == "jarowinkler":
                compare.string(f"_{field_name}", f"_{field_name}", method="jarowinkler", threshold=threshold, 
                                label=field_name, missing_value=0)
            elif method == "date":
                compare.date(f"_{field_name}", f"_{field_name}", label=field_name, missing_value=0)
            else:
                logger.warning(f"Unknown comparison method {method} for field {field_name}")
        
        # Compute similarities
        features = compare.compute(pairs, df)
        
        # Apply weights to the features
        for field_config in self.prob_match_fields:
            field_name = field_config.get("name")
            weight = field_config.get("weight", 1.0)
            
            if field_name in features.columns:
                features[field_name] = features[field_name] * weight
        
        # Sum up the weighted scores
        features['total_score'] = features.sum(axis=1) / sum(f.get("weight", 1.0) 
                                                           for f in self.prob_match_fields 
                                                           if f.get("name") in features.columns)
        
        # Get matches above threshold
        matches = features[features['total_score'] >= self.match_threshold]
        logger.info(f"Probabilistic matching found {len(matches)} matches above threshold {self.match_threshold}")
        
        # Create map of source_id to entity_id
        for idx, score in matches['total_score'].items():
            # Get the record indices
            rec1_idx, rec2_idx = idx
            
            # Get the record data
            rec1 = df.iloc[rec1_idx]
            rec2 = df.iloc[rec2_idx]
            
            source_system1 = rec1.get('source_system', 'unknown')
            source_id1 = rec1.get('source_record_id', 'unknown')
            source_system2 = rec2.get('source_system', 'unknown')
            source_id2 = rec2.get('source_record_id', 'unknown')
            
            # Determine the entity ID to use
            key1 = (source_system1, source_id1)
            key2 = (source_system2, source_id2)
            
            entity_id = None
            
            # Check if either record already has an entity ID
            if key1 in self.entity_map:
                entity_id = self.entity_map[key1]
            elif key2 in self.entity_map:
                entity_id = self.entity_map[key2]
            else:
                # Create a new entity ID if neither record has one
                entity_id = self._add_new_entity(rec1)
            
            # Set the entity ID for both records
            self.entity_map[key1] = entity_id
            self.entity_map[key2] = entity_id
            
            # Add to the matched IDs
            matched_ids[key1] = {
                'entity_id': entity_id,
                'confidence': float(score),
                'match_type': 'probabilistic',
                'match_fields': ','.join(features.columns[:-1])
            }
            
            matched_ids[key2] = {
                'entity_id': entity_id,
                'confidence': float(score),
                'match_type': 'probabilistic',
                'match_fields': ','.join(features.columns[:-1])
            }
        
        return matched_ids
    
    def match_records(self, records: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Match records to entities.
        
        Args:
            records: Records to match
            
        Returns:
            Mapping from source IDs to entity information
        """
        logger.info(f"Matching {len(records)} records")
        
        # Preprocess the records
        prepped_records = self._preprocess_dataframe(records)
        
        # Perform exact matching
        exact_matches, unmatched = self._match_exact(prepped_records)
        
        # Perform probabilistic matching on remaining records
        prob_matches = self._match_probabilistic(unmatched)
        
        # Combine the matches
        all_matches = {**exact_matches, **prob_matches}
        
        # For records that weren't matched, create new entities
        for _, record in unmatched.iterrows():
            source_system = record.get('source_system', 'unknown')
            source_id = record.get('source_record_id', 'unknown')
            
            key = (source_system, source_id)
            if key not in all_matches:
                entity_id = self._add_new_entity(record)
                
                all_matches[key] = {
                    'entity_id': entity_id,
                    'confidence': 1.0,  # New entity, so confidence is 1.0
                    'match_type': 'new',
                    'match_fields': ''
                }
        
        # Log matching statistics
        total_matches = len([m for m in all_matches.values() if m['match_type'] != 'new'])
        high_confidence = len([m for m in all_matches.values() 
                             if m['match_type'] != 'new' and m['confidence'] >= self.high_confidence_threshold])
        
        logger.info(f"Matching complete: {total_matches} matches, {self.new_entity_count} new entities")
        logger.info(f"High confidence matches: {high_confidence}")
        
        return all_matches 