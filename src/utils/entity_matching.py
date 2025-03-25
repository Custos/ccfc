"""Entity matching utilities for the CCFC Voter Data Platform."""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional
import jellyfish

logger = logging.getLogger(__name__)

class EntityMatcher:
    """Match entities across different data sources."""
    
    def __init__(self, config):
        """Initialize the entity matcher.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.entities = {}
        # Use lower threshold for donation matching - default is 0.8, but 0.7 is more appropriate for donations
        self.match_threshold = config.get("entity_matching.threshold", 0.7)
        self.name_weight = config.get("entity_matching.weights.name", 0.7)  # Increased from 0.6
        self.address_weight = config.get("entity_matching.weights.address", 0.3)  # Decreased from 0.4
        
        # Add cache for faster repeat lookups
        self.match_cache = {}
        
        logger.info(f"Entity matcher initialized with threshold {self.match_threshold}")
    
    def load_entities(self, df: pd.DataFrame) -> None:
        """Load entity records for matching.
        
        Args:
            df: DataFrame containing entity records
        """
        logger.info(f"Loading {len(df)} entities for matching")
        
        # Create a dictionary for fast lookup
        entities_loaded = 0
        skipped_entities = 0
        
        for _, row in df.iterrows():
            entity_id = row.get('voter_id')
            if not entity_id:
                skipped_entities += 1
                continue
                
            # Create normalized keys for matching
            first_name = str(row.get('first_name', '')).lower().strip()
            last_name = str(row.get('last_name', '')).lower().strip()
            
            # Skip entities without minimal matching info
            if not first_name or not last_name:
                skipped_entities += 1
                continue
            
            # Store all available attributes for matching
            self.entities[entity_id] = {
                'entity_id': entity_id,
                'first_name': first_name,
                'last_name': last_name,
                'middle_name': str(row.get('middle_name', '')).lower().strip(),
                'suffix': str(row.get('suffix', '')).lower().strip(),
                'city': str(row.get('city', '')).lower().strip(),
                'state': str(row.get('state', '')).lower().strip(),
                'zip': str(row.get('zip', '')).lower().strip(),
                'county': str(row.get('county', '')).lower().strip()
            }
            entities_loaded += 1
        
        if skipped_entities > 0:
            logger.warning(f"Skipped {skipped_entities} entities with missing ID or name information")
        
        logger.info(f"Loaded {entities_loaded} valid entities for matching")
    
    def match_entity(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Match an entity record against the loaded entities.
        
        Args:
            record: Entity record to match
            
        Returns:
            Dictionary with match information or None if no match found
        """
        if not self.entities:
            logger.warning("No entities loaded for matching")
            return None
            
        # Normalize record for matching
        first_name = str(record.get('first_name', '')).lower().strip()
        last_name = str(record.get('last_name', '')).lower().strip()
        middle_name = str(record.get('middle_name', '')).lower().strip()
        city = str(record.get('city', '')).lower().strip()
        state = str(record.get('state', '')).lower().strip()
        zip_code = str(record.get('zip', '')).lower().strip()
        
        # Skip entities without minimal matching info
        if not first_name or not last_name:
            return {'match_found': False, 'reason': 'missing_name'}
        
        # Check cache first
        cache_key = f"{first_name}|{last_name}|{middle_name}|{city}|{state}|{zip_code}"
        if cache_key in self.match_cache:
            return self.match_cache[cache_key]
        
        # Find best match
        best_match = None
        best_score = 0.0
        
        # Create a first pass using exact last name matches (for optimization)
        candidate_entities = {}
        for entity_id, entity in self.entities.items():
            # Quick initial filter: if last names match exactly or partial match for long names
            if (last_name == entity['last_name'] or 
                (len(last_name) > 5 and entity['last_name'].startswith(last_name)) or
                (len(entity['last_name']) > 5 and last_name.startswith(entity['last_name']))):
                candidate_entities[entity_id] = entity
        
        # If we have too few candidates, expand to all entities
        if len(candidate_entities) < 10:
            candidate_entities = self.entities
        
        # Now do detailed matching on candidates
        for entity_id, entity in candidate_entities.items():
            # Calculate name similarity
            first_name_sim = jellyfish.jaro_winkler_similarity(first_name, entity['first_name'])
            last_name_sim = jellyfish.jaro_winkler_similarity(last_name, entity['last_name'])
            
            # Calculate middle name similarity if available
            middle_name_sim = 1.0
            if middle_name and entity['middle_name']:
                middle_name_sim = jellyfish.jaro_winkler_similarity(middle_name, entity['middle_name'])
            
            # Name score is weighted average of first, middle, last
            name_score = (first_name_sim * 0.4 + last_name_sim * 0.5 + middle_name_sim * 0.1)
            
            # Calculate address similarity if available
            address_score = 0.0
            address_count = 0
            
            if city and entity['city']:
                city_sim = jellyfish.jaro_winkler_similarity(city, entity['city'])
                address_score += city_sim
                address_count += 1
            
            if state and entity['state']:
                state_sim = 1.0 if state == entity['state'] else 0.0
                address_score += state_sim
                address_count += 1
            
            if zip_code and entity['zip']:
                # For zip codes, consider first 5 digits (primary postal code)
                # More lenient zip matching for donations
                if zip_code[:5] == entity['zip'][:5]:
                    zip_sim = 1.0
                elif zip_code and entity['zip'] and zip_code[:3] == entity['zip'][:3]:
                    # Same first 3 digits = same general area, partial match
                    zip_sim = 0.7
                else:
                    zip_sim = 0.0
                    
                address_score += zip_sim
                address_count += 1
            
            # Calculate final address score
            if address_count > 0:
                address_score /= address_count
            else:
                address_score = 0.0
            
            # Calculate overall score
            overall_score = (name_score * self.name_weight + 
                            address_score * self.address_weight)
            
            # For high name confidence, relax the requirement for address match
            if name_score > 0.95:
                overall_score = max(overall_score, name_score * 0.9)
            
            # Update best match if better
            if overall_score > best_score and overall_score >= self.match_threshold:
                best_score = overall_score
                best_match = {
                    'entity_id': entity_id,
                    'match_score': overall_score,
                    'name_score': name_score,
                    'address_score': address_score,
                    'match_found': True
                }
        
        if not best_match:
            result = {'match_found': False}
        else:
            result = best_match
            
        # Cache the result
        self.match_cache[cache_key] = result
        
        return result 

    def match_voter_records(self, voter_df: pd.DataFrame) -> pd.DataFrame:
        """Match a DataFrame of voter records against the loaded entities.
        
        Args:
            voter_df: DataFrame containing voter records to match
            
        Returns:
            DataFrame with match results added
        """
        if voter_df.empty:
            return pd.DataFrame()
            
        # Add columns for match results
        voter_df = voter_df.copy()
        voter_df['match_found'] = False
        voter_df['matched_voter_id'] = None
        voter_df['match_score'] = 0.0
        
        # Match each voter record
        for idx, record in voter_df.iterrows():
            # Create a dict for matching
            match_record = {
                'first_name': record.get('first_name', ''),
                'last_name': record.get('last_name', ''),
                'middle_name': record.get('middle_name', ''),
                'city': record.get('_city', ''),
                'state': record.get('_state', ''),
                'zip': record.get('_zip', '')
            }
            
            # Try to match
            match_result = self.match_entity(match_record)
            
            if match_result and match_result.get('match_found', False):
                # Update match information
                voter_df.at[idx, 'match_found'] = True
                voter_df.at[idx, 'matched_voter_id'] = match_result['entity_id']
                voter_df.at[idx, 'match_score'] = match_result['match_score']
                
                # Update the voter ID to use the matched ID
                orig_voter_id = voter_df.at[idx, 'voter_id']
                voter_df.at[idx, 'voter_id'] = match_result['entity_id']
                
                # Log the match
                if idx % 100 == 0:  # Log every 100th match for performance
                    logger.debug(f"Matched {record.get('first_name', '')} {record.get('last_name', '')} to existing voter {match_result['entity_id']} with score {match_result['match_score']:.2f}")
        
        # Log match statistics
        matched_count = len(voter_df[voter_df['match_found'] == True])
        unmatched_count = len(voter_df) - matched_count
        logger.info(f"Entity matching complete: {matched_count} matched, {unmatched_count} new")
        
        return voter_df 