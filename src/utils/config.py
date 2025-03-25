"""Configuration utilities for the CCFC Voter Data Platform."""

import os
import yaml
import json
import logging
from typing import Dict, Any, List, Optional, Union, TypedDict

# Type alias for configuration dictionary
ConfigType = Dict[str, Any]

logger = logging.getLogger(__name__)

class Config:
    """Configuration manager for the CCFC Voter Data Platform."""
    
    def __init__(self, config_path: str = None):
        """Initialize the configuration manager.
        
        Args:
            config_path: Path to the configuration file. If None, uses default.
        """
        if config_path is None:
            # Default to the config directory in the project root
            root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            config_path = os.path.join(root_dir, "config", "config.yaml")
        
        logger.info(f"Loading configuration from {config_path}")
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        
        # Load schemas
        self.schemas = self._load_schemas()
    
    def _load_schemas(self) -> Dict[str, List[Dict[str, Any]]]:
        """Load all schema definitions."""
        schemas = {}
        schema_dir = self.get("paths.schema_dir")
        
        if not schema_dir:
            logger.warning("No schema directory specified in config")
            return schemas
        
        # Get the absolute path to the schema directory
        root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        schema_dir = os.path.join(root_dir, schema_dir)
        
        if not os.path.exists(schema_dir):
            logger.warning(f"Schema directory {schema_dir} does not exist")
            return schemas
        
        # Load each schema file
        for filename in os.listdir(schema_dir):
            if filename.endswith("_schema.json"):
                schema_name = filename.replace("_schema.json", "")
                schema_path = os.path.join(schema_dir, filename)
                
                with open(schema_path, "r") as f:
                    try:
                        schema = json.load(f)
                        schemas[schema_name] = schema
                        logger.debug(f"Loaded schema: {schema_name}")
                    except json.JSONDecodeError:
                        logger.error(f"Error loading schema from {schema_path}")
        
        logger.info(f"Loaded {len(schemas)} schemas: {', '.join(schemas.keys())}")
        return schemas
    
    def get(self, path: str, default: Any = None) -> Any:
        """Get a configuration value using dot notation.
        
        Args:
            path: Configuration path in dot notation (e.g., "paths.input_dir")
            default: Default value to return if path not found
            
        Returns:
            Configuration value or default
        """
        parts = path.split(".")
        value = self.config
        
        for part in parts:
            if part not in value:
                return default
            value = value[part]
        
        return value
    
    def get_schema(self, name: str) -> Optional[List[Dict[str, Any]]]:
        """Get a schema by name.
        
        Args:
            name: Schema name without _schema.json suffix
            
        Returns:
            Schema definition or None if not found
        """
        return self.schemas.get(name)
    
    def get_source_config(self, source_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for a specific data source.
        
        Args:
            source_name: Name of the source
            
        Returns:
            Source configuration or None if not found
        """
        sources = self.get("sources", {})
        return sources.get(source_name)
    
    def get_matching_config(self) -> Dict[str, Any]:
        """Get the matching configuration."""
        return self.get("matching", {})
    
    def set(self, path: str, value: Any) -> None:
        """Set a configuration value using dot notation.
        
        Args:
            path: Configuration path in dot notation (e.g., "paths.input_dir")
            value: Value to set
        """
        parts = path.split(".")
        config = self.config
        
        # Navigate to the right nested dictionary
        for part in parts[:-1]:
            if part not in config:
                config[part] = {}
            config = config[part]
        
        # Set the value at the final level
        config[parts[-1]] = value
        logger.debug(f"Set config value at {path} to {value}") 