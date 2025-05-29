import yaml
import concurrent.futures
from typing import List, Dict
import logging
from pathlib import Path
import os
import re

from .data_ingestion import ShopifyDataIngestion
from ..data_warehouse.snowflake_connector import SnowflakeConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MultiStoreIngestion:
    """Handles data ingestion for multiple Shopify stores in parallel."""
    
    def __init__(self, config_path: str = None):
        """Initialize multi-store ingestion with configuration."""
        self.config_path = config_path or os.path.join('config', 'stores.yaml')
        self.stores_config = self._load_config()
        self.max_workers = 5  # Maximum number of concurrent store ingestions
        
        # Load default Snowflake configuration from environment
        self.default_snowflake_config = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'role': os.getenv('SNOWFLAKE_ROLE')
        }
        
        # Validate configuration
        missing_env_vars = [k for k, v in self.default_snowflake_config.items() if not v]
        if missing_env_vars:
            logger.error(f"Missing required environment variables: {', '.join(missing_env_vars)}")
        
    def _resolve_env_vars(self, value: str) -> str:
        """Resolve environment variables in string values."""
        if not isinstance(value, str):
            return value
            
        # Find all ${VAR} or $VAR patterns
        pattern = r'\${([^}]+)}|\$([a-zA-Z_][a-zA-Z0-9_]*)'
        
        def replace_var(match):
            var_name = match.group(1) or match.group(2)
            env_value = os.getenv(var_name)
            if env_value is None:
                logger.warning(f"Environment variable {var_name} not found")
                return match.group(0)  # Return original if not found
            logger.debug(f"Resolved environment variable {var_name}")
            return env_value
            
        return re.sub(pattern, replace_var, value)

    def _resolve_config_env_vars(self, config: Dict) -> Dict:
        """Recursively resolve environment variables in configuration."""
        if isinstance(config, dict):
            return {k: self._resolve_config_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._resolve_config_env_vars(v) for v in config]
        elif isinstance(config, str):
            return self._resolve_env_vars(config)
        return config
        
    def _load_config(self) -> Dict:
        """Load store configurations from YAML file and environment."""
        config = {}
        
        # Add default store from environment variables if available
        shop_name = os.getenv('SHOPIFY_SHOP_NAME')
        access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
        
        logger.info("Loading environment variables:")
        logger.info(f"  SHOPIFY_SHOP_NAME: {shop_name}")
        token_preview = access_token[:4] + '...' if access_token else 'Not set'
        logger.info(f"  SHOPIFY_ACCESS_TOKEN: {token_preview}")
        
        if shop_name and access_token:
            logger.info(f"Loading configuration for shop: {shop_name}")
            config['default_store'] = {
                'name': shop_name,
                'shopify': {
                    'shop_url': f"{shop_name}.myshopify.com",
                    'access_token': access_token,
                    'api_version': os.getenv('SHOPIFY_API_VERSION', '2024-01')
                },
                'snowflake': {
                    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
                    'database': os.getenv('SNOWFLAKE_DATABASE'),
                    'schema': 'STORE_DEFAULT'
                }
            }
        else:
            logger.warning("Missing required Shopify environment variables: SHOPIFY_SHOP_NAME or SHOPIFY_ACCESS_TOKEN")
        
        # Load additional stores from YAML if available
        try:
            logger.info(f"Loading store configurations from: {self.config_path}")
            with open(self.config_path, 'r') as f:
                yaml_config = yaml.safe_load(f)
                if yaml_config and 'stores' in yaml_config:
                    # Log raw configuration before resolution
                    logger.info("Raw store configurations:")
                    for store_id, store_config in yaml_config['stores'].items():
                        logger.info(f"  {store_id}:")
                        if 'shopify' in store_config:
                            logger.info(f"    shop_url: {store_config['shopify'].get('shop_url')}")
                            logger.info(f"    api_version: {store_config['shopify'].get('api_version')}")
                            token = store_config['shopify'].get('access_token', '')
                            if isinstance(token, str) and '${' in token:
                                logger.info(f"    access_token template: {token}")
                    
                    # Resolve environment variables in the YAML configuration
                    resolved_config = self._resolve_config_env_vars(yaml_config['stores'])
                    config.update(resolved_config)
                    logger.info(f"Loaded {len(yaml_config['stores'])} additional stores from config file")
                    
                    # Log resolved configuration
                    logger.info("Resolved store configurations:")
                    for store_id, store_config in resolved_config.items():
                        logger.info(f"  {store_id}:")
                        if 'shopify' in store_config:
                            logger.info(f"    shop_url: {store_config['shopify'].get('shop_url')}")
                            logger.info(f"    api_version: {store_config['shopify'].get('api_version')}")
                            token = store_config['shopify'].get('access_token', '')
                            token_preview = token[:4] + '...' if token else 'Not set'
                            logger.info(f"    access_token: {token_preview}")
                    
        except FileNotFoundError:
            logger.warning(f"Store configuration file not found: {self.config_path}")
        except Exception as e:
            logger.error(f"Error loading store configuration: {str(e)}")
        
        return config
            
    def _setup_store_connection(self, store_id: str) -> tuple:
        """Set up connections for a specific store."""
        try:
            if store_id not in self.stores_config:
                raise KeyError(f"Store '{store_id}' not found in configuration")
                
            store_config = self.stores_config[store_id]
            logger.info(f"Setting up connection for store: {store_config['name']}")
            
            # Debug logging for store configuration
            logger.info(f"Store configuration for {store_id}:")
            if 'shopify' in store_config:
                logger.info(f"  Shopify config:")
                logger.info(f"    shop_url: {store_config['shopify'].get('shop_url')}")
                logger.info(f"    api_version: {store_config['shopify'].get('api_version')}")
                # Log partial access token for debugging (first 4 chars)
                token = store_config['shopify'].get('access_token', '')
                token_preview = token[:4] + '...' if token else 'Not set'
                logger.info(f"    access_token: {token_preview}")
            
            # Initialize Snowflake connection for the store
            snowflake_config = self.default_snowflake_config.copy()
            if 'snowflake' in store_config:
                snowflake_config.update({
                    'warehouse': store_config['snowflake'].get('warehouse', snowflake_config['warehouse']),
                    'database': store_config['snowflake'].get('database', snowflake_config['database']),
                    'schema': store_config['snowflake'].get('schema', f"STORE_{store_id.upper()}")
                })
            
            # Initialize Shopify connection for the store
            if 'shopify' not in store_config:
                raise ValueError(f"Missing Shopify configuration for store {store_id}")
                
            shopify_config = {
                'shop_url': store_config['shopify']['shop_url'],
                'access_token': store_config['shopify']['access_token'],
                'api_version': store_config['shopify'].get('api_version', '2024-01')
            }
            
            # Debug logging for final configurations
            logger.info("Final Shopify configuration:")
            logger.info(f"  shop_url: {shopify_config['shop_url']}")
            logger.info(f"  api_version: {shopify_config['api_version']}")
            token_preview = shopify_config['access_token'][:4] + '...' if shopify_config['access_token'] else 'Not set'
            logger.info(f"  access_token: {token_preview}")
            
            # Validate configurations
            missing_snowflake = [k for k, v in snowflake_config.items() if not v]
            if missing_snowflake:
                raise ValueError(f"Missing Snowflake configuration: {', '.join(missing_snowflake)}")
                
            missing_shopify = [k for k, v in shopify_config.items() if not v]
            if missing_shopify:
                raise ValueError(f"Missing Shopify configuration: {', '.join(missing_shopify)}")
            
            return snowflake_config, shopify_config
            
        except Exception as e:
            logger.error(f"Error setting up connections for store {store_id}: {str(e)}")
            raise
        
    def _ingest_store_data(self, store_id: str) -> bool:
        """Ingest data for a single store."""
        try:
            logger.info(f"Starting data ingestion for store: {store_id}")
            
            # Set up connections
            snowflake_config, shopify_config = self._setup_store_connection(store_id)
            
            # Log configuration (excluding sensitive data)
            logger.info(f"Using Snowflake config: database={snowflake_config['database']}, "
                       f"warehouse={snowflake_config['warehouse']}, schema={snowflake_config['schema']}")
            logger.info(f"Using Shopify config: shop_url={shopify_config['shop_url']}, "
                       f"api_version={shopify_config['api_version']}")
            
            # Initialize ingestion for the store
            ingestion = ShopifyDataIngestion(
                snowflake_config=snowflake_config,
                shopify_config=shopify_config
            )
            
            # Run ingestion
            success = ingestion.run_ingestion()
            
            if success:
                logger.info(f"Successfully completed ingestion for store: {store_id}")
            else:
                logger.error(f"Ingestion failed for store: {store_id}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error during ingestion for store {store_id}: {str(e)}")
            return False
            
    def ingest_stores(self, store_ids: List[str] = None) -> Dict[str, bool]:
        """
        Ingest data for specified stores in parallel.
        
        Args:
            store_ids: List of store IDs to process. If None, process all stores.
            
        Returns:
            Dictionary mapping store IDs to ingestion success status.
        """
        store_ids = store_ids or list(self.stores_config.keys())
        results = {}
        
        # Validate store IDs
        invalid_stores = [store_id for store_id in store_ids if store_id not in self.stores_config]
        if invalid_stores:
            logger.error(f"Invalid store IDs: {', '.join(invalid_stores)}")
            return {store_id: False for store_id in store_ids}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all ingestion tasks
            future_to_store = {
                executor.submit(self._ingest_store_data, store_id): store_id 
                for store_id in store_ids
            }
            
            # Process completed tasks
            for future in concurrent.futures.as_completed(future_to_store):
                store_id = future_to_store[future]
                try:
                    success = future.result()
                    results[store_id] = success
                except Exception as e:
                    logger.error(f"Unexpected error for store {store_id}: {str(e)}")
                    results[store_id] = False
                    
        return results
        
    def ingest_all_stores(self) -> Dict[str, bool]:
        """Ingest data for all configured stores."""
        return self.ingest_stores()
        
    def get_store_status(self, store_id: str) -> Dict:
        """Get current ingestion status and metrics for a store."""
        try:
            snowflake_config, _ = self._setup_store_connection(store_id)
            connector = SnowflakeConnector(**snowflake_config)
            
            # Get basic metrics
            metrics = connector.execute_query("""
                SELECT 
                    (SELECT COUNT(*) FROM customers) as customer_count,
                    (SELECT COUNT(*) FROM orders) as order_count,
                    (SELECT MAX(created_at) FROM orders) as last_order_date
            """)
            
            return {
                'store_id': store_id,
                'metrics': metrics[0] if metrics else None,
                'status': 'active'
            }
            
        except Exception as e:
            logger.error(f"Error getting status for store {store_id}: {str(e)}")
            return {
                'store_id': store_id,
                'metrics': None,
                'status': 'error',
                'error': str(e)
            } 