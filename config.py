import os
from dataclasses import dataclass
from typing import Optional, Dict
from dotenv import load_dotenv
import yaml
from urllib.parse import quote_plus

# Load environment variables
load_dotenv()

@dataclass
class ShopifyConfig:
    shop_name: str
    shop_url: str
    api_key: str
    api_secret: str
    access_token: str
    api_version: str
    schema_name: str

    @classmethod
    def from_env(cls, store_config: Dict) -> 'ShopifyConfig':
        """Create Shopify configuration from environment variables and store config."""
        return cls(
            shop_name=os.getenv('SHOPIFY_SHOP_NAME'),
            shop_url=store_config.get('shop_url', os.getenv('SHOPIFY_SHOP_URL')),
            api_key=os.getenv('SHOPIFY_API_KEY'),
            api_secret=os.getenv('SHOPIFY_API_SECRET'),
            access_token=os.getenv('SHOPIFY_ACCESS_TOKEN'),
            api_version=store_config.get('api_version', os.getenv('SHOPIFY_API_VERSION')),
            schema_name=store_config.get('schema_name')
        )

@dataclass
class SnowflakeConfig:
    user: str
    password: str
    account: str
    warehouse: str
    database: str
    role: str
    schema: Optional[str] = None
    
    @classmethod
    def from_env(cls, schema: Optional[str] = None) -> 'SnowflakeConfig':
        """Create configuration from environment variables."""
        # Use the same account value that works in minimal_test.py
        account = 'pipykkn-pvb40654'
        
        return cls(
            user='jleu',  # Use the working username from minimal_test.py
            password='SwitchTeam123!%',  # Use the working password from minimal_test.py
            account=account,
            warehouse='CLV_WAREHOUSE',
            database='CLV_ANALYTICS',
            role='SHOPIFY_CLV_ROLE',  # Use SHOPIFY_CLV_ROLE instead of ACCOUNTADMIN
            schema=schema
        )
    
    def to_dict(self) -> dict:
        """Convert config to dictionary for SQLAlchemy."""
        config_dict = {
            'user': self.user,
            'password': self.password,
            'account': self.account,
            'warehouse': self.warehouse,
            'database': self.database,
            'role': self.role,
            'client_session_keep_alive': True,
            'authenticator': 'snowflake'
        }
        if self.schema:
            config_dict['schema'] = self.schema
        return config_dict

    def get_connection_url(self) -> str:
        """Get SQLAlchemy connection URL for Snowflake."""
        # Properly encode password for URL
        encoded_password = quote_plus(self.password)
        
        # Build the basic connection URL without additional parameters
        conn_str = f"snowflake://{self.user}:{encoded_password}@{self.account}"
        
        # Add database and schema
        conn_str += f"/{self.database}"
        if self.schema:
            conn_str += f"/{self.schema}"
            
        # Add warehouse and role
        conn_str += f"?warehouse={self.warehouse}&role={self.role}"
        
        return conn_str

    def get_engine_params(self) -> dict:
        """Get additional parameters for SQLAlchemy engine creation."""
        return {
            'connect_args': {
                'client_session_keep_alive': True,
                'authenticator': 'snowflake'
            }
        }

class StoreConfig:
    def __init__(self, store_name: str):
        self.store_name = store_name
        self.store_config = self._load_store_config()
        self.shopify = ShopifyConfig.from_env(self.store_config)
        self.snowflake = SnowflakeConfig.from_env(self.store_config.get('schema_name'))
    
    def _load_store_config(self) -> Dict:
        """Load store configuration from YAML file."""
        with open('stores.yaml', 'r') as f:
            config = yaml.safe_load(f)
            store_config = config.get('stores', {}).get(self.store_name)
            if not store_config:
                raise ValueError(f"Store '{self.store_name}' not found in stores.yaml")
            return store_config

# Create a default config instance
default_config = SnowflakeConfig.from_env() 