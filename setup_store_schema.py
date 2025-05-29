import os
import yaml
from dotenv import load_dotenv
import snowflake.connector
import logging
import re
from typing import Dict, Any, Tuple
from snowflake.connector.errors import OperationalError, ProgrammingError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StoreSchemaSetup:
    def __init__(self):
        load_dotenv()
        self.warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'CLV_WAREHOUSE')
        self.database = os.getenv('SNOWFLAKE_DATABASE', 'CLV_ANALYTICS')
        self.role = os.getenv('SNOWFLAKE_ADMIN_ROLE', 'ACCOUNTADMIN')
        self.conn = None
        self.cursor = None

    def _parse_account_url(self, url: str) -> Dict[str, str]:
        """Parse a Snowflake URL into account components."""
        # Remove any whitespace and convert to lowercase
        url = url.strip().lower()
        
        # Handle app.snowflake.com format
        if 'app.snowflake.com' in url:
            # Extract org and account from URL
            # Format: app.snowflake.com/org/account/...
            parts = url.split('/')
            if len(parts) >= 4:
                org = parts[3]  # org identifier
                account = parts[4]  # account identifier
                return {
                    'organization': org,
                    'account': account,
                    'region': None
                }
        
        # Handle traditional format
        url = url.replace('https://', '').replace('http://', '')
        url = url.replace('.snowflakecomputing.com', '')
        parts = url.split('.')
        
        if '-' in parts[0]:
            org, account = parts[0].split('-', 1)
            region = parts[1] if len(parts) > 1 else None
            return {
                'organization': org,
                'account': account,
                'region': region
            }
        
        return {
            'organization': None,
            'account': parts[0],
            'region': parts[1] if len(parts) > 1 else None
        }

    def _format_account(self, account: str) -> Dict[str, str]:
        """Format the account identifier and region."""
        # Remove any whitespace and convert to lowercase
        account = account.strip().lower()
        
        # Get components from URL or account string
        if any(domain in account for domain in ['snowflakecomputing.com', 'app.snowflake.com']):
            components = self._parse_account_url(account)
        else:
            # Direct account identifier provided
            if '-' in account:
                org, acc = account.split('-', 1)
                components = {
                    'organization': org,
                    'account': acc,
                    'region': None
                }
            else:
                components = {
                    'organization': account,
                    'account': None,
                    'region': None
                }
        
        # Check for region in environment variable
        env_region = os.getenv('SNOWFLAKE_REGION', '').strip().lower()
        if env_region:
            # Fix common region format issues
            env_region = env_region.replace('_', '-')  # Replace underscore with hyphen
            components['region'] = env_region
        elif not components['region']:
            # Default to US-West-2 for app.snowflake.com
            components['region'] = 'us-west-2'
        
        return components

    def connect_to_snowflake(self):
        """Establish connection to Snowflake with admin privileges."""
        try:
            # Get connection parameters
            account = os.getenv('SNOWFLAKE_ACCOUNT')
            if not account:
                raise ValueError("""SNOWFLAKE_ACCOUNT environment variable is not set.
                    For app.snowflake.com URLs, use the format:
                    SNOWFLAKE_ACCOUNT=organization
                    Example: If your URL is app.snowflake.com/pipykkn/pvb40654,
                    set SNOWFLAKE_ACCOUNT=pipykkn""")
            
            # Format account identifier and get region
            try:
                components = self._format_account(account)
                account_id = components['organization'] or components['account']
                region = components['region']
                logger.info(f"Using Snowflake account: {account_id} in region: {region}")
            except ValueError as e:
                logger.error(f"Account format error: {str(e)}")
                logger.error(f"Current account value: {account}")
                logger.error("For app.snowflake.com URLs, use: organization")
                raise
            
            # Use admin credentials for setup
            user = os.getenv('SNOWFLAKE_ADMIN_USER') or os.getenv('SNOWFLAKE_USER')
            password = os.getenv('SNOWFLAKE_ADMIN_PASSWORD') or os.getenv('SNOWFLAKE_PASSWORD')
            role = os.getenv('SNOWFLAKE_ADMIN_ROLE', 'ACCOUNTADMIN')
            
            if not user or not password:
                raise ValueError("Missing admin credentials. Please set SNOWFLAKE_ADMIN_USER and SNOWFLAKE_ADMIN_PASSWORD")
            
            logger.info(f"Connecting as user: {user} with role: {role}")
            
            # For modern Snowflake accounts, we need to use the full account identifier
            account_identifier = account_id
            if region:
                account_identifier = f"{account_id}.{region}"
            
            logger.info(f"Using account identifier: {account_identifier}")
            
            # For app.snowflake.com accounts, we need to set the account URL format
            self.conn = snowflake.connector.connect(
                user=user,
                password=password,
                account=account_identifier,
                warehouse=self.warehouse,
                database=self.database,
                role=role,
                insecure_mode=False,
                ocsp_fail_open=True,
                validate_default_parameters=True,
                client_session_keep_alive=True,
                application='CLV_PLATFORM',
                protocol='https',
                timezone='UTC'
            )
            self.cursor = self.conn.cursor()
            
            # Test connection and print details
            self.cursor.execute("""
                SELECT 
                    CURRENT_ACCOUNT(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_ROLE(),
                    CURRENT_VERSION()
            """)
            result = self.cursor.fetchone()
            logger.info(f"""Successfully connected to Snowflake:
                Account: {result[0]}
                Warehouse: {result[1]}
                Database: {result[2]}
                Role: {result[3]}
                Version: {result[4]}
            """)
            
        except OperationalError as e:
            if "certificate" in str(e).lower():
                logger.error(f"Certificate validation error. Account: {account}")
                logger.error("For app.snowflake.com URLs:")
                logger.error("1. Use ONLY the organization part as the account identifier")
                logger.error("2. Example: If your URL is app.snowflake.com/pipykkn/pvb40654")
                logger.error("   Set SNOWFLAKE_ACCOUNT=pipykkn")
                logger.error("3. Make sure SNOWFLAKE_REGION is correct (e.g., us-west-2)")
                logger.error(f"Current region setting: {region}")
            elif "could not connect" in str(e).lower():
                logger.error(f"Connection failed. Account: {account}")
                logger.error("Please check:")
                logger.error("1. Your network connectivity")
                logger.error("2. VPN settings if applicable")
                logger.error("3. Account and region format")
                logger.error(f"   Current settings: account={account_identifier}")
                logger.error("4. Admin credentials (using SNOWFLAKE_ADMIN_USER)")
            else:
                logger.error(f"Operational error connecting to Snowflake: {str(e)}")
            raise
        except ValueError as e:
            logger.error(f"Configuration error: {str(e)}")
            logger.error("Please check your .env file for all required variables")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to Snowflake: {str(e)}")
            raise

    def setup_store_schema(self, store_id: str, store_config: Dict[str, Any]):
        """Set up schema and permissions for a new store."""
        try:
            schema_name = f"{store_id}_schema".upper()
            
            # Create schema if it doesn't exist
            self.cursor.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {self.database}.{schema_name}
            """)
            
            # Grant usage on schema to the CLV role
            self.cursor.execute(f"""
                GRANT USAGE ON SCHEMA {self.database}.{schema_name} TO ROLE {self.role}
            """)
            
            # Grant all privileges on future tables in schema to the CLV role
            self.cursor.execute(f"""
                GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA {self.database}.{schema_name} 
                TO ROLE {self.role}
            """)
            
            # Grant all privileges on future views in schema to the CLV role
            self.cursor.execute(f"""
                GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA {self.database}.{schema_name} 
                TO ROLE {self.role}
            """)

            # Create required tables
            self._create_store_tables(schema_name)
            
            logger.info(f"Successfully set up schema and permissions for store {store_id}")
            return schema_name
        except Exception as e:
            logger.error(f"Failed to set up schema for store {store_id}: {str(e)}")
            raise

    def _create_store_tables(self, schema_name: str):
        """Create the required tables for the store."""
        try:
            # Create customers table
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{schema_name}.customers (
                    customer_id VARCHAR NOT NULL,
                    store_id VARCHAR NOT NULL,
                    email VARCHAR,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    orders_count NUMBER,
                    total_spent NUMBER(38,2),
                    created_at TIMESTAMP_NTZ,
                    updated_at TIMESTAMP_NTZ,
                    accepts_marketing BOOLEAN,
                    verified_email BOOLEAN,
                    tax_exempt BOOLEAN,
                    tags VARIANT,
                    PRIMARY KEY (customer_id, store_id)
                )
            """)

            # Create orders table
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{schema_name}.orders (
                    order_id VARCHAR NOT NULL,
                    store_id VARCHAR NOT NULL,
                    customer_id VARCHAR,
                    order_number VARCHAR,
                    total_price NUMBER(38,2),
                    subtotal_price NUMBER(38,2),
                    total_tax NUMBER(38,2),
                    total_discounts NUMBER(38,2),
                    currency VARCHAR,
                    financial_status VARCHAR,
                    fulfillment_status VARCHAR,
                    created_at TIMESTAMP_NTZ,
                    updated_at TIMESTAMP_NTZ,
                    cancelled_at TIMESTAMP_NTZ,
                    PRIMARY KEY (order_id, store_id)
                )
            """)

            # Create order_items table
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{schema_name}.order_items (
                    order_item_id VARCHAR NOT NULL,
                    store_id VARCHAR NOT NULL,
                    order_id VARCHAR NOT NULL,
                    product_id VARCHAR,
                    variant_id VARCHAR,
                    title VARCHAR,
                    quantity NUMBER,
                    price NUMBER(38,2),
                    sku VARCHAR,
                    vendor VARCHAR,
                    requires_shipping BOOLEAN,
                    taxable BOOLEAN,
                    name VARCHAR,
                    fulfillment_status VARCHAR,
                    total_discount NUMBER(38,2),
                    PRIMARY KEY (order_item_id, store_id)
                )
            """)

            # Create abandoned_checkouts table
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.database}.{schema_name}.abandoned_checkouts (
                    checkout_id VARCHAR NOT NULL,
                    store_id VARCHAR NOT NULL,
                    customer_id VARCHAR,
                    email VARCHAR,
                    total_price NUMBER(38,2),
                    subtotal_price NUMBER(38,2),
                    total_tax NUMBER(38,2),
                    total_discounts NUMBER(38,2),
                    currency VARCHAR,
                    created_at TIMESTAMP_NTZ,
                    updated_at TIMESTAMP_NTZ,
                    abandoned_at TIMESTAMP_NTZ,
                    PRIMARY KEY (checkout_id, store_id)
                )
            """)

            logger.info(f"Successfully created tables in schema {schema_name}")
        except Exception as e:
            logger.error(f"Failed to create tables in schema {schema_name}: {str(e)}")
            raise

def setup_new_store(store_id: str, store_config: Dict[str, Any]):
    """Main function to set up a new store."""
    setup = StoreSchemaSetup()
    try:
        setup.connect_to_snowflake()
        schema_name = setup.setup_store_schema(store_id, store_config)
        
        # Update store config with schema name
        store_config['snowflake']['schema'] = schema_name
        store_config['snowflake']['warehouse'] = setup.warehouse
        store_config['snowflake']['database'] = setup.database
        
        return store_config
    finally:
        if setup.cursor:
            setup.cursor.close()
        if setup.conn:
            setup.conn.close()

if __name__ == "__main__":
    # Example usage
    store_config = {
        'name': 'New Test Store',
        'shopify': {
            'shop_url': 'new-test-store.myshopify.com',
            'api_version': '2024-01',
            'access_token': '${NEW_STORE_ACCESS_TOKEN}'
        },
        'snowflake': {}
    }
    
    updated_config = setup_new_store('new_test_store', store_config)
    print(f"Store setup complete. Updated config: {updated_config}") 