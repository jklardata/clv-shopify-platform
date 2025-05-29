import os
import yaml
from dotenv import load_dotenv
import snowflake.connector
import logging
import certifi
import urllib3

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_store_config():
    """Load store configuration from YAML file."""
    with open('config/stores.yaml', 'r') as f:
        return yaml.safe_load(f)

def get_snowflake_connection(config):
    """Create a Snowflake connection using the provided configuration."""
    load_dotenv()
    
    # Configure SSL settings
    os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
    os.environ['SSL_CERT_FILE'] = certifi.where()
    urllib3.util.ssl_.DEFAULT_CERTS = certifi.where()
    
    # Get Snowflake credentials from environment variables
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=config['warehouse'],
        database=config['database'],
        role=os.getenv('SNOWFLAKE_ROLE'),
        ocsp_response_cache_filename='/tmp/ocsp_response_cache',
        insecure_mode=True,
        validate_default_parameters=True,
        client_session_keep_alive=True,
        application='ShopifyCLV'
    )
    return conn

def create_store_schema(cursor, store_id, store_config):
    """Create a schema for a specific store."""
    schema_name = store_config['snowflake']['schema']
    logger.info(f"Setting up schema {schema_name} for store {store_id}")
    
    try:
        # Try to create schema if we have permissions
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        logger.info(f"Successfully created schema {schema_name}")
    except Exception as e:
        logger.warning(f"Could not create schema {schema_name}: {str(e)}")
        logger.info("Attempting to use existing schema...")
    
    try:
        # Use the schema
        cursor.execute(f"USE SCHEMA {schema_name}")
        logger.info(f"Successfully using schema {schema_name}")
    except Exception as e:
        logger.error(f"Could not use schema {schema_name}: {str(e)}")
        raise
    
    # Read and execute table creation SQL
    with open('sql/create_tables.sql', 'r') as f:
        sql_template = f.read()
    
    # Split script into individual statements and execute each one
    statements = [stmt.strip() for stmt in sql_template.split(';') if stmt.strip()]
    
    for statement in statements:
        if statement and not statement.isspace():
            try:
                # Skip database creation statement
                if 'CREATE DATABASE' in statement.upper():
                    continue
                    
                # Skip drop table statements initially
                if 'DROP TABLE' in statement.upper():
                    continue
                    
                logger.info(f"Executing: {statement[:100]}...")  # Log first 100 chars
                cursor.execute(statement)
                logger.info("Statement executed successfully")
            except Exception as e:
                if 'already exists' in str(e).lower():
                    logger.info(f"Table already exists, continuing...")
                else:
                    logger.error(f"Error executing statement: {str(e)}")
                    logger.error(f"Failed statement: {statement[:200]}...")
                    raise

def setup_snowflake():
    """Set up Snowflake schemas and tables for all configured stores."""
    try:
        # Load store configuration
        config = load_store_config()
        global_config = config['global']['snowflake']
        
        # Create connection
        conn = get_snowflake_connection(global_config)
        cursor = conn.cursor()
        
        try:
            # Use existing database
            cursor.execute(f"USE DATABASE {global_config['database']}")
            logger.info(f"Successfully using database {global_config['database']}")
        except Exception as e:
            logger.error(f"Could not use database {global_config['database']}: {str(e)}")
            raise
        
        # Process each store
        for store_id, store_config in config['stores'].items():
            try:
                logger.info(f"Processing store: {store_id}")
                create_store_schema(cursor, store_id, store_config)
                logger.info(f"Successfully set up schema for store: {store_id}")
            except Exception as e:
                logger.error(f"Error setting up store {store_id}: {str(e)}")
                # Continue with next store even if one fails
                continue
        
        cursor.close()
        conn.close()
        logger.info("Snowflake setup completed successfully")
        
    except Exception as e:
        logger.error(f"Error in Snowflake setup: {str(e)}")
        raise

if __name__ == "__main__":
    setup_snowflake() 