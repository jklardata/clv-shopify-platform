import os
from dotenv import load_dotenv
import snowflake.connector
import logging
import certifi
import urllib3

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def admin_setup():
    """Set up initial Snowflake database, schema, and permissions."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Configure SSL settings
        os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
        os.environ['SSL_CERT_FILE'] = certifi.where()
        urllib3.util.ssl_.DEFAULT_CERTS = certifi.where()
        
        # Get Snowflake credentials - use admin credentials from environment
        snowflake_config = {
            'user': os.getenv('SNOWFLAKE_ADMIN_USER'),
            'password': os.getenv('SNOWFLAKE_ADMIN_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'role': 'ACCOUNTADMIN'  # Must be ACCOUNTADMIN for initial setup
        }
        
        # Log configuration (excluding sensitive data)
        logger.info(f"Connecting to Snowflake account: {snowflake_config['account']}")
        logger.info(f"Using role: {snowflake_config['role']}")
        
        # Configure Snowflake connection
        conn = snowflake.connector.connect(
            user=snowflake_config['user'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            warehouse=snowflake_config['warehouse'],
            role=snowflake_config['role'],
            insecure_mode=True
        )
        
        cursor = conn.cursor()
        
        # Execute setup commands
        setup_commands = [
            # Create database
            "CREATE DATABASE IF NOT EXISTS CLV_ANALYTICS",
            
            # Create schema
            "USE DATABASE CLV_ANALYTICS",
            "CREATE SCHEMA IF NOT EXISTS ECOMM_TRANSACTIONS",
            
            # Create role if it doesn't exist
            "CREATE ROLE IF NOT EXISTS SHOPIFY_CLV_ROLE",
            
            # Grant warehouse access
            f"GRANT USAGE ON WAREHOUSE {snowflake_config['warehouse']} TO ROLE SHOPIFY_CLV_ROLE",
            
            # Grant database privileges
            "GRANT USAGE ON DATABASE CLV_ANALYTICS TO ROLE SHOPIFY_CLV_ROLE",
            
            # Grant schema privileges
            "GRANT USAGE ON SCHEMA CLV_ANALYTICS.ECOMM_TRANSACTIONS TO ROLE SHOPIFY_CLV_ROLE",
            "GRANT CREATE TABLE ON SCHEMA CLV_ANALYTICS.ECOMM_TRANSACTIONS TO ROLE SHOPIFY_CLV_ROLE",
            "GRANT MODIFY ON SCHEMA CLV_ANALYTICS.ECOMM_TRANSACTIONS TO ROLE SHOPIFY_CLV_ROLE",
            
            # Grant future privileges
            "GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA CLV_ANALYTICS.ECOMM_TRANSACTIONS TO ROLE SHOPIFY_CLV_ROLE",
            
            # Grant role to user if specified
            f"GRANT ROLE SHOPIFY_CLV_ROLE TO USER {os.getenv('SNOWFLAKE_USER')}"
        ]
        
        for command in setup_commands:
            try:
                logger.info(f"Executing: {command[:100]}...")  # Log first 100 chars
                cursor.execute(command)
                logger.info("Command executed successfully")
            except Exception as e:
                logger.error(f"Error executing command: {str(e)}")
                logger.error(f"Failed command: {command}")
                raise
        
        logger.info("Successfully completed admin setup")
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error in admin setup: {str(e)}")
        raise

if __name__ == "__main__":
    admin_setup() 