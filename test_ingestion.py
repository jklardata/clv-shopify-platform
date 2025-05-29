from src.shopify.multi_store_ingestion import MultiStoreIngestion
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def verify_env_variables():
    """Verify that all required environment variables are set."""
    required_vars = {
        'Snowflake': [
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER',
            'SNOWFLAKE_PASSWORD',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA',
            'SNOWFLAKE_ROLE'
        ],
        'Shopify': [
            'SHOPIFY_SHOP_NAME',
            'SHOPIFY_ACCESS_TOKEN',
            'SHOPIFY_API_VERSION'
        ]
    }
    
    missing_vars = []
    for category, vars in required_vars.items():
        for var in vars:
            if not os.getenv(var):
                missing_vars.append(var)
                
    if missing_vars:
        logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
        return False
        
    # Log successful configuration (without sensitive data)
    logger.info(f"Snowflake Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    logger.info(f"Snowflake User: {os.getenv('SNOWFLAKE_USER')}")
    logger.info(f"Snowflake Database: {os.getenv('SNOWFLAKE_DATABASE')}")
    logger.info(f"Shopify Shop: {os.getenv('SHOPIFY_SHOP_NAME')}")
    
    return True

def main():
    try:
        # Load environment variables
        env_path = Path('.env')
        if not env_path.exists():
            logger.error(f"No .env file found at {env_path.absolute()}")
            return
            
        load_dotenv(verbose=True)
        logger.info("Loaded .env file")
        
        # Verify environment variables
        if not verify_env_variables():
            logger.error("Environment verification failed")
            return
            
        # Initialize the ingestion
        logger.info("Initializing multi-store ingestion...")
        ingestion = MultiStoreIngestion()

        # Run ingestion for the clv-test-store
        logger.info("Starting data ingestion for clv-test-store...")
        results = ingestion.ingest_stores(['clv_test_store'])

        # Check results
        for store_id, success in results.items():
            if success:
                logger.info(f"Successfully ingested data for store: {store_id}")
                
                # Get store status and metrics
                status = ingestion.get_store_status(store_id)
                logger.info(f"Store status: {status}")
            else:
                logger.error(f"Failed to ingest data for store: {store_id}")

    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}")
        raise

if __name__ == "__main__":
    main() 