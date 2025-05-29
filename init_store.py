from sqlalchemy import create_engine, text
from config import StoreConfig
from models import Base
from database import get_engine
import sys

def init_store_schema(store_name: str):
    """Initialize store schema and create all tables."""
    try:
        # Load store configuration
        store_config = StoreConfig(store_name)
        schema_name = store_config.snowflake.schema
        database = store_config.snowflake.database
        
        print(f"Connecting to Snowflake...")
        print(f"Account: {store_config.snowflake.account}")
        print(f"User: {store_config.snowflake.user}")
        print(f"Database: {database}")
        print(f"Warehouse: {store_config.snowflake.warehouse}")
        print(f"Initial Role: ACCOUNTADMIN (for schema creation)")
        print(f"Schema: {schema_name}")
        
        # First connect as ACCOUNTADMIN to create schema
        base_config = store_config.snowflake
        base_config.schema = None
        base_config.role = 'ACCOUNTADMIN'
        engine = get_engine(base_config.get_connection_url())
        
        print(f"\nCreating schema '{schema_name}'...")
        
        # Create schema and grant privileges
        with engine.connect() as conn:
            # Create database if it doesn't exist
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {database}"))
            conn.execute(text(f"USE DATABASE {database}"))
            
            # Create schema if it doesn't exist
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.execute(text(f"USE SCHEMA {schema_name}"))
            
            # Grant privileges to SHOPIFY_CLV_ROLE
            conn.execute(text(f"GRANT ALL PRIVILEGES ON DATABASE {database} TO ROLE SHOPIFY_CLV_ROLE"))
            conn.execute(text(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_name} TO ROLE SHOPIFY_CLV_ROLE"))
            conn.execute(text(f"GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema_name} TO ROLE SHOPIFY_CLV_ROLE"))
            conn.execute(text(f"GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA {schema_name} TO ROLE SHOPIFY_CLV_ROLE"))
            
            # Verify schema creation
            result = conn.execute(text(f"""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name = '{schema_name}'
            """))
            if not result.fetchone():
                raise Exception(f"Failed to create schema '{schema_name}'")
                
            conn.commit()
        
        print(f"Switching to SHOPIFY_CLV_ROLE...")
        
        # Now connect with SHOPIFY_CLV_ROLE to create tables
        store_config.snowflake.role = 'SHOPIFY_CLV_ROLE'
        engine = get_engine(store_config.snowflake.get_connection_url())
        
        print(f"Creating tables in schema '{schema_name}'...")
        
        # Create all tables in the schema
        Base.metadata.create_all(engine)
        
        # Verify table creation
        with engine.connect() as conn:
            for table in ['customers', 'orders', 'order_items', 'abandoned_checkouts', 'returns']:
                result = conn.execute(text(f"""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = '{schema_name}' 
                    AND table_name = '{table.upper()}'
                """))
                if not result.fetchone():
                    raise Exception(f"Failed to create table '{table}'")
        
        print(f"Successfully initialized schema '{schema_name}' and created all tables for store '{store_name}'")
        
    except Exception as e:
        print(f"Error initializing store schema: {str(e)}")
        raise

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python init_store.py <store_name>")
        sys.exit(1)
    
    store_name = sys.argv[1]
    init_store_schema(store_name) 