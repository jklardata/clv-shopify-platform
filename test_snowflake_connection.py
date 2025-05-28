from dotenv import load_dotenv
import os
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine, text

def test_direct_connection():
    """Test connection using snowflake-connector-python"""
    print("Testing direct Snowflake connection...")
    try:
        # Load environment variables
        load_dotenv()
        
        # Create connection
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        
        # Test connection with a simple query
        cursor = conn.cursor()
        
        # Test warehouse
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        if warehouse:
            try:
                cursor.execute(f'USE WAREHOUSE {warehouse}')
                print(f"Successfully connected to warehouse: {warehouse}")
            except Exception as e:
                print(f"Warning: Could not use warehouse {warehouse}: {str(e)}")
        
        # Test database
        database = os.getenv('SNOWFLAKE_DATABASE')
        if database:
            try:
                cursor.execute(f'USE DATABASE {database}')
                print(f"Successfully connected to database: {database}")
            except Exception as e:
                print(f"Warning: Could not use database {database}: {str(e)}")
        
        # Test schema
        schema = os.getenv('SNOWFLAKE_SCHEMA')
        if database and schema:
            try:
                cursor.execute(f'USE SCHEMA {database}.{schema}')
                print(f"Successfully connected to schema: {schema}")
            except Exception as e:
                print(f"Warning: Could not use schema {schema}: {str(e)}")
        
        # Get current session info
        cursor.execute('SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()')
        result = cursor.fetchone()
        print("\nCurrent Session Information:")
        print(f"Current Warehouse: {result[0]}")
        print(f"Current Database: {result[1]}")
        print(f"Current Schema: {result[2]}")
        
        # Print environment variables for debugging
        print("\nEnvironment Variables:")
        print(f"SNOWFLAKE_ACCOUNT: {os.getenv('SNOWFLAKE_ACCOUNT')}")
        print(f"SNOWFLAKE_USER: {os.getenv('SNOWFLAKE_USER')}")
        print(f"SNOWFLAKE_WAREHOUSE: {os.getenv('SNOWFLAKE_WAREHOUSE')}")
        print(f"SNOWFLAKE_DATABASE: {os.getenv('SNOWFLAKE_DATABASE')}")
        print(f"SNOWFLAKE_SCHEMA: {os.getenv('SNOWFLAKE_SCHEMA')}")
        
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        
    finally:
        if 'conn' in locals():
            conn.close()

def test_sqlalchemy_connection():
    """Test connection using SQLAlchemy (used in our application)"""
    print("\nTesting SQLAlchemy connection...")
    try:
        # Create SQLAlchemy engine
        engine = create_engine(URL(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        ))
        
        # Test connection
        with engine.connect() as connection:
            result = connection.execute(text('SELECT CURRENT_TIMESTAMP() as time')).fetchone()
            print(f"Successfully connected via SQLAlchemy!")
            print(f"Current Timestamp: {result.time}")
            
    except Exception as e:
        print(f"Error connecting via SQLAlchemy: {str(e)}")

if __name__ == "__main__":
    print("Testing Snowflake Connections...")
    print("=" * 50)
    test_direct_connection()
    test_sqlalchemy_connection()
    print("=" * 50) 