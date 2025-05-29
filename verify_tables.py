from config import StoreConfig
from sqlalchemy import text
from database import get_engine

def verify_tables(store_name: str):
    """Verify tables in the schema."""
    try:
        # Load store configuration
        store_config = StoreConfig(store_name)
        schema_name = store_config.snowflake.schema
        
        # Connect with ACCOUNTADMIN to have full visibility
        store_config.snowflake.role = 'ACCOUNTADMIN'
        engine = get_engine(store_config.snowflake.get_connection_url())
        
        print(f"\nVerifying tables in schema '{schema_name}'...")
        
        with engine.connect() as conn:
            # List all tables in the schema
            result = conn.execute(text(f"""
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}'
                ORDER BY table_name;
            """))
            
            tables = result.fetchall()
            
            if not tables:
                print(f"No tables found in schema '{schema_name}'")
            else:
                print(f"\nFound {len(tables)} tables:")
                for table in tables:
                    print(f"- {table.table_name} ({table.table_type})")
            
            # Also check for any errors in table creation
            result = conn.execute(text(f"""
                SELECT *
                FROM information_schema.tables
                WHERE table_schema = '{schema_name}'
                  AND table_type = 'BASE TABLE'
                  AND table_name IN ('customers', 'orders', 'order_items', 'abandoned_checkouts', 'returns');
            """))
            
            expected_tables = {'customers', 'orders', 'order_items', 'abandoned_checkouts', 'returns'}
            found_tables = {row.table_name for row in result}
            missing_tables = expected_tables - found_tables
            
            if missing_tables:
                print(f"\nMissing tables: {', '.join(missing_tables)}")
                
                # Check if schema exists
                result = conn.execute(text(f"""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name = '{schema_name}';
                """))
                if not result.fetchone():
                    print(f"\nSchema '{schema_name}' does not exist!")
                
                # Check privileges
                result = conn.execute(text(f"""
                    SHOW GRANTS TO ROLE SHOPIFY_CLV_ROLE;
                """))
                print("\nPrivileges for SHOPIFY_CLV_ROLE:")
                for row in result:
                    print(f"- {row.privilege} on {row.name}.{row.granted_on}")

    except Exception as e:
        print(f"Error verifying tables: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python verify_tables.py <store_name>")
        sys.exit(1)
    
    store_name = sys.argv[1]
    verify_tables(store_name) 