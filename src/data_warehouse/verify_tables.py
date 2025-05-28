from dotenv import load_dotenv
import os
from snowflake.connector import connect
from tabulate import tabulate

def verify_tables():
    """Verify the structure of all created tables in Snowflake."""
    load_dotenv()
    
    # Print connection details for debugging
    account = os.getenv('SNOWFLAKE_ACCOUNT')
    print(f"Attempting to connect to Snowflake account: {account}")
    
    # Add connection parameters to disable OCSP check
    conn = connect(
        account=account,
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        ocsp_response_cache_filename='/tmp/ocsp_response_cache',
        insecure_mode=True  # This bypasses certificate validation
    )
    
    cursor = conn.cursor()
    
    try:
        # List of tables to verify
        tables = [
            'customers',
            'orders',
            'order_items',
            'abandoned_checkouts',
            'refunds',
            'customer_metrics'
        ]
        
        for table in tables:
            print(f"\nDescribing table: {table}")
            print("=" * 50)
            
            # Get column information
            cursor.execute(f"DESCRIBE TABLE {table}")
            columns = cursor.fetchall()
            
            # Format column information
            column_info = [[col[0], col[1], col[3], col[4]] for col in columns]
            print(tabulate(column_info, headers=['Column', 'Type', 'Kind', 'Null?'], tablefmt='grid'))
            
            # Get primary key information
            cursor.execute(f"""
            SHOW PRIMARY KEYS IN TABLE {table}
            """)
            pks = cursor.fetchall()
            if pks:
                print("\nPrimary Keys:")
                pk_info = [[pk[4], pk[5]] for pk in pks]
                print(tabulate(pk_info, headers=['Name', 'Column'], tablefmt='grid'))
            
            # Get foreign key information
            cursor.execute(f"""
            SHOW IMPORTED KEYS IN TABLE {table}
            """)
            fks = cursor.fetchall()
            if fks:
                print("\nForeign Keys:")
                fk_info = [[fk[11], fk[7], fk[2], fk[3]] for fk in fks]
                print(tabulate(fk_info, headers=['Name', 'Column', 'Referenced Table', 'Referenced Column'], tablefmt='grid'))
            
            print("\n")
            
        # Verify table relationships
        print("Verifying table relationships...")
        print("=" * 50)
        cursor.execute("""
        SELECT 
            COUNT(*) as count,
            'customers' as table_name
        FROM customers
        UNION ALL
        SELECT 
            COUNT(*),
            'orders'
        FROM orders
        UNION ALL
        SELECT 
            COUNT(*),
            'order_items'
        FROM order_items
        UNION ALL
        SELECT 
            COUNT(*),
            'abandoned_checkouts'
        FROM abandoned_checkouts
        UNION ALL
        SELECT 
            COUNT(*),
            'refunds'
        FROM refunds
        UNION ALL
        SELECT 
            COUNT(*),
            'customer_metrics'
        FROM customer_metrics
        """)
        
        row_counts = cursor.fetchall()
        print("\nCurrent row counts:")
        print(tabulate(row_counts, headers=['Count', 'Table'], tablefmt='grid'))
            
    except Exception as e:
        print(f"Error verifying tables: {str(e)}")
        raise
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Verifying Snowflake table structures...")
    verify_tables()
    print("\nVerification complete!") 