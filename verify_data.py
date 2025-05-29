import os
from dotenv import load_dotenv
import snowflake.connector
import logging
from tabulate import tabulate

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_verification_queries():
    """Run verification queries on the test data."""
    try:
        # Load environment variables
        load_dotenv()
        
        # Get Snowflake credentials
        snowflake_config = {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
            'database': os.getenv('SNOWFLAKE_DATABASE'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA'),
            'role': os.getenv('SNOWFLAKE_ROLE')
        }
        
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=snowflake_config['user'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            warehouse=snowflake_config['warehouse'],
            database=snowflake_config['database'],
            schema=snowflake_config['schema'],
            role=snowflake_config['role'],
            insecure_mode=True
        )
        
        cursor = conn.cursor()
        
        # Query 1: Count records in each table
        logger.info("\n=== Record Counts ===")
        count_query = """
        SELECT 'customers' as table_name, COUNT(*) as count FROM customers
        UNION ALL
        SELECT 'orders', COUNT(*) FROM orders
        UNION ALL
        SELECT 'order_items', COUNT(*) FROM order_items
        UNION ALL
        SELECT 'abandoned_checkouts', COUNT(*) FROM abandoned_checkouts;
        """
        cursor.execute(count_query)
        results = cursor.fetchall()
        print(tabulate(results, headers=['Table', 'Count'], tablefmt='psql'))
        
        # Query 2: Customer summary with orders and items
        logger.info("\n=== Customer Summary ===")
        customer_summary = """
        SELECT 
            c.customer_id,
            c.email,
            c.first_name,
            c.last_name,
            c.customer_state,
            COUNT(DISTINCT o.order_id) as order_count,
            COUNT(DISTINCT oi.order_item_id) as total_items,
            SUM(o.total_price) as total_spent
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY 1, 2, 3, 4, 5
        ORDER BY total_spent DESC;
        """
        cursor.execute(customer_summary)
        results = cursor.fetchall()
        print(tabulate(results, headers=['Customer ID', 'Email', 'First Name', 'Last Name', 
                                       'State', 'Orders', 'Items', 'Total Spent'], 
                      tablefmt='psql'))
        
        # Query 3: Order items summary
        logger.info("\n=== Popular Products ===")
        product_summary = """
        SELECT 
            oi.title,
            COUNT(*) as times_ordered,
            SUM(oi.quantity) as total_quantity,
            AVG(oi.price) as avg_price,
            SUM(oi.quantity * oi.price) as total_revenue
        FROM order_items oi
        GROUP BY 1
        ORDER BY total_revenue DESC
        LIMIT 5;
        """
        cursor.execute(product_summary)
        results = cursor.fetchall()
        print(tabulate(results, headers=['Product', 'Times Ordered', 'Total Quantity', 
                                       'Avg Price', 'Total Revenue'], 
                      tablefmt='psql'))
        
        # Query 4: Abandoned checkout analysis
        logger.info("\n=== Abandoned Checkouts ===")
        abandoned_summary = """
        SELECT 
            c.email,
            c.first_name,
            c.last_name,
            ac.total_price,
            ac.created_at,
            ac.abandoned_at,
            TIMESTAMPDIFF(minute, ac.created_at, ac.abandoned_at) as minutes_to_abandon
        FROM abandoned_checkouts ac
        JOIN customers c ON ac.customer_id = c.customer_id
        ORDER BY ac.total_price DESC;
        """
        cursor.execute(abandoned_summary)
        results = cursor.fetchall()
        print(tabulate(results, headers=['Email', 'First Name', 'Last Name', 'Cart Value', 
                                       'Created At', 'Abandoned At', 'Minutes to Abandon'], 
                      tablefmt='psql'))
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error verifying data: {str(e)}")
        raise

if __name__ == "__main__":
    run_verification_queries() 