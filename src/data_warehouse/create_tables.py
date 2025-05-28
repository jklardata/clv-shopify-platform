from dotenv import load_dotenv
import os
from snowflake.connector import connect

def create_tables():
    """Create the necessary tables in Snowflake for storing Shopify data."""
    load_dotenv()
    
    # Connect to Snowflake
    conn = connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    
    cursor = conn.cursor()
    
    try:
        # Create customers table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(255) PRIMARY KEY,
            email VARCHAR(255),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            orders_count INTEGER,
            total_spent FLOAT,
            currency VARCHAR(10),
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            last_order_date TIMESTAMP,
            first_order_date TIMESTAMP,
            accepts_marketing BOOLEAN,
            customer_locale VARCHAR(50)
        )
        """)
        
        # Create orders table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(255) PRIMARY KEY,
            customer_id VARCHAR(255),
            order_number INTEGER,
            order_status VARCHAR(50),
            total_price FLOAT,
            subtotal_price FLOAT,
            total_tax FLOAT,
            total_discounts FLOAT,
            currency VARCHAR(10),
            financial_status VARCHAR(50),
            fulfillment_status VARCHAR(50),
            payment_gateway VARCHAR(100),
            source_name VARCHAR(100),
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            cancelled_at TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
        """)
        
        # Create order_items table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id VARCHAR(255) PRIMARY KEY,
            order_id VARCHAR(255),
            product_id VARCHAR(255),
            variant_id VARCHAR(255),
            title VARCHAR(500),
            quantity INTEGER,
            price FLOAT,
            sku VARCHAR(255),
            vendor VARCHAR(255),
            requires_shipping BOOLEAN,
            taxable BOOLEAN,
            created_at TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        )
        """)
        
        # Create abandoned_checkouts table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS abandoned_checkouts (
            checkout_id VARCHAR(255) PRIMARY KEY,
            customer_id VARCHAR(255),
            email VARCHAR(255),
            total_price FLOAT,
            subtotal_price FLOAT,
            total_tax FLOAT,
            total_discounts FLOAT,
            currency VARCHAR(10),
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            abandoned_at TIMESTAMP,
            recovery_url VARCHAR(1000),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
        """)
        
        # Create refunds table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS refunds (
            refund_id VARCHAR(255) PRIMARY KEY,
            order_id VARCHAR(255),
            amount FLOAT,
            currency VARCHAR(10),
            reason VARCHAR(500),
            created_at TIMESTAMP,
            processed_at TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        )
        """)
        
        # Create customer_metrics table for CLV calculations
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_metrics (
            customer_id VARCHAR(255) PRIMARY KEY,
            total_orders INTEGER,
            total_spent FLOAT,
            average_order_value FLOAT,
            purchase_frequency FLOAT,
            customer_value FLOAT,
            acquisition_source VARCHAR(100),
            first_order_date TIMESTAMP,
            last_order_date TIMESTAMP,
            predicted_clv FLOAT,
            clv_confidence_score FLOAT,
            last_prediction_date TIMESTAMP,
            abandoned_checkouts_count INTEGER,
            refund_rate FLOAT,
            updated_at TIMESTAMP,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
        """)
        
        print("Successfully created all tables!")
        
    except Exception as e:
        print(f"Error creating tables: {str(e)}")
        raise
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("Creating Snowflake tables for Shopify data...")
    create_tables()
    print("Table creation process completed.") 