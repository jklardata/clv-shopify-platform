import os
from dotenv import load_dotenv
import snowflake.connector
import logging
from datetime import datetime, timedelta
import random
import uuid

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_test_data():
    """Generate sample test data for all tables."""
    
    # Generate 5 customers
    customers = []
    for i in range(5):
        customer_id = str(uuid.uuid4())
        created_at = datetime.now() - timedelta(days=random.randint(1, 365))
        customers.append({
            'customer_id': customer_id,
            'email': f'customer{i+1}@example.com',
            'first_name': f'FirstName{i+1}',
            'last_name': f'LastName{i+1}',
            'orders_count': random.randint(1, 5),
            'total_spent': round(random.uniform(100, 1000), 2),
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': (created_at + timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S'),
            'accepts_marketing': random.choice([True, False]),
            'customer_state': random.choice(['enabled', 'disabled', 'invited']),
            'verified_email': True,
            'tax_exempt': False,
            'tags': 'test_data,sample',
            'currency': 'USD',
            'country': 'United States',
            'province': 'California',
            'city': 'San Francisco',
            'zip': '94105'
        })

    # Generate 10 orders (2 orders per customer on average)
    orders = []
    for i in range(10):
        order_id = str(uuid.uuid4())
        customer = random.choice(customers)
        created_at = datetime.strptime(customer['created_at'], '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(1, 30))
        orders.append({
            'order_id': order_id,
            'customer_id': customer['customer_id'],
            'order_number': f'ORDER{i+1}',
            'total_price': round(random.uniform(50, 500), 2),
            'subtotal_price': round(random.uniform(45, 450), 2),
            'total_tax': round(random.uniform(5, 50), 2),
            'total_discounts': round(random.uniform(0, 25), 2),
            'currency': 'USD',
            'financial_status': random.choice(['paid', 'pending', 'refunded']),
            'fulfillment_status': random.choice(['fulfilled', 'partial', 'unfulfilled']),
            'processing_method': 'direct',
            'source_name': 'web',
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': (created_at + timedelta(hours=random.randint(1, 24))).strftime('%Y-%m-%d %H:%M:%S'),
            'processed_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'gateway': 'stripe',
            'test': False,
            'taxes_included': True,
            'total_weight': random.randint(100, 5000),
            'total_items': random.randint(1, 5)
        })

    # Generate 20 order items (2 items per order on average)
    order_items = []
    for i in range(20):
        order = random.choice(orders)
        created_at = datetime.strptime(order['created_at'], '%Y-%m-%d %H:%M:%S')
        order_items.append({
            'order_item_id': str(uuid.uuid4()),
            'order_id': order['order_id'],
            'product_id': str(uuid.uuid4()),
            'variant_id': str(uuid.uuid4()),
            'title': f'Test Product {i+1}',
            'quantity': random.randint(1, 3),
            'price': round(random.uniform(10, 100), 2),
            'sku': f'SKU{i+1}',
            'vendor': 'Test Vendor',
            'requires_shipping': True,
            'taxable': True,
            'name': f'Test Product {i+1} - Variant 1',
            'fulfillment_status': order['fulfillment_status'],
            'grams': random.randint(50, 1000),
            'total_discount': round(random.uniform(0, 10), 2),
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S')
        })

    # Generate 3 abandoned checkouts
    abandoned_checkouts = []
    for i in range(3):
        customer = random.choice(customers)
        created_at = datetime.strptime(customer['created_at'], '%Y-%m-%d %H:%M:%S') + timedelta(days=random.randint(1, 30))
        abandoned_checkouts.append({
            'checkout_id': str(uuid.uuid4()),
            'customer_id': customer['customer_id'],
            'email': customer['email'],
            'total_price': round(random.uniform(50, 500), 2),
            'subtotal_price': round(random.uniform(45, 450), 2),
            'total_tax': round(random.uniform(5, 50), 2),
            'total_discounts': round(random.uniform(0, 25), 2),
            'currency': 'USD',
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': (created_at + timedelta(minutes=random.randint(5, 60))).strftime('%Y-%m-%d %H:%M:%S'),
            'abandoned_at': (created_at + timedelta(minutes=random.randint(5, 60))).strftime('%Y-%m-%d %H:%M:%S'),
            'recovery_url': f'https://shop.example.com/cart/recover/{uuid.uuid4()}'
        })

    return customers, orders, order_items, abandoned_checkouts

def insert_test_data():
    """Insert test data into Snowflake tables."""
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
        
        # Generate test data
        customers, orders, order_items, abandoned_checkouts = generate_test_data()
        
        # Insert customers
        logger.info("Inserting customer data...")
        for customer in customers:
            insert_sql = """
            INSERT INTO customers (
                customer_id, email, first_name, last_name, orders_count, total_spent,
                created_at, updated_at, accepts_marketing, customer_state, verified_email,
                tax_exempt, tags, currency, country, province, city, zip
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.execute(insert_sql, (
                customer['customer_id'], customer['email'], customer['first_name'],
                customer['last_name'], customer['orders_count'], customer['total_spent'],
                customer['created_at'], customer['updated_at'], customer['accepts_marketing'],
                customer['customer_state'], customer['verified_email'], customer['tax_exempt'],
                customer['tags'], customer['currency'], customer['country'],
                customer['province'], customer['city'], customer['zip']
            ))
        
        # Insert orders
        logger.info("Inserting order data...")
        for order in orders:
            insert_sql = """
            INSERT INTO orders (
                order_id, customer_id, order_number, total_price, subtotal_price,
                total_tax, total_discounts, currency, financial_status,
                fulfillment_status, processing_method, source_name, created_at,
                updated_at, processed_at, gateway, test, taxes_included,
                total_weight, total_items
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.execute(insert_sql, (
                order['order_id'], order['customer_id'], order['order_number'],
                order['total_price'], order['subtotal_price'], order['total_tax'],
                order['total_discounts'], order['currency'], order['financial_status'],
                order['fulfillment_status'], order['processing_method'], order['source_name'],
                order['created_at'], order['updated_at'], order['processed_at'],
                order['gateway'], order['test'], order['taxes_included'],
                order['total_weight'], order['total_items']
            ))
        
        # Insert order items
        logger.info("Inserting order item data...")
        for item in order_items:
            insert_sql = """
            INSERT INTO order_items (
                order_item_id, order_id, product_id, variant_id, title, quantity,
                price, sku, vendor, requires_shipping, taxable, name,
                fulfillment_status, grams, total_discount, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.execute(insert_sql, (
                item['order_item_id'], item['order_id'], item['product_id'],
                item['variant_id'], item['title'], item['quantity'], item['price'],
                item['sku'], item['vendor'], item['requires_shipping'], item['taxable'],
                item['name'], item['fulfillment_status'], item['grams'],
                item['total_discount'], item['created_at']
            ))
        
        # Insert abandoned checkouts
        logger.info("Inserting abandoned checkout data...")
        for checkout in abandoned_checkouts:
            insert_sql = """
            INSERT INTO abandoned_checkouts (
                checkout_id, customer_id, email, total_price, subtotal_price,
                total_tax, total_discounts, currency, created_at, updated_at,
                abandoned_at, recovery_url
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            cursor.execute(insert_sql, (
                checkout['checkout_id'], checkout['customer_id'], checkout['email'],
                checkout['total_price'], checkout['subtotal_price'], checkout['total_tax'],
                checkout['total_discounts'], checkout['currency'], checkout['created_at'],
                checkout['updated_at'], checkout['abandoned_at'], checkout['recovery_url']
            ))
        
        # Commit all changes
        conn.commit()
        logger.info("Successfully inserted all test data")
        
        # Close connection
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error inserting test data: {str(e)}")
        raise

if __name__ == "__main__":
    insert_test_data() 