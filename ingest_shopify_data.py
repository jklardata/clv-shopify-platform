import os
import yaml
from dotenv import load_dotenv
import snowflake.connector
import logging
import shopify
from datetime import datetime, timedelta
import time
from typing import Dict, List, Any
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ShopifyDataIngestion:
    def __init__(self, store_id: str, store_config: Dict[str, Any], global_config: Dict[str, Any]):
        self.store_id = store_id
        self.store_config = store_config
        self.global_config = global_config
        self.snowflake_conn = None
        self.cursor = None
        self.setup_shopify_session()

    def setup_shopify_session(self):
        """Initialize Shopify API session."""
        try:
            shop_url = self.store_config['shopify']['shop_url']
            api_version = self.store_config['shopify'].get('api_version', '2024-01')
            access_token = os.getenv(self.store_config['shopify']['access_token'].replace('${', '').replace('}', ''))
            
            if not access_token:
                raise ValueError(f"Access token not found for store {self.store_id}")
            
            session = shopify.Session(shop_url, api_version, access_token)
            shopify.ShopifyResource.activate_session(session)
            logger.info(f"Successfully set up Shopify session for store {self.store_id}")
        except Exception as e:
            logger.error(f"Failed to setup Shopify session for store {self.store_id}: {str(e)}")
            raise

    def get_snowflake_connection(self):
        """Establish Snowflake connection."""
        try:
            load_dotenv()
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=self.store_config['snowflake']['warehouse'],
                database=self.store_config['snowflake']['database'],
                schema=self.store_config['snowflake']['schema'],
                role=os.getenv('SNOWFLAKE_ROLE')
            )
            self.snowflake_conn = conn
            self.cursor = conn.cursor()
            logger.info(f"Successfully connected to Snowflake for store {self.store_id}")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake for store {self.store_id}: {str(e)}")
            raise

    def fetch_customers(self, batch_size: int = 250) -> List[Dict]:
        """Fetch customers from Shopify."""
        customers = []
        try:
            page = 1
            while True:
                batch = shopify.Customer.find(limit=batch_size, page=page)
                if not batch:
                    break
                customers.extend([customer.to_dict() for customer in batch])
                page += 1
                time.sleep(0.5)  # Rate limiting
        except Exception as e:
            logger.error(f"Error fetching customers for store {self.store_id}: {str(e)}")
            raise
        return customers

    def fetch_orders(self, batch_size: int = 250) -> List[Dict]:
        """Fetch orders from Shopify."""
        orders = []
        try:
            page = 1
            while True:
                batch = shopify.Order.find(limit=batch_size, page=page)
                if not batch:
                    break
                orders.extend([order.to_dict() for order in batch])
                page += 1
                time.sleep(0.5)  # Rate limiting
        except Exception as e:
            logger.error(f"Error fetching orders for store {self.store_id}: {str(e)}")
            raise
        return orders

    def fetch_abandoned_checkouts(self, batch_size: int = 250) -> List[Dict]:
        """Fetch abandoned checkouts from Shopify."""
        checkouts = []
        try:
            page = 1
            while True:
                batch = shopify.Checkout.find(
                    limit=batch_size,
                    page=page,
                    status='any'
                )
                if not batch:
                    break
                checkouts.extend([checkout.to_dict() for checkout in batch])
                page += 1
                time.sleep(0.5)  # Rate limiting
        except Exception as e:
            logger.error(f"Error fetching abandoned checkouts for store {self.store_id}: {str(e)}")
            raise
        return checkouts

    def insert_customers(self, customers: List[Dict]):
        """Insert customers into Snowflake."""
        try:
            for customer in customers:
                self.cursor.execute("""
                    INSERT INTO customers (
                        customer_id, store_id, email, first_name, last_name,
                        orders_count, total_spent, created_at, updated_at,
                        accepts_marketing, verified_email, tax_exempt, tags
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(customer['id']), self.store_id, customer.get('email'),
                    customer.get('first_name'), customer.get('last_name'),
                    customer.get('orders_count'), customer.get('total_spent'),
                    customer.get('created_at'), customer.get('updated_at'),
                    customer.get('accepts_marketing'), customer.get('verified_email'),
                    customer.get('tax_exempt'), json.dumps(customer.get('tags', []))
                ))
            self.snowflake_conn.commit()
            logger.info(f"Successfully inserted {len(customers)} customers for store {self.store_id}")
        except Exception as e:
            logger.error(f"Error inserting customers for store {self.store_id}: {str(e)}")
            self.snowflake_conn.rollback()
            raise

    def insert_orders(self, orders: List[Dict]):
        """Insert orders and order items into Snowflake."""
        try:
            for order in orders:
                # Insert order
                self.cursor.execute("""
                    INSERT INTO orders (
                        order_id, store_id, customer_id, order_number,
                        total_price, subtotal_price, total_tax, total_discounts,
                        currency, financial_status, fulfillment_status,
                        created_at, updated_at, cancelled_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(order['id']), self.store_id, str(order.get('customer', {}).get('id')),
                    order.get('order_number'), order.get('total_price'),
                    order.get('subtotal_price'), order.get('total_tax'),
                    order.get('total_discounts'), order.get('currency'),
                    order.get('financial_status'), order.get('fulfillment_status'),
                    order.get('created_at'), order.get('updated_at'),
                    order.get('cancelled_at')
                ))

                # Insert order items
                for item in order.get('line_items', []):
                    self.cursor.execute("""
                        INSERT INTO order_items (
                            order_item_id, store_id, order_id, product_id,
                            variant_id, title, quantity, price, sku,
                            vendor, requires_shipping, taxable, name,
                            fulfillment_status, total_discount
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        str(item['id']), self.store_id, str(order['id']),
                        str(item.get('product_id')), str(item.get('variant_id')),
                        item.get('title'), item.get('quantity'), item.get('price'),
                        item.get('sku'), item.get('vendor'), item.get('requires_shipping'),
                        item.get('taxable'), item.get('name'),
                        item.get('fulfillment_status'), item.get('total_discount')
                    ))

            self.snowflake_conn.commit()
            logger.info(f"Successfully inserted orders and items for store {self.store_id}")
        except Exception as e:
            logger.error(f"Error inserting orders for store {self.store_id}: {str(e)}")
            self.snowflake_conn.rollback()
            raise

    def insert_abandoned_checkouts(self, checkouts: List[Dict]):
        """Insert abandoned checkouts into Snowflake."""
        try:
            for checkout in checkouts:
                self.cursor.execute("""
                    INSERT INTO abandoned_checkouts (
                        checkout_id, store_id, customer_id, email,
                        total_price, subtotal_price, total_tax,
                        total_discounts, currency, created_at,
                        updated_at, abandoned_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(checkout['id']), self.store_id,
                    str(checkout.get('customer', {}).get('id')),
                    checkout.get('email'), checkout.get('total_price'),
                    checkout.get('subtotal_price'), checkout.get('total_tax'),
                    checkout.get('total_discounts'), checkout.get('currency'),
                    checkout.get('created_at'), checkout.get('updated_at'),
                    checkout.get('abandoned_at')
                ))
            self.snowflake_conn.commit()
            logger.info(f"Successfully inserted {len(checkouts)} abandoned checkouts for store {self.store_id}")
        except Exception as e:
            logger.error(f"Error inserting abandoned checkouts for store {self.store_id}: {str(e)}")
            self.snowflake_conn.rollback()
            raise

    def process_store_data(self):
        """Process all data for a store."""
        try:
            self.get_snowflake_connection()
            
            # Fetch and insert customers
            customers = self.fetch_customers(self.store_config['snowflake'].get('batch_size', 250))
            self.insert_customers(customers)
            
            # Fetch and insert orders
            orders = self.fetch_orders(self.store_config['snowflake'].get('batch_size', 250))
            self.insert_orders(orders)
            
            # Fetch and insert abandoned checkouts
            checkouts = self.fetch_abandoned_checkouts(self.store_config['snowflake'].get('batch_size', 250))
            self.insert_abandoned_checkouts(checkouts)
            
            logger.info(f"Successfully processed all data for store {self.store_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to process data for store {self.store_id}: {str(e)}")
            return False
        finally:
            if self.cursor:
                self.cursor.close()
            if self.snowflake_conn:
                self.snowflake_conn.close()
            shopify.ShopifyResource.clear_session()

def load_config():
    """Load configuration from YAML file."""
    with open('config/stores.yaml', 'r') as f:
        return yaml.safe_load(f)

def process_stores():
    """Process all stores in parallel."""
    config = load_config()
    global_config = config['global']
    
    # Process stores in parallel
    with ThreadPoolExecutor(max_workers=config.get('global_settings', {}).get('max_concurrent_stores', 5)) as executor:
        future_to_store = {
            executor.submit(
                ShopifyDataIngestion(store_id, store_config, global_config).process_store_data
            ): store_id
            for store_id, store_config in config['stores'].items()
            if not store_id.startswith('#')  # Skip commented stores
        }
        
        for future in as_completed(future_to_store):
            store_id = future_to_store[future]
            try:
                success = future.result()
                if success:
                    logger.info(f"Completed processing store: {store_id}")
                else:
                    logger.warning(f"Failed to process store: {store_id}")
            except Exception as e:
                logger.error(f"Store {store_id} generated an exception: {str(e)}")

if __name__ == "__main__":
    process_stores() 