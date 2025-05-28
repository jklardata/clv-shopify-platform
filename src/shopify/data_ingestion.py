from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import shopify
from snowflake.connector import connect
import pandas as pd
from typing import List, Dict, Optional
import logging
import ssl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Snowflake logging
snowflake_logger = logging.getLogger('snowflake.connector')
snowflake_logger.setLevel(logging.WARNING)

class ShopifyDataIngestion:
    def __init__(self):
        load_dotenv()
        
        # Configure Snowflake connection parameters
        os.environ['SNOWFLAKE_PYTHON_CONNECTOR_OCSP_MODE'] = 'INSECURE'
        os.environ['REQUESTS_CA_BUNDLE'] = '/etc/ssl/cert.pem'  # Standard macOS CA bundle location
        
        # Get Snowflake role from environment
        snowflake_role = os.getenv('SNOWFLAKE_ROLE')
        if not snowflake_role:
            raise ValueError("SNOWFLAKE_ROLE environment variable is not set")
        
        # Initialize Snowflake connection with SSL configuration
        self.snowflake_conn = connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            role=snowflake_role,
            ocsp_response_cache_filename='/tmp/ocsp_response_cache',
            ssl_verify_certificate=False,
            insecure_mode=True,
            validate_default_parameters=True,
            client_session_keep_alive=True,
            application='ShopifyCLV'
        )
        
        logger.info(f"Connected to Snowflake with role: {snowflake_role}")
        
        # Initialize Shopify connection
        shop_url = os.getenv('SHOPIFY_SHOP_NAME')  # Using SHOPIFY_SHOP_NAME directly
        api_version = '2024-01'  # Using a recent stable version
        access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
        
        if not shop_url:
            raise ValueError("SHOPIFY_SHOP_NAME environment variable is not set")
        if not access_token:
            raise ValueError("SHOPIFY_ACCESS_TOKEN environment variable is not set")
            
        # Ensure shop_url has the full myshopify.com domain
        if not shop_url.endswith('.myshopify.com'):
            shop_url = f"https://{shop_url}.myshopify.com"
        elif not shop_url.startswith('https://'):
            shop_url = f"https://{shop_url}"
        
        # Configure Shopify API
        shopify.Session.setup(api_key=access_token, secret=None)
        session = shopify.Session(shop_url, api_version, access_token)
        shopify.ShopifyResource.activate_session(session)
        
        logger.info(f"Successfully initialized connections to Snowflake and Shopify ({shop_url})")
    
    def __del__(self):
        """Cleanup connections"""
        if hasattr(self, 'snowflake_conn'):
            self.snowflake_conn.close()
        shopify.ShopifyResource.clear_session()
    
    def fetch_customers(self, days_back: int = 30) -> List[Dict]:
        """Fetch customers from Shopify."""
        print("Fetching customers...")
        created_at_min = datetime.now() - timedelta(days=days_back)
        
        customers = []
        since_id = None
        
        while True:
            params = {
                'created_at_min': created_at_min.isoformat(),
                'limit': 250
            }
            if since_id:
                params['since_id'] = since_id
            
            batch = shopify.Customer.find(**params)
            
            if not batch:
                break
            
            for customer in batch:
                # Get customer data with safe attribute access
                customer_data = {
                    'customer_id': str(customer.id),
                    'email': getattr(customer, 'email', None),
                    'first_name': getattr(customer, 'first_name', None),
                    'last_name': getattr(customer, 'last_name', None),
                    'orders_count': getattr(customer, 'orders_count', 0),
                    'total_spent': float(getattr(customer, 'total_spent', 0)),
                    'created_at': getattr(customer, 'created_at', None),
                    'updated_at': getattr(customer, 'updated_at', None),
                    'accepts_marketing': getattr(customer, 'accepts_marketing', False),
                    'state': getattr(customer, 'state', 'enabled'),
                    'last_order_id': str(getattr(customer, 'last_order_id', None)) if getattr(customer, 'last_order_id', None) else None,
                    'note': getattr(customer, 'note', None),
                    'verified_email': getattr(customer, 'verified_email', False),
                    'tax_exempt': getattr(customer, 'tax_exempt', False),
                    'tags': getattr(customer, 'tags', ''),
                    'currency': 'USD'  # Default currency
                }
                
                # Safely get country code from default address if it exists
                try:
                    default_address = customer.default_address
                    if default_address:
                        customer_data.update({
                            'country': getattr(default_address, 'country', None),
                            'province': getattr(default_address, 'province', None),
                            'city': getattr(default_address, 'city', None),
                            'zip': getattr(default_address, 'zip', None)
                        })
                except AttributeError:
                    customer_data.update({
                        'country': None,
                        'province': None,
                        'city': None,
                        'zip': None
                    })
                
                customers.append(customer_data)
            
            # Update since_id for next iteration
            if batch:
                since_id = batch[-1].id
            else:
                break
            
            logger.info(f"Fetched {len(customers)} customers so far...")
        
        return customers
    
    def fetch_orders(self, days_back: int = 30) -> List[Dict]:
        """Fetch orders from Shopify."""
        print("Fetching orders...")
        created_at_min = datetime.now() - timedelta(days=days_back)
        
        orders = []
        order_items = []
        since_id = None
        
        while True:
            params = {
                'created_at_min': created_at_min.isoformat(),
                'limit': 250,
                'status': 'any'
            }
            if since_id:
                params['since_id'] = since_id
            
            batch = shopify.Order.find(**params)
            
            if not batch:
                break
            
            for order in batch:
                # Get order data with safe attribute access
                order_data = {
                    'order_id': str(order.id),
                    'customer_id': str(order.customer.id) if order.customer else None,
                    'order_number': getattr(order, 'order_number', None),
                    'total_price': float(getattr(order, 'total_price', 0)),
                    'subtotal_price': float(getattr(order, 'subtotal_price', 0)),
                    'total_tax': float(getattr(order, 'total_tax', 0)),
                    'total_discounts': float(getattr(order, 'total_discounts', 0)),
                    'currency': getattr(order, 'currency', 'USD'),
                    'financial_status': getattr(order, 'financial_status', None),
                    'fulfillment_status': getattr(order, 'fulfillment_status', None),
                    'processing_method': getattr(order, 'processing_method', None),
                    'source_name': getattr(order, 'source_name', None),
                    'created_at': getattr(order, 'created_at', None),
                    'updated_at': getattr(order, 'updated_at', None),
                    'cancelled_at': getattr(order, 'cancelled_at', None),
                    'closed_at': getattr(order, 'closed_at', None),
                    'processed_at': getattr(order, 'processed_at', None),
                    'gateway': getattr(order, 'gateway', None),
                    'test': getattr(order, 'test', False),
                    'taxes_included': getattr(order, 'taxes_included', False),
                    'total_weight': getattr(order, 'total_weight', 0),
                    'total_items': len(getattr(order, 'line_items', [])),
                    'tags': getattr(order, 'tags', '')
                }
                
                # Add shipping address details if available
                try:
                    shipping_address = order.shipping_address
                    if shipping_address:
                        order_data.update({
                            'shipping_name': getattr(shipping_address, 'name', None),
                            'shipping_address1': getattr(shipping_address, 'address1', None),
                            'shipping_city': getattr(shipping_address, 'city', None),
                            'shipping_province': getattr(shipping_address, 'province', None),
                            'shipping_country': getattr(shipping_address, 'country', None),
                            'shipping_zip': getattr(shipping_address, 'zip', None)
                        })
                except AttributeError:
                    pass
                
                orders.append(order_data)
                
                # Process line items
                for item in getattr(order, 'line_items', []):
                    order_items.append({
                        'order_item_id': str(item.id),
                        'order_id': str(order.id),
                        'product_id': str(getattr(item, 'product_id', None)) if getattr(item, 'product_id', None) else None,
                        'variant_id': str(getattr(item, 'variant_id', None)) if getattr(item, 'variant_id', None) else None,
                        'title': getattr(item, 'title', None),
                        'quantity': getattr(item, 'quantity', 0),
                        'price': float(getattr(item, 'price', 0)),
                        'sku': getattr(item, 'sku', None),
                        'vendor': getattr(item, 'vendor', None),
                        'requires_shipping': getattr(item, 'requires_shipping', False),
                        'taxable': getattr(item, 'taxable', False),
                        'name': getattr(item, 'name', None),
                        'fulfillment_status': getattr(item, 'fulfillment_status', None),
                        'grams': getattr(item, 'grams', 0),
                        'total_discount': float(getattr(item, 'total_discount', 0)),
                        'created_at': order_data['created_at']
                    })
            
            # Update since_id for next iteration
            if batch:
                since_id = batch[-1].id
            else:
                break
            
            logger.info(f"Fetched {len(orders)} orders and {len(order_items)} order items so far...")
        
        return orders, order_items
    
    def fetch_abandoned_checkouts(self, days_back: int = 30) -> List[Dict]:
        """Fetch abandoned checkouts from Shopify."""
        print("Fetching abandoned checkouts...")
        created_at_min = datetime.now() - timedelta(days=days_back)
        
        checkouts = []
        since_id = None
        
        while True:
            params = {
                'created_at_min': created_at_min.isoformat(),
                'limit': 250,
                'status': 'any'
            }
            if since_id:
                params['since_id'] = since_id
            
            batch = shopify.Checkout.find(**params)
            
            if not batch:
                break
            
            for checkout in batch:
                checkouts.append({
                    'checkout_id': str(checkout.id),
                    'customer_id': str(checkout.customer.id) if checkout.customer else None,
                    'email': checkout.email,
                    'total_price': float(checkout.total_price),
                    'subtotal_price': float(checkout.subtotal_price),
                    'total_tax': float(checkout.total_tax),
                    'total_discounts': float(checkout.total_discounts),
                    'currency': checkout.currency,
                    'created_at': checkout.created_at,
                    'updated_at': checkout.updated_at,
                    'abandoned_at': checkout.created_at,  # Using created_at as abandoned_at
                    'recovery_url': checkout.recovery_url
                })
            
            # Update since_id for next iteration
            if batch:
                since_id = batch[-1].id
            else:
                break
            
            logger.info(f"Fetched {len(checkouts)} abandoned checkouts so far...")
        
        return checkouts
    
    def verify_table_columns(self, table_name: str) -> List[str]:
        """Verify and return the columns of a Snowflake table."""
        cursor = self.snowflake_conn.cursor()
        try:
            cursor.execute(f"DESC TABLE {table_name}")
            columns = [row[0].lower() for row in cursor.fetchall()]
            logger.info(f"Table {table_name} columns: {', '.join(columns)}")
            return columns
        finally:
            cursor.close()

    def load_to_snowflake(self, data: List[Dict], table_name: str):
        """Load data into Snowflake table."""
        if not data:
            print(f"No data to load for table: {table_name}")
            return
        
        print(f"Loading data into {table_name}...")
        
        # Verify table columns and filter DataFrame columns accordingly
        table_columns = self.verify_table_columns(table_name)
        df = pd.DataFrame(data)
        
        # Make all column names lowercase to match Snowflake
        df.columns = df.columns.str.lower()
        
        # Only keep columns that exist in the table
        df = df[[col for col in df.columns if col in table_columns]]
        
        # Convert timestamps to string format
        timestamp_columns = df.select_dtypes(include=['datetime64[ns]']).columns
        for col in timestamp_columns:
            df[col] = df[col].astype(str)
        
        # Remove duplicates based on ID column
        id_column = f"{table_name[:-1]}_id"
        df = df.drop_duplicates(subset=[id_column], keep='last')
        
        # Create temporary table for upserting
        temp_table = f"TEMP_{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        cursor = self.snowflake_conn.cursor()
        try:
            # Create temporary table
            create_temp_table = f"CREATE TEMPORARY TABLE {temp_table} LIKE {table_name}"
            cursor.execute(create_temp_table)
            
            # Convert DataFrame to list of tuples for insertion
            columns = df.columns.tolist()
            values = [tuple(x) for x in df.values]
            
            # Generate placeholders for the INSERT statement
            placeholders = ','.join(['%s'] * len(columns))
            
            # Prepare INSERT statement
            insert_sql = f"INSERT INTO {temp_table} ({','.join(columns)}) VALUES ({placeholders})"
            
            # Execute batch insert
            cursor.executemany(insert_sql, values)
            
            # Determine ordering column for deduplication
            ordering_column = 'updated_at' if 'updated_at' in columns else 'created_at'
            if ordering_column not in columns:
                # If neither updated_at nor created_at exists, just use the first non-id column
                ordering_column = next(col for col in columns if col != id_column)
            
            # Perform upsert using MERGE with explicit column handling
            set_clause = ', '.join(
                f't.{col} = s.{col}' 
                for col in columns 
                if col != id_column
            )
            
            merge_sql = f"""
            MERGE INTO {table_name} t
            USING (
                SELECT *,
                ROW_NUMBER() OVER (PARTITION BY {id_column} ORDER BY {ordering_column} DESC NULLS LAST) as rn
                FROM {temp_table}
            ) s
            ON t.{id_column} = s.{id_column}
            AND s.rn = 1
            WHEN MATCHED THEN
                UPDATE SET {set_clause}
            WHEN NOT MATCHED AND s.rn = 1 THEN
                INSERT ({','.join(columns)})
                VALUES ({','.join(f's.{col}' for col in columns)})
            """
            
            cursor.execute(merge_sql)
            
            # Commit the transaction
            self.snowflake_conn.commit()
            
            print(f"Successfully loaded {len(df)} rows into {table_name}")
            
        except Exception as e:
            print(f"Error loading data into {table_name}: {str(e)}")
            raise
        
        finally:
            # Clean up temporary table
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
            except:
                pass
            cursor.close()
    
    def sync_data(self, days_back: int = 30):
        """Sync all data from Shopify to Snowflake."""
        try:
            # Fetch and load customers
            customers = self.fetch_customers(days_back)
            self.load_to_snowflake(customers, 'customers')
            
            # Fetch and load orders and order items
            orders, order_items = self.fetch_orders(days_back)
            self.load_to_snowflake(orders, 'orders')
            self.load_to_snowflake(order_items, 'order_items')
            
            # Fetch and load abandoned checkouts
            checkouts = self.fetch_abandoned_checkouts(days_back)
            self.load_to_snowflake(checkouts, 'abandoned_checkouts')
            
            print("Data sync completed successfully!")
            
        except Exception as e:
            print(f"Error during data sync: {str(e)}")
            raise
        
        finally:
            self.snowflake_conn.close()

if __name__ == "__main__":
    try:
        ingestion = ShopifyDataIngestion()
        logger.info("Starting data sync process...")
        ingestion.sync_data(days_back=30)  # Sync last 30 days of data
        logger.info("Data ingestion completed successfully")
    except Exception as e:
        logger.error(f"Error during data ingestion: {str(e)}")
        raise 