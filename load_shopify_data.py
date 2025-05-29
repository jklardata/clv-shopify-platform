import shopify
from config import StoreConfig
from models import Customer, Order, OrderLineItem, AbandonedCheckout, OrderReturn
from database import get_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
import os
import ssl
import certifi
import urllib3
import requests
from urllib3.util.ssl_ import create_urllib3_context
from sqlalchemy import text

def init_shopify_api(config):
    """Initialize Shopify API connection."""
    # Try multiple certificate paths
    cert_paths = [
        '/private/etc/ssl/cert.pem',  # macOS system certificates
        certifi.where(),              # certifi certificates
        '/Library/Frameworks/Python.framework/Versions/3.11/etc/openssl/cert.pem'  # Python 3.11 specific path
    ]
    
    # Find the first valid certificate path
    valid_cert = None
    for cert_path in cert_paths:
        if os.path.exists(cert_path):
            try:
                ssl_context = ssl.create_default_context(cafile=cert_path)
                ssl_context.verify_mode = ssl.CERT_REQUIRED
                valid_cert = cert_path
                break
            except Exception:
                continue
    
    if valid_cert is None:
        raise Exception("No valid SSL certificate found. Please run: /Applications/Python\\ 3.11/Install\\ Certificates.command")
    
    # Configure SSL for shopify-python-api
    os.environ['SSL_CERT_FILE'] = valid_cert
    shopify.ShopifyResource.verify_ssl = True
    
    shop_url = f"https://{config.shopify.api_key}:{config.shopify.access_token}@{config.shopify.shop_url}/admin/api/{config.shopify.api_version}"
    shopify.ShopifyResource.set_site(shop_url)

def load_customers(session):
    """Load customers from Shopify API using cursor-based pagination."""
    print("Loading customers...")
    since_id = 0
    
    while True:
        try:
            customers = shopify.Customer.find(since_id=since_id, limit=250)
            if not customers:
                break
                
            for shopify_customer in customers:
                # Get the raw customer data as a Python dict
                customer_data = json.loads(shopify_customer.to_json())
                customer_obj = customer_data.get('customer', {})
                
                if since_id == 0:  # Only print for first customer
                    print("Available customer fields:", customer_obj.keys())
                
                # Extract email marketing consent
                email_consent = customer_obj.get('email_marketing_consent', {})
                accepts_marketing = email_consent.get('state') == 'subscribed'
                
                # Create customer object with raw dict for custom_data
                customer = Customer(
                    id=str(customer_obj.get('id')),
                    email=customer_obj.get('email'),
                    first_name=customer_obj.get('first_name'),
                    last_name=customer_obj.get('last_name'),
                    total_spent=float(customer_obj.get('total_spent', 0) or 0),
                    orders_count=int(customer_obj.get('orders_count', 0) or 0),
                    accepts_marketing=accepts_marketing,
                    tax_exempt=customer_obj.get('tax_exempt', False),
                    tags=customer_obj.get('tags', ''),
                    created_at=datetime.fromisoformat(customer_obj.get('created_at').replace('Z', '+00:00')) if customer_obj.get('created_at') else None,
                    updated_at=datetime.fromisoformat(customer_obj.get('updated_at').replace('Z', '+00:00')) if customer_obj.get('updated_at') else None
                )
                
                # Use session.merge with autoflush disabled to prevent premature flushing
                with session.no_autoflush:
                    session.merge(customer)
            
            session.commit()
            print(f"Processed {len(customers)} customers since ID {since_id}")
            
            # Update since_id to the last customer's ID
            since_id = max(int(c.id) for c in customers)
            
        except Exception as e:
            print(f"Error processing customers: {str(e)}")
            if 'customer_data' in locals():
                print(f"Last customer data: {json.dumps(customer_data, indent=2)}")
            session.rollback()
            raise

def load_orders(session):
    """Load orders and order items from Shopify API using cursor-based pagination."""
    print("Loading orders...")
    since_id = 0
    
    while True:
        try:
            orders = shopify.Order.find(since_id=since_id, limit=250, status='any')
            if not orders:
                break
                
            for shopify_order in orders:
                # Get the raw order data
                order_data = json.loads(shopify_order.to_json())
                order_obj = order_data.get('order', {})
                
                if since_id == 0:  # Only print for first order
                    print("Available order fields:", order_obj.keys())
                
                order = Order(
                    id=str(order_obj.get('id')),
                    customer_id=str(order_obj['customer'].get('id')) if order_obj.get('customer') else None,
                    order_number=order_obj.get('name'),
                    total_price=float(order_obj.get('total_price', 0) or 0),
                    subtotal_price=float(order_obj.get('subtotal_price', 0) or 0),
                    total_tax=float(order_obj.get('total_tax', 0) or 0),
                    total_discounts=float(order_obj.get('total_discounts', 0) or 0),
                    currency=order_obj.get('currency'),
                    financial_status=order_obj.get('financial_status'),
                    fulfillment_status=order_obj.get('fulfillment_status'),
                    order_date=datetime.fromisoformat(order_obj.get('created_at').replace('Z', '+00:00')) if order_obj.get('created_at') else None,
                    cancelled_at=datetime.fromisoformat(order_obj.get('cancelled_at').replace('Z', '+00:00')) if order_obj.get('cancelled_at') else None,
                    cancel_reason=order_obj.get('cancel_reason'),
                    tags=order_obj.get('tags', '')
                )
                session.merge(order)
                
                # Process line items
                for item in order_obj.get('line_items', []):
                    if since_id == 0 and order_obj['line_items'].index(item) == 0:
                        print("Available line item fields:", item.keys())
                    
                    line_item = OrderLineItem(
                        id=str(item.get('id')),
                        order_id=str(order_obj.get('id')),
                        product_id=str(item.get('product_id')) if item.get('product_id') else None,
                        variant_id=str(item.get('variant_id')) if item.get('variant_id') else None,
                        title=item.get('title'),
                        quantity=int(item.get('quantity', 0)),
                        price=float(item.get('price', 0) or 0),
                        total_discount=float(item.get('total_discount', 0) or 0),
                        sku=item.get('sku'),
                        fulfillable_quantity=int(item.get('fulfillable_quantity', 0)),
                        fulfillment_status=item.get('fulfillment_status'),
                        gift_card=item.get('gift_card', False),
                        taxable=item.get('taxable', True)
                    )
                    session.merge(line_item)
            
            session.commit()
            print(f"Processed {len(orders)} orders since ID {since_id}")
            
            # Update since_id to the last order's ID
            since_id = max(int(o.id) for o in orders)
            
        except Exception as e:
            print(f"Error processing orders: {str(e)}")
            if 'order_data' in locals():
                print(f"Last order data: {json.dumps(order_data, indent=2)}")
            session.rollback()
            raise

def load_abandoned_checkouts(session):
    """Load abandoned checkouts from Shopify API using cursor-based pagination."""
    print("Loading abandoned checkouts...")
    since_id = 0
    
    while True:
        try:
            checkouts = shopify.Checkout.find(since_id=since_id, limit=250, status='any')
            if not checkouts:
                break
                
            for checkout in checkouts:
                if not checkout.completed_at:  # Only process abandoned checkouts
                    # Get the raw checkout data
                    checkout_data = json.loads(checkout.to_json())
                    checkout_obj = checkout_data.get('checkout', {})
                    
                    if since_id == 0:  # Only print for first checkout
                        print("Available checkout fields:", checkout_obj.keys())
                    
                    abandoned = AbandonedCheckout(
                        id=str(checkout_obj.get('id')),
                        customer_id=str(checkout_obj['customer'].get('id')) if checkout_obj.get('customer') else None,
                        email=checkout_obj.get('email'),
                        total_price=float(checkout_obj.get('total_price', 0) or 0),
                        subtotal_price=float(checkout_obj.get('subtotal_price', 0) or 0),
                        total_tax=float(checkout_obj.get('total_tax', 0) or 0),
                        total_discounts=float(checkout_obj.get('total_discounts', 0) or 0),
                        currency=checkout_obj.get('currency'),
                        completed_at=datetime.fromisoformat(checkout_obj.get('completed_at').replace('Z', '+00:00')) if checkout_obj.get('completed_at') else None,
                        abandoned_at=datetime.fromisoformat(checkout_obj.get('created_at').replace('Z', '+00:00')) if checkout_obj.get('created_at') else None,
                        recovery_url=checkout_obj.get('recovery_url')
                    )
                    session.merge(abandoned)
            
            session.commit()
            print(f"Processed {len(checkouts)} checkouts since ID {since_id}")
            
            # Update since_id to the last checkout's ID
            if checkouts:
                since_id = max(int(c.id) for c in checkouts)
                
        except Exception as e:
            print(f"Error processing checkouts: {str(e)}")
            if 'checkout_data' in locals():
                print(f"Last checkout data: {json.dumps(checkout_data, indent=2)}")
            session.rollback()
            raise

def main(store_name: str):
    """Main function to load all Shopify data."""
    try:
        # Load configuration
        store_config = StoreConfig(store_name)
        
        # Initialize Shopify API
        init_shopify_api(store_config)
        
        # Create database session
        engine = get_engine(store_config.snowflake.get_connection_url())
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Load all data
        load_customers(session)
        load_orders(session)
        load_abandoned_checkouts(session)
        
        session.close()
        print("Successfully loaded all Shopify data")
        
    except Exception as e:
        print(f"Error loading Shopify data: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python load_shopify_data.py <store_name>")
        sys.exit(1)
    
    store_name = sys.argv[1]
    main(store_name)

 