import os
from typing import Dict, List, Optional
from datetime import datetime, timedelta

import shopify
from dotenv import load_dotenv

class ShopifyConnector:
    def __init__(self):
        load_dotenv()
        
        # Initialize Shopify session
        shop_url = f"https://{os.getenv('SHOPIFY_SHOP_NAME')}.myshopify.com"
        api_version = os.getenv('SHOPIFY_API_VERSION', '2024-01')
        access_token = os.getenv('SHOPIFY_ACCESS_TOKEN')
        
        shopify.Session.setup(api_key=access_token, secret=None)
        self.session = shopify.Session(shop_url, api_version, access_token)
        shopify.ShopifyResource.activate_session(self.session)

    def get_orders(self, days_back: int = 30) -> List[Dict]:
        """Fetch orders from the last n days."""
        created_at_min = datetime.now() - timedelta(days=days_back)
        
        orders = []
        page = 1
        
        while True:
            batch = shopify.Order.find(
                created_at_min=created_at_min.isoformat(),
                limit=250,
                page=page,
                status='any'
            )
            
            if not batch:
                break
                
            orders.extend([order.to_dict() for order in batch])
            page += 1
            
        return orders

    def get_abandoned_checkouts(self, days_back: int = 30) -> List[Dict]:
        """Fetch abandoned checkouts from the last n days."""
        created_at_min = datetime.now() - timedelta(days=days_back)
        
        checkouts = []
        page = 1
        
        while True:
            batch = shopify.Checkout.find(
                created_at_min=created_at_min.isoformat(),
                limit=250,
                page=page,
                status='any'
            )
            
            if not batch:
                break
                
            checkouts.extend([checkout.to_dict() for checkout in batch])
            page += 1
            
        return checkouts

    def get_refunds(self, order_id: int) -> List[Dict]:
        """Fetch refunds for a specific order."""
        try:
            order = shopify.Order.find(order_id)
            refunds = order.refunds()
            return [refund.to_dict() for refund in refunds]
        except Exception as e:
            print(f"Error fetching refunds for order {order_id}: {str(e)}")
            return []

    def get_customer_metrics(self, customer_id: int) -> Dict:
        """Fetch customer-specific metrics."""
        try:
            customer = shopify.Customer.find(customer_id)
            orders = shopify.Order.find(customer_id=customer_id)
            
            total_orders = len(orders)
            total_spent = sum(float(order.total_price) for order in orders)
            
            return {
                'customer_id': customer_id,
                'total_orders': total_orders,
                'total_spent': total_spent,
                'average_order_value': total_spent / total_orders if total_orders > 0 else 0,
                'first_order_date': min(order.created_at for order in orders) if orders else None,
                'last_order_date': max(order.created_at for order in orders) if orders else None
            }
        except Exception as e:
            print(f"Error fetching metrics for customer {customer_id}: {str(e)}")
            return {} 