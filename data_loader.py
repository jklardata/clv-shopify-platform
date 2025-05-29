from typing import List, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from models import Customer, Order, OrderLineItem, CustomerStatus
import json

class DataLoader:
    def __init__(self, db_session: Session):
        self.session = db_session
    
    def load_customer(self, customer_data: Dict[str, Any]) -> Customer:
        """Load or update a customer record."""
        customer_id = str(customer_data['id'])
        customer = self.session.query(Customer).get(customer_id)
        
        if not customer:
            customer = Customer(
                id=customer_id,
                email=customer_data.get('email'),
                first_name=customer_data.get('first_name'),
                last_name=customer_data.get('last_name'),
                total_spent=float(customer_data.get('total_spent', 0)),
                orders_count=int(customer_data.get('orders_count', 0)),
                status=CustomerStatus.NEW,
                metadata=customer_data
            )
            self.session.add(customer)
        else:
            # Update existing customer
            customer.email = customer_data.get('email', customer.email)
            customer.first_name = customer_data.get('first_name', customer.first_name)
            customer.last_name = customer_data.get('last_name', customer.last_name)
            customer.total_spent = float(customer_data.get('total_spent', customer.total_spent))
            customer.orders_count = int(customer_data.get('orders_count', customer.orders_count))
            customer.metadata = customer_data
        
        return customer
    
    def load_order(self, order_data: Dict[str, Any]) -> Order:
        """Load or update an order record."""
        order_id = str(order_data['id'])
        order = self.session.query(Order).get(order_id)
        
        if not order:
            order = Order(
                id=order_id,
                customer_id=str(order_data.get('customer', {}).get('id')),
                order_number=order_data.get('order_number'),
                total_price=float(order_data.get('total_price', 0)),
                subtotal_price=float(order_data.get('subtotal_price', 0)),
                total_tax=float(order_data.get('total_tax', 0)),
                total_discounts=float(order_data.get('total_discounts', 0)),
                currency=order_data.get('currency'),
                financial_status=order_data.get('financial_status'),
                fulfillment_status=order_data.get('fulfillment_status'),
                order_date=datetime.fromisoformat(order_data.get('created_at').replace('Z', '+00:00')),
                metadata=order_data
            )
            self.session.add(order)
            
            # Load line items
            for item_data in order_data.get('line_items', []):
                line_item = OrderLineItem(
                    id=str(item_data['id']),
                    order_id=order_id,
                    product_id=str(item_data.get('product_id')),
                    variant_id=str(item_data.get('variant_id')),
                    title=item_data.get('title'),
                    quantity=int(item_data.get('quantity', 0)),
                    price=float(item_data.get('price', 0)),
                    total_discount=float(item_data.get('total_discount', 0)),
                    sku=item_data.get('sku'),
                    metadata=item_data
                )
                self.session.add(line_item)
        
        return order
    
    def update_customer_metrics(self, customer: Customer):
        """Update customer metrics based on orders."""
        orders = customer.orders
        if orders:
            customer.first_order_date = min(order.order_date for order in orders)
            customer.last_order_date = max(order.order_date for order in orders)
            customer.total_spent = sum(order.total_price for order in orders)
            customer.orders_count = len(orders)
            
            # Update customer status
            days_since_last_order = (datetime.utcnow() - customer.last_order_date).days
            if days_since_last_order <= 90:  # Active if ordered in last 90 days
                customer.status = CustomerStatus.ACTIVE
            else:
                customer.status = CustomerStatus.INACTIVE
            
            # Calculate and update CLV
            customer.clv_prediction = customer.calculate_clv()
    
    def process_shopify_data(self, data: Dict[str, Any]):
        """Process Shopify webhook data."""
        try:
            if 'customer' in data:
                customer = self.load_customer(data['customer'])
                self.session.commit()
            
            if 'order' in data:
                order = self.load_order(data['order'])
                if order.customer:
                    self.update_customer_metrics(order.customer)
                self.session.commit()
                
        except Exception as e:
            self.session.rollback()
            raise e 