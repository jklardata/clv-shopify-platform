from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Enum, Boolean, Text, types
from snowflake.sqlalchemy import VARIANT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator, UserDefinedType
import enum
from datetime import datetime
from config import StoreConfig
import os
import json

# Get schema name from environment or store config
def get_schema_name():
    store_name = os.getenv('SHOPIFY_SHOP_NAME', 'clv-test-store')
    store_config = StoreConfig(store_name)
    return store_config.snowflake.schema

# Get the schema name
SCHEMA_NAME = get_schema_name()

class CustomerStatus(enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    NEW = "new"

Base = declarative_base()

class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(String, primary_key=True)  # Shopify customer ID
    email = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    total_spent = Column(Float, default=0.0)
    orders_count = Column(Integer, default=0)
    status = Column(Enum(CustomerStatus), default=CustomerStatus.NEW)
    first_order_date = Column(DateTime, nullable=True)
    last_order_date = Column(DateTime, nullable=True)
    clv_prediction = Column(Float, nullable=True)
    accepts_marketing = Column(Boolean, default=False)
    tax_exempt = Column(Boolean, default=False)
    tags = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    orders = relationship("Order", back_populates="customer")
    abandoned_checkouts = relationship("AbandonedCheckout", back_populates="customer")
    
    def calculate_clv(self):
        """Calculate customer lifetime value."""
        if not self.orders:
            return 0.0
        return sum(order.total_price for order in self.orders)

class Order(Base):
    __tablename__ = "orders"
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(String, primary_key=True)  # Shopify order ID
    customer_id = Column(String, ForeignKey(f"{SCHEMA_NAME}.customers.id"))
    order_number = Column(String)
    total_price = Column(Float)
    subtotal_price = Column(Float)
    total_tax = Column(Float)
    total_discounts = Column(Float)
    currency = Column(String)
    financial_status = Column(String)
    fulfillment_status = Column(String)
    order_date = Column(DateTime)
    cancelled_at = Column(DateTime, nullable=True)
    cancel_reason = Column(String, nullable=True)
    tags = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    customer = relationship("Customer", back_populates="orders")
    line_items = relationship("OrderLineItem", back_populates="order")
    returns = relationship("OrderReturn", back_populates="order")

class OrderLineItem(Base):
    __tablename__ = "order_items"
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(String, primary_key=True)  # Shopify line item ID
    order_id = Column(String, ForeignKey(f"{SCHEMA_NAME}.orders.id"))
    product_id = Column(String, nullable=True)
    variant_id = Column(String, nullable=True)
    title = Column(String)
    quantity = Column(Integer)
    price = Column(Float)
    total_discount = Column(Float)
    sku = Column(String, nullable=True)
    fulfillable_quantity = Column(Integer, default=0)
    fulfillment_status = Column(String, nullable=True)
    gift_card = Column(Boolean, default=False)
    taxable = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    order = relationship("Order", back_populates="line_items")

class AbandonedCheckout(Base):
    __tablename__ = "abandoned_checkouts"
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(String, primary_key=True)  # Shopify checkout ID
    customer_id = Column(String, ForeignKey(f"{SCHEMA_NAME}.customers.id"), nullable=True)
    email = Column(String, nullable=True)
    total_price = Column(Float)
    subtotal_price = Column(Float)
    total_tax = Column(Float)
    total_discounts = Column(Float)
    currency = Column(String)
    completed_at = Column(DateTime, nullable=True)
    abandoned_at = Column(DateTime)
    recovery_url = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    customer = relationship("Customer", back_populates="abandoned_checkouts")

class OrderReturn(Base):
    __tablename__ = "returns"
    __table_args__ = {'schema': SCHEMA_NAME}
    
    id = Column(String, primary_key=True)  # Shopify return ID
    order_id = Column(String, ForeignKey(f"{SCHEMA_NAME}.orders.id"))
    status = Column(String)
    return_date = Column(DateTime)
    refund_amount = Column(Float, default=0.0)
    restock_items = Column(Boolean, default=False)
    note = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    order = relationship("Order", back_populates="returns") 