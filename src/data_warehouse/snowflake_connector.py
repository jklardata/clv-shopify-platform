import os
from typing import Dict, List, Optional
from datetime import datetime

import pandas as pd
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from dotenv import load_dotenv

class SnowflakeConnector:
    def __init__(self):
        load_dotenv()
        
        # Initialize Snowflake connection
        self.engine = create_engine(URL(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA', 'ecomm_transactions')
        ))

    def create_tables(self):
        """Create necessary tables in Snowflake."""
        with self.engine.connect() as conn:
            # Orders table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id VARCHAR(255) PRIMARY KEY,
                    customer_id VARCHAR(255),
                    order_date TIMESTAMP,
                    total_price FLOAT,
                    currency VARCHAR(10),
                    order_status VARCHAR(50),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)

            # Abandoned checkouts table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS abandoned_checkouts (
                    checkout_id VARCHAR(255) PRIMARY KEY,
                    customer_id VARCHAR(255),
                    created_at TIMESTAMP,
                    total_price FLOAT,
                    currency VARCHAR(10),
                    recovery_url VARCHAR(1000)
                )
            """)

            # Refunds table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS refunds (
                    refund_id VARCHAR(255) PRIMARY KEY,
                    order_id VARCHAR(255),
                    created_at TIMESTAMP,
                    amount FLOAT,
                    currency VARCHAR(10),
                    FOREIGN KEY (order_id) REFERENCES orders(order_id)
                )
            """)

            # Customer metrics table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS customer_metrics (
                    customer_id VARCHAR(255) PRIMARY KEY,
                    total_orders INT,
                    total_spent FLOAT,
                    average_order_value FLOAT,
                    first_order_date TIMESTAMP,
                    last_order_date TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)

    def upsert_orders(self, orders: List[Dict]):
        """Upsert orders data into Snowflake."""
        df = pd.DataFrame(orders)
        df.to_sql('orders', self.engine, if_exists='append', index=False, method='multi')

    def upsert_abandoned_checkouts(self, checkouts: List[Dict]):
        """Upsert abandoned checkouts data into Snowflake."""
        df = pd.DataFrame(checkouts)
        df.to_sql('abandoned_checkouts', self.engine, if_exists='append', index=False, method='multi')

    def upsert_refunds(self, refunds: List[Dict]):
        """Upsert refunds data into Snowflake."""
        df = pd.DataFrame(refunds)
        df.to_sql('refunds', self.engine, if_exists='append', index=False, method='multi')

    def upsert_customer_metrics(self, metrics: List[Dict]):
        """Upsert customer metrics data into Snowflake."""
        df = pd.DataFrame(metrics)
        df['updated_at'] = datetime.now()
        df.to_sql('customer_metrics', self.engine, if_exists='append', index=False, method='multi')

    def get_customer_clv_data(self, customer_id: str) -> Dict:
        """Retrieve customer CLV-related data from Snowflake."""
        query = """
            SELECT 
                c.customer_id,
                c.total_orders,
                c.total_spent,
                c.average_order_value,
                c.first_order_date,
                c.last_order_date,
                COUNT(DISTINCT ac.checkout_id) as abandoned_checkouts_count,
                COUNT(DISTINCT r.refund_id) as refunds_count,
                COALESCE(SUM(r.amount), 0) as total_refunded
            FROM customer_metrics c
            LEFT JOIN abandoned_checkouts ac ON c.customer_id = ac.customer_id
            LEFT JOIN orders o ON c.customer_id = o.customer_id
            LEFT JOIN refunds r ON o.order_id = r.order_id
            WHERE c.customer_id = %s
            GROUP BY 1, 2, 3, 4, 5, 6
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(query, (customer_id,)).fetchone()
            return dict(result) if result else None 