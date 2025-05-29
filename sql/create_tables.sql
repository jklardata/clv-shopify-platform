-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(255) PRIMARY KEY,
    store_id VARCHAR(255),
    email VARCHAR(255),
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    orders_count INTEGER,
    total_spent FLOAT,
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    accepts_marketing BOOLEAN,
    customer_state VARCHAR(50),
    last_order_id VARCHAR(255),
    note TEXT,
    verified_email BOOLEAN,
    tax_exempt BOOLEAN,
    tags TEXT,
    currency VARCHAR(10),
    country VARCHAR(100),
    province VARCHAR(100),
    city VARCHAR(100),
    zip VARCHAR(20),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(255) PRIMARY KEY,
    store_id VARCHAR(255),
    customer_id VARCHAR(255),
    order_number VARCHAR(50),
    total_price FLOAT,
    subtotal_price FLOAT,
    total_tax FLOAT,
    total_discounts FLOAT,
    currency VARCHAR(10),
    financial_status VARCHAR(50),
    fulfillment_status VARCHAR(50),
    processing_method VARCHAR(50),
    source_name VARCHAR(100),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    cancelled_at TIMESTAMP_NTZ,
    closed_at TIMESTAMP_NTZ,
    processed_at TIMESTAMP_NTZ,
    gateway VARCHAR(100),
    test BOOLEAN,
    taxes_included BOOLEAN,
    total_weight FLOAT,
    total_items INTEGER,
    tags TEXT,
    shipping_name VARCHAR(255),
    shipping_address1 VARCHAR(255),
    shipping_city VARCHAR(100),
    shipping_province VARCHAR(100),
    shipping_country VARCHAR(100),
    shipping_zip VARCHAR(20),
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Create order items table
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id VARCHAR(255) PRIMARY KEY,
    store_id VARCHAR(255),
    order_id VARCHAR(255),
    product_id VARCHAR(255),
    variant_id VARCHAR(255),
    title VARCHAR(255),
    quantity INTEGER,
    price FLOAT,
    sku VARCHAR(100),
    vendor VARCHAR(255),
    requires_shipping BOOLEAN,
    taxable BOOLEAN,
    name VARCHAR(255),
    fulfillment_status VARCHAR(50),
    grams INTEGER,
    total_discount FLOAT,
    created_at TIMESTAMP_NTZ,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

-- Create abandoned checkouts table
CREATE TABLE IF NOT EXISTS abandoned_checkouts (
    checkout_id VARCHAR(255) PRIMARY KEY,
    store_id VARCHAR(255),
    customer_id VARCHAR(255),
    email VARCHAR(255),
    total_price FLOAT,
    subtotal_price FLOAT,
    total_tax FLOAT,
    total_discounts FLOAT,
    currency VARCHAR(10),
    created_at TIMESTAMP_NTZ,
    updated_at TIMESTAMP_NTZ,
    abandoned_at TIMESTAMP_NTZ,
    recovery_url TEXT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Create returns table
CREATE TABLE IF NOT EXISTS returns (
    return_id VARCHAR(255) PRIMARY KEY,
    store_id VARCHAR(255),
    order_id VARCHAR(255),
    customer_id VARCHAR(255),
    status VARCHAR(50),
    created_at TIMESTAMP_NTZ,
    processed_at TIMESTAMP_NTZ,
    refund_amount FLOAT,
    currency VARCHAR(10),
    reason TEXT,
    note TEXT,
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
); 