# Shopify to Snowflake CLV Pipeline

A robust data pipeline for calculating and analyzing Customer Lifetime Value (CLV) metrics by syncing Shopify data to Snowflake.

## Overview

This platform enables businesses to track and analyze customer lifetime value by automatically syncing Shopify customer, order, and checkout data to Snowflake. The pipeline handles various data types including customers, orders, order line items, abandoned checkouts, and returns - providing a complete view of customer purchasing behavior.

## Features

- **Automated Data Sync**: Continuously syncs Shopify data to Snowflake using cursor-based pagination
- **Rich Customer Data**: Captures comprehensive customer information including:
  - Basic profile (email, name, etc.)
  - Purchase history
  - Order details
  - Abandoned checkouts
  - Returns and refunds
  
- **CLV Metrics**: Calculate key CLV metrics:
  - Historic customer lifetime value
  - Total spent
  - Orders count
  - First/last order dates
  - Marketing consent status
  - Customer tags

- **Flexible Schema**: Data stored in a configurable Snowflake schema per store

## Requirements

- Python 3.11+
- Shopify Admin API access
- Snowflake account with write permissions
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/clv-shopify-platform.git
cd clv-shopify-platform
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
export SHOPIFY_SHOP_NAME="your-store-name"
export SHOPIFY_ACCESS_TOKEN="your-access-token"
export SNOWFLAKE_USER="your-snowflake-user"
export SNOWFLAKE_PASSWORD="your-snowflake-password"
export SNOWFLAKE_ACCOUNT="your-snowflake-account"
```

## Usage

Run the data sync:

```bash
python load_shopify_data.py your-store-name
```

This will:
1. Initialize connections to Shopify and Snowflake
2. Load customer data
3. Load order data and line items
4. Load abandoned checkout data
5. Calculate and update CLV metrics

## Data Model

The platform uses the following core tables in Snowflake:

- `customers`: Customer profiles and CLV metrics
- `orders`: Order details and status
- `order_items`: Individual line items from orders
- `abandoned_checkouts`: Abandoned cart data
- `returns`: Order return and refund data

## Benefits

1. **Data-Driven Decisions**: Make informed decisions based on actual customer behavior and value

2. **Customer Segmentation**: Segment customers based on:
   - Purchase frequency
   - Total spend
   - Product preferences
   - Likelihood to return

3. **Marketing Optimization**: 
   - Target high-value customers
   - Re-engage inactive customers
   - Optimize acquisition costs
   - Personalize marketing campaigns

4. **Revenue Forecasting**:
   - Predict future customer value
   - Identify trends in customer behavior
   - Forecast revenue based on customer segments

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For support, please open an issue in the GitHub repository or contact the maintainers.