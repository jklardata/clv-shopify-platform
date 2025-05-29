import os
from dotenv import load_dotenv
import snowflake.connector
from sqlalchemy import create_engine
import urllib.parse
import logging
from typing import Dict, Optional, Tuple
import concurrent.futures
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Common Snowflake regions
REGIONS = [
    'us-west-2',      # US West (Oregon)
    'us-east-1',      # US East (Virginia)
    'us-central-1',   # US Central (Iowa)
    'ca-central-1',   # Canada (Central)
    'eu-central-1',   # EU (Frankfurt)
    'eu-west-1',      # EU (Ireland)
    'ap-southeast-1', # Singapore
    'ap-southeast-2', # Sydney
    'ap-northeast-1', # Tokyo
]

def test_region_connection(account: str, user: str, password: str, region: str) -> Tuple[str, bool, str]:
    """Test connection to a specific region."""
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=f"{account}.{region}",
            insecure_mode=False,
            ocsp_fail_open=True,
            timeout=10  # Short timeout for quick testing
        )
        conn.close()
        return (region, True, "Success")
    except Exception as e:
        error_msg = str(e)
        # If we get a specific error about invalid username/password, this might be the correct region
        if "incorrect username or password" in error_msg.lower():
            return (region, True, "Correct region (auth failed)")
        return (region, False, error_msg)

def find_snowflake_region(account: str, user: str, password: str) -> Optional[str]:
    """Try to find the correct Snowflake region."""
    print("\nTesting different regions to find the correct one...")
    print("This may take a minute...\n")
    
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_region = {
            executor.submit(test_region_connection, account, user, password, region): region
            for region in REGIONS
        }
        
        for future in concurrent.futures.as_completed(future_to_region):
            region = future_to_region[future]
            try:
                result = future.result()
                results.append(result)
                print(f"Testing {region:15} : {result[2]}")
            except Exception as e:
                print(f"Testing {region:15} : Error - {str(e)}")
    
    # Check results
    potential_regions = [r[0] for r in results if r[1]]
    if potential_regions:
        return potential_regions[0]
    return None

def parse_account_url(account: str) -> Dict[str, str]:
    """Parse a Snowflake account identifier into components."""
    account = account.strip().lower()
    
    # Handle app.snowflake.com format
    if 'app.snowflake.com' in account:
        parts = account.split('/')
        if len(parts) >= 4:
            return {
                'organization': parts[3],
                'account': parts[4] if len(parts) > 4 else None,
                'region': None
            }
    
    # Handle traditional format
    if '-' in account:
        org, acc = account.split('-', 1)
        return {
            'organization': org,
            'account': acc,
            'region': None
        }
    
    return {
        'organization': account,
        'account': None,
        'region': None
    }

def format_account_identifier(account: str, region: Optional[str] = None) -> str:
    """Format the account identifier for connection."""
    components = parse_account_url(account)
    
    # Clean and validate region
    if region:
        region = region.strip().lower().replace('_', '-')
    
    # Build account identifier
    if components['organization'] and components['account']:
        # Use full org-account format
        base = f"{components['organization']}-{components['account']}"
    else:
        # Use just org/account
        base = components['organization'] or components['account']
    
    # Add region if provided
    if region:
        return f"{base}.{region}"
    return base

def test_direct_connection():
    """Test direct Snowflake connection."""
    print("\nTesting direct Snowflake connection...")
    try:
        # Get connection parameters
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_ADMIN_USER') or os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_ADMIN_PASSWORD') or os.getenv('SNOWFLAKE_PASSWORD')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        database = os.getenv('SNOWFLAKE_DATABASE')
        role = os.getenv('SNOWFLAKE_ADMIN_ROLE')

        # Validate required parameters
        if not all([account, user, password]):
            raise ValueError("Missing required connection parameters")

        # For app.snowflake.com URLs, extract organization and account
        if 'app.snowflake.com' in account:
            parts = account.strip('/').split('/')
            if len(parts) >= 5:
                org_id = parts[3]
                account_id = parts[4].split('#')[0]  # Remove any hash/fragment
                print(f"Extracted from URL - Organization: {org_id}, Account: {account_id}")
                # For app.snowflake.com, use just the organization ID
                account = org_id
        else:
            # If not a URL, assume it's the organization ID
            account = account.lower()

        print(f"Using account identifier: {account}")
        
        # Create connection with modern parameters
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            role=role,
            client_session_keep_alive=True,
            application='CLV_PLATFORM',
            authenticator='snowflake',
            client_session_keep_alive_heartbeat_frequency=3600,
            login_timeout=30,
            network_timeout=30,
            ocsp_response_cache_filename='/tmp/ocsp_response_cache'
        )

        # Test connection
        cur = conn.cursor()
        cur.execute('SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION()')
        result = cur.fetchone()
        print(f"""
Connection successful!
Account: {result[0]}
User: {result[1]}
Role: {result[2]}
Version: {result[3]}
        """)
        
        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        print("\nTroubleshooting tips:")
        print("1. For app.snowflake.com accounts, use ONLY the organization identifier")
        print("2. Example: If your URL is app.snowflake.com/pipykkn/pvb40654")
        print("   Set SNOWFLAKE_ACCOUNT=pipykkn")
        print("3. Make sure you're using the correct username/password")
        print("4. Try removing any region settings")
        print("5. Check if you can connect using SnowSQL CLI")
        return False

def test_sqlalchemy_connection():
    """Test SQLAlchemy connection to Snowflake."""
    print("\nTesting SQLAlchemy connection...")
    try:
        # Get connection parameters
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_ADMIN_USER') or os.getenv('SNOWFLAKE_USER')
        password = os.getenv('SNOWFLAKE_ADMIN_PASSWORD') or os.getenv('SNOWFLAKE_PASSWORD')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        database = os.getenv('SNOWFLAKE_DATABASE')
        role = os.getenv('SNOWFLAKE_ADMIN_ROLE')

        # For app.snowflake.com URLs, extract organization and account
        if 'app.snowflake.com' in account:
            parts = account.strip('/').split('/')
            if len(parts) >= 5:
                org_id = parts[3]
                account_id = parts[4].split('#')[0]  # Remove any hash/fragment
                print(f"Extracted from URL - Organization: {org_id}, Account: {account_id}")
                # For app.snowflake.com, use just the organization ID
                account = org_id
        else:
            # If not a URL, assume it's the organization ID
            account = account.lower()

        print(f"Using account identifier: {account}")

        # Create SQLAlchemy connection URL with modern parameters
        conn_str = f"snowflake://{user}:{urllib.parse.quote_plus(password)}@{account}"
        
        # Add database if specified
        if database:
            conn_str += f"/{database}"
        
        # Create engine with modern parameters
        engine_params = {
            'connect_args': {
                'warehouse': warehouse,
                'role': role,
                'client_session_keep_alive': True,
                'application': 'CLV_PLATFORM',
                'authenticator': 'snowflake',
                'client_session_keep_alive_heartbeat_frequency': 3600,
                'login_timeout': 30,
                'network_timeout': 30,
                'ocsp_response_cache_filename': '/tmp/ocsp_response_cache'
            }
        }

        # Create engine
        engine = create_engine(conn_str, **engine_params)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute('SELECT CURRENT_ACCOUNT(), CURRENT_USER(), CURRENT_ROLE(), CURRENT_VERSION()').fetchone()
            print(f"""
Connection successful!
Account: {result[0]}
User: {result[1]}
Role: {result[2]}
Version: {result[3]}
            """)
        return True

    except Exception as e:
        print(f"Error connecting via SQLAlchemy: {str(e)}")
        return False

def main():
    """Main test function."""
    print("Testing Snowflake Connections...")
    print("=" * 50)

    # Load environment variables
    load_dotenv()
    
    # Get and display connection parameters (safely)
    account = os.getenv('SNOWFLAKE_ACCOUNT', '')
    user = os.getenv('SNOWFLAKE_ADMIN_USER') or os.getenv('SNOWFLAKE_USER', '')
    warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', '')
    database = os.getenv('SNOWFLAKE_DATABASE', '')
    role = os.getenv('SNOWFLAKE_ADMIN_ROLE', '')

    print("\nConnection Parameters:")
    print(f"Account: {account}")
    print(f"User: {user}")
    print(f"Warehouse: {warehouse}")
    print(f"Database: {database}")
    print(f"Role: {role}")
    print("=" * 50)

    # Test connections
    direct_success = test_direct_connection()
    sqlalchemy_success = test_sqlalchemy_connection()

    # Summary
    print("\nConnection Test Summary:")
    print(f"Direct Connection: {'✓ Success' if direct_success else '✗ Failed'}")
    print(f"SQLAlchemy Connection: {'✓ Success' if sqlalchemy_success else '✗ Failed'}")

if __name__ == "__main__":
    main() 