import snowflake.connector

# Connection parameters
conn_params = {
    'user': 'jleu',
    'password': 'SwitchTeam123!%',
    'account': 'pipykkn-pvb40654',
    'warehouse': 'CLV_WAREHOUSE',
    'database': 'CLV_ANALYTICS',
    'role': 'ACCOUNTADMIN',
    'client_session_keep_alive': True,
    'authenticator': 'snowflake'
}

try:
    # Create connection
    conn = snowflake.connector.connect(**conn_params)
    
    # Test connection
    cur = conn.cursor()
    cur.execute('SELECT CURRENT_VERSION()')
    version = cur.fetchone()[0]
    print(f"Connected to Snowflake! Version: {version}")
    
    # Close connection
    cur.close()
    conn.close()

except Exception as e:
    print(f"Error connecting to Snowflake: {str(e)}") 