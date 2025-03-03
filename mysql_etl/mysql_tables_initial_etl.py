from sqlalchemy import create_engine
from pandas import read_sql_query
from clickhouse_driver import Client
from prefect import flow, task
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database Credentials
MYSQL_URL = 'mysql+pymysql://root:rootpass@localhost:3307/zembo'
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'

# Prefect Tasks
@task
def extract_data(table_name):
    """Extract data from MySQL"""
    engine = create_engine(MYSQL_URL)
    query = f'SELECT * FROM {table_name}'
    
    try:
        df = read_sql_query(query, engine)
        if table_name == "customers":
            df["customer_email"] = "REDACTED"
            df["customer_address"] = "REDACTED"
        logger.info(f"Extracted {len(df)} records from {table_name}.")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from {table_name}: {e}")
        return None
    finally:
        engine.dispose()

@task
def load_to_clickhouse(df, table_name):
    """Load data into ClickHouse"""
    if df is None or df.empty:
        logger.warning(f"No data to load into ClickHouse for {table_name}.")
        return

    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER)

    try:
        if table_name == "customers":
            client.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                customer_id Int32,
                customer_name String,
                customer_email String,
                customer_address String,
                registration_date DateTime
            ) ENGINE = MergeTree() ORDER BY customer_id
            ''')

        elif table_name == "orders":
            client.execute('''
            CREATE TABLE IF NOT EXISTS orders (
                order_id Int32,
                customer_id Int32,
                order_date DateTime,
                order_status String,
                total_amount Float64,
                battery_id Int32
            ) ENGINE = MergeTree() ORDER BY order_id
            ''')

        elif table_name == "battery_swaps":
            client.execute('''
            CREATE TABLE IF NOT EXISTS battery_swaps (
                swap_id Int32,
                battery_id Int32,
                station_id Int32,
                station_name String,
                rider_id Int32,
                swap_timestamp DateTime,
                battery_status String
            ) ENGINE = MergeTree() ORDER BY swap_id
            ''')

        data = [tuple(x) for x in df.to_numpy()]
        columns = ', '.join(df.columns)

        client.execute(f'INSERT INTO {table_name} ({columns}) VALUES', data)

        logger.info(f"Successfully loaded {len(df)} records into {table_name}.")
    except Exception as e:
        logger.error(f"Error loading data into ClickHouse ({table_name}): {e}")
    finally:
        client.disconnect()

@flow
def mysql_to_clickhouse_etl():
    """Main Prefect Flow for initial extract and load data"""
    for table in ["customers", "orders", "battery_swaps"]:
        data = extract_data(table)
        load_to_clickhouse(data, table)

# Run Prefect Flow
if __name__ == "__main__":
    mysql_to_clickhouse_etl()
