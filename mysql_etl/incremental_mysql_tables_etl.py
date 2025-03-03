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

# Define table tracking columns for incremental updates
INCREMENTAL_COLUMNS = {
    "customers": "customer_id",       
    "orders": "order_id",             
    "battery_swaps": "swap_id"        
}

@task
def get_last_inserted_value(table_name):
    """Retrieve the last inserted value from ClickHouse for incremental loading."""
    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER)

    try:
        column = INCREMENTAL_COLUMNS.get(table_name)
        if not column:
            logger.error(f"Table {table_name} is not configured for incremental updates.")
            return None

        query = f"SELECT MAX({column}) FROM {table_name}"
        result = client.execute(query)

        last_value = result[0][0] if result and result[0][0] else None

        if last_value:
            logger.info(f"Last inserted {column} in ClickHouse ({table_name}): {last_value}")
        else:
            logger.warning(f"No existing data in {table_name}, full load will be performed.")

        return last_value

    except Exception as e:
        logger.error(f"Error fetching last inserted value from ClickHouse for {table_name}: {e}")
        return None

    finally:
        client.disconnect()

@task
def extract_updated_data(table_name, last_value):
    """Extract only new or updated records from MySQL with PII redacted."""
    engine = create_engine(MYSQL_URL)

    column = INCREMENTAL_COLUMNS.get(table_name)
    if not column:
        logger.error(f"Table {table_name} is not configured for incremental updates.")
        return None
# 
    query = f"SELECT * FROM {table_name}"

    # incremental filtering only if we have a last value
    if last_value:
        query += f" WHERE {column} > {last_value}"

    try:
        df = read_sql_query(query, engine)

        # Redact PII data for customers table
        if table_name == "customers":
            df["customer_email"] = "REDACTED"
            df["customer_address"] = "REDACTED"

        logger.info(f"Extracted {len(df)} new records from {table_name} (PII redacted if applicable).")
        return df
    except Exception as e:
        logger.error(f"Error extracting data from {table_name}: {e}")
        return None
    finally:
        engine.dispose()

@task
def load_to_clickhouse(df, table_name):
    """Load only new or updated data into ClickHouse."""
    if df is None or df.empty:
        logger.warning(f"No new data to load into ClickHouse for {table_name}.")
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
                rider_id Int32,
                swap_timestamp DateTime,
                battery_status String
            ) ENGINE = MergeTree() ORDER BY swap_id 
            ''')

        data = [tuple(x) for x in df.to_numpy()]
        columns = ', '.join(df.columns)

        client.execute(f'INSERT INTO {table_name} ({columns}) VALUES', data)

        logger.info(f"Successfully loaded {len(df)} new records into {table_name}.")
    except Exception as e:
        logger.error(f"Error loading data into ClickHouse ({table_name}): {e}")
    finally:
        client.disconnect()

@flow
def incremental_mysql_to_clickhouse_etl():
    """ETL Flow to extract and load only new or updated records"""
    for table in ["customers", "orders", "battery_swaps"]:
        last_value = get_last_inserted_value(table)
        new_data = extract_updated_data(table, last_value)
        load_to_clickhouse(new_data, table)

if __name__ == "__main__":
    incremental_mysql_to_clickhouse_etl()
