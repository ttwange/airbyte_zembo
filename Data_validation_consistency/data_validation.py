# from sqlalchemy import create_engine
# from pandas import read_sql_query
# from clickhouse_driver import Client
# from prefect import flow, task
# import logging

# # Configure Logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Database Credentials
# MYSQL_URL = 'mysql+pymysql://root:rootpass@localhost:3307/zembo'
# CLICKHOUSE_HOST = 'localhost'
# CLICKHOUSE_PORT = 9000
# CLICKHOUSE_USER = 'default'

# @task
# def get_mysql_count(table_name):
#     """Get record count from MySQL"""
#     engine = create_engine(MYSQL_URL)
#     query = f'SELECT COUNT(*) FROM {table_name}'

#     try:
#         result = read_sql_query(query, engine)
#         mysql_count = result.iloc[0, 0]
#         logger.info(f"📊 MySQL {table_name}: {mysql_count} records")
#         return mysql_count
#     except Exception as e:
#         logger.error(f"❌ Error getting count from MySQL ({table_name}): {e}")
#         return None
#     finally:
#         engine.dispose()

# @task
# def get_clickhouse_count(table_name):
#     """Get record count from ClickHouse"""
#     client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER)

#     try:
#         query = f'SELECT COUNT(*) FROM {table_name}'
#         result = client.execute(query)
#         clickhouse_count = result[0][0]
#         logger.info(f"📊 ClickHouse {table_name}: {clickhouse_count} records")
#         return clickhouse_count
#     except Exception as e:
#         logger.error(f"❌ Error getting count from ClickHouse ({table_name}): {e}")
#         return None
#     finally:
#         client.disconnect()

# @task
# def compare_counts(mysql_count, clickhouse_count, table_name):
#     """Compare counts between MySQL and ClickHouse"""
#     if mysql_count is None or clickhouse_count is None:
#         logger.warning(f"⚠️ Skipping comparison for {table_name} due to missing data")
#         return False

#     if mysql_count == clickhouse_count:
#         logger.info(f"✅ Data is consistent for {table_name}: {mysql_count} records")
#         return True
#     else:
#         logger.warning(f"⚠️ MISMATCH for {table_name}: MySQL={mysql_count}, ClickHouse={clickhouse_count}")
#         return False

# @flow
# def validate_data():
#     """Compare data consistency between MySQL and ClickHouse"""
#     tables = ["customers", "orders", "battery_swaps"]

#     for table in tables:
#         mysql_count = get_mysql_count(table)
#         clickhouse_count = get_clickhouse_count(table)
#         compare_counts(mysql_count, clickhouse_count, table)

# if __name__ == "__main__":
#     validate_data()

from sqlalchemy import create_engine
from pandas import read_sql_query
from clickhouse_driver import Client
from prefect import flow, task
import logging
import sys

# Configure logging to output to a file
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler("output.txt"),  # Logs to output.txt
        logging.StreamHandler(sys.stdout)   # Also logs to console
    ]
)
logger = logging.getLogger(__name__)

# Database Credentials
MYSQL_URL = 'mysql+pymysql://root:rootpass@localhost:3307/zembo'
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = 'default'


@task(log_prints=True)
def get_mysql_count(table_name):
    """Get record count from MySQL"""
    engine = create_engine(MYSQL_URL)
    query = f'SELECT COUNT(*) FROM {table_name}'

    try:
        result = read_sql_query(query, engine)
        mysql_count = result.iloc[0, 0]
        logger.info(f"📊 MySQL {table_name}: {mysql_count} records")
        return mysql_count
    except Exception as e:
        logger.error(f"❌ Error getting count from MySQL ({table_name}): {e}")
        return None
    finally:
        engine.dispose()


@task(log_prints=True)
def get_clickhouse_count(table_name):
    """Get record count from ClickHouse"""
    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER)

    try:
        query = f'SELECT COUNT(*) FROM {table_name}'
        result = client.execute(query)
        clickhouse_count = result[0][0]
        logger.info(f"📊 ClickHouse {table_name}: {clickhouse_count} records")
        return clickhouse_count
    except Exception as e:
        logger.error(f"❌ Error getting count from ClickHouse ({table_name}): {e}")
        return None
    finally:
        client.disconnect()


@task(log_prints=True)
def compare_counts(mysql_count, clickhouse_count, table_name):
    """Compare counts between MySQL and ClickHouse"""
    if mysql_count is None or clickhouse_count is None:
        logger.warning(f"⚠️ Skipping comparison for {table_name} due to missing data")
        return False

    if mysql_count == clickhouse_count:
        logger.info(f"✅ Data is consistent for {table_name}: {mysql_count} records")
        return True
    else:
        logger.warning(f"⚠️ MISMATCH for {table_name}: MySQL={mysql_count}, ClickHouse={clickhouse_count}")
        return False


@flow(log_prints=True)
def validate_data():
    """Compare data consistency between MySQL and ClickHouse"""
    logger.info("🔍 Running Data Validation for MySQL & ClickHouse")
    tables = ["customers", "orders", "battery_swaps"]

    for table in tables:
        mysql_count = get_mysql_count(table)
        clickhouse_count = get_clickhouse_count(table)
        compare_counts(mysql_count, clickhouse_count, table)

    logger.info("✅ Data validation completed.")


if __name__ == "__main__":
    validate_data()
