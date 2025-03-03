from datetime import datetime, time
import pytz
from influxdb_client import InfluxDBClient
from clickhouse_driver import Client
from prefect import task, flow

# Constants for the InfluxDB connection
INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_TOKEN = "K49ea6K6amUsrJUmv"
INFLUXDB_ORG = "myorg"
INFLUXDB_BUCKET = "sensor_data"

# Constants for the ClickHouse connection
CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 9000


@task
def get_last_inserted_timestamp():
    """Retrieve the latest timestamp from ClickHouse."""
    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user='default')
    
    query = "SELECT MAX(time) FROM battery_events"
    result = client.execute(query)
    
    client.disconnect()
    
    last_timestamp = result[0][0] if result and result[0][0] else None
    print(f"Last inserted timestamp in ClickHouse: {last_timestamp}")
    
    return last_timestamp


@task
def extract_data(last_timestamp):
    """Extract only new battery event data from InfluxDB."""
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    query_api = client.query_api()

    # Convert last_timestamp to RFC3339 format (InfluxDB uses this)
    if last_timestamp:
        last_timestamp_str = last_timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
        time_filter = f'|> range(start: {last_timestamp_str})'
    else:
        time_filter = '|> range(start: -24h)'  # If no data in ClickHouse, fetch last 24 hours

    query = f'''
    from(bucket:"{INFLUXDB_BUCKET}") 
      {time_filter} 
      |> filter(fn: (r) => r._measurement == "battery_events")
    '''

    result = query_api.query(query)
    
    # Dictionary to store events per timestamp
    data_dict = {}

    for table in result:
        for record in table.records:
            time = record.get_time()
            field_name = record.get_field()
            value = record.get_value()

            if time not in data_dict:
                data_dict[time] = {
                    "time": time,
                    "battery_id": record["battery_id"],
                    "device_id": record["device_id"],
                    "station_id": record["station_id"],
                }
            
            data_dict[time][field_name] = value

    client.close()
    return list(data_dict.values())


@task
def transform_data(data, target_timezone='UTC'):
    """Transform data: convert timestamps and ensure correct data types."""
    transformed_data = []
    target_tz = pytz.timezone(target_timezone)

    for record in data:
        if record['time']:
            local_time = record['time'].astimezone(target_tz)
        else:
            local_time = datetime.utcnow().astimezone(target_tz)

        transformed_data.append({
            "time": local_time,
            "battery_id": record["battery_id"],
            "device_id": record["device_id"],
            "station_id": record["station_id"],
            "temperature_bms_pcb": float(record.get("temperature_bms_pcb", 0.0)),
            "temperature_internal": float(record.get("temperature_internal", 0.0)),
            "temperature_cell": float(record.get("temperature_cell", 0.0)),
            "c_fet_state": int(record.get("c_fet_state", 0)),
            "d_fet_state": int(record.get("d_fet_state", 0)),
            "soc": float(record.get("soc", 0.0)),
            "mileage": float(record.get("mileage", 0.0)),
            "speed": float(record.get("speed", 0.0)),
            "current_flow": float(record.get("current_flow", 0.0)),
            "latitude": float(record.get("latitude", 0.0)),
            "longitude": float(record.get("longitude", 0.0)),
            "alert_message": str(record.get("alert_message", "No Alert"))
        })

    return transformed_data


@task
def load_data_to_clickhouse(data):
    """Load transformed data into ClickHouse with efficient batch inserts."""
    if not data:
        print("No new data to insert into ClickHouse.")
        return

    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user='default')

    client.execute('''
    CREATE TABLE IF NOT EXISTS battery_events (
        time DateTime,
        battery_id String,
        device_id String,
        station_id String,
        temperature_bms_pcb Float32,
        temperature_internal Float32,
        temperature_cell Float32,
        c_fet_state UInt8,
        d_fet_state UInt8,
        soc Float32,
        mileage Float32,
        speed Float32,
        current_flow Float32,
        latitude Float32,
        longitude Float32,
        alert_message String
    ) ENGINE = MergeTree() ORDER BY time
    ''')

    # Insert Data
    client.execute(
        'INSERT INTO battery_events VALUES',
        [(row['time'], row['battery_id'], row['device_id'], row['station_id'], row['temperature_bms_pcb'], 
          row['temperature_internal'], row['temperature_cell'], row['c_fet_state'], row['d_fet_state'],
          row['soc'], row['mileage'], row['speed'], row['current_flow'], row['latitude'], row['longitude'],
          row['alert_message']) for row in data]
    )

    print(f"{len(data)} new records inserted into ClickHouse.")
    client.disconnect()


@flow
def battery_events_etl():
    """Prefect ETL flow to process only new battery events."""
    print("Starting Battery Events ETL Pipeline...")

    last_timestamp = get_last_inserted_timestamp()
    raw_data = extract_data(last_timestamp)
    
    if not raw_data:
        print("No new data found. Exiting ETL process.")
        return
    
    print("Data Extracted...")
    transformed_data = transform_data(raw_data)
    print("Data Transformed...")
    load_data_to_clickhouse(transformed_data)
    print("ETL process completed successfully!")


if __name__ == "__main__":
    battery_events_etl()