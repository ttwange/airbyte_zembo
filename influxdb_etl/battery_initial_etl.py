from datetime import datetime
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
def extract_data():
    """ Extract and pivot battery event data from InfluxDB. """
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    query_api = client.query_api()
    
    query = f'''
    from(bucket:"{INFLUXDB_BUCKET}") 
      |> range(start: -24h) 
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

    # Convert dictionary back to list format
    data_for_export = list(data_dict.values())

    client.close()
    return data_for_export

@task
def transform_data(data, target_timezone='UTC'):
    """ Transform data: convert timestamps and ensure correct data types. """
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
            "temperature_bms_pcb": float(record["temperature_bms_pcb"]) if record["temperature_bms_pcb"] is not None else 0.0,
            "temperature_internal": float(record["temperature_internal"]) if record["temperature_internal"] is not None else 0.0,
            "temperature_cell": float(record["temperature_cell"]) if record["temperature_cell"] is not None else 0.0,
            "c_fet_state": int(record["c_fet_state"]) if record["c_fet_state"] is not None else 0,
            "d_fet_state": int(record["d_fet_state"]) if record["d_fet_state"] is not None else 0,
            "soc": float(record["soc"]) if record["soc"] is not None else 0.0,
            "mileage": float(record["mileage"]) if record["mileage"] is not None else 0.0,
            "speed": float(record["speed"]) if record["speed"] is not None else 0.0,
            "current_flow": float(record["current_flow"]) if record["current_flow"] is not None else 0.0,
            "latitude": float(record["latitude"]) if record["latitude"] is not None else 0.0,
            "longitude": float(record["longitude"]) if record["longitude"] is not None else 0.0,
            "alert_message": str(record["alert_message"]) if record["alert_message"] else "No Alert"
        })

    return transformed_data

@task
def load_data_to_clickhouse(data):
    """ Load transformed data into ClickHouse. """
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

    for row in data:
        if not isinstance(row["temperature_bms_pcb"], float):
            print(f"WARNING: Invalid `temperature_bms_pcb`: {row['temperature_bms_pcb']} (Fixing)")
            row["temperature_bms_pcb"] = 0.0
    
    # Insert Data
    client.execute(
        'INSERT INTO battery_events VALUES',
        [(row['time'], row['battery_id'], row['device_id'], row['station_id'], row['temperature_bms_pcb'], 
          row['temperature_internal'], row['temperature_cell'], row['c_fet_state'], row['d_fet_state'],
          row['soc'], row['mileage'], row['speed'], row['current_flow'], row['latitude'], row['longitude'],
          row['alert_message']) for row in data]
    )

    print(f"{len(data)} records inserted into ClickHouse.")
    client.disconnect()

@flow
def battery_events_etl():
    """ Prefect ETL flow to process battery events. """
    print("Starting Battery Events ETL Pipeline...")
    raw_data = extract_data()
    print("Data Extracted...")
    transformed_data = transform_data(raw_data)
    print("Data Transformed...")
    load_data_to_clickhouse(transformed_data)
    print("ETL process completed successfully!")

# Run the Prefect flow
if __name__ == "__main__":
    battery_events_etl()
