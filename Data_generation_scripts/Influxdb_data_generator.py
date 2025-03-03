from influxdb_client import InfluxDBClient, Point, WritePrecision
import random
from prefect import task, flow, get_run_logger
from datetime import datetime
import pytz
import os
import time

# Environment Variables for Config
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "K49ea6K6amUsrJUmv")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "myorg")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor_data")

# Define the target total records
TOTAL_RECORDS = 100000
BATCH_SIZE = 5000 

# Track inserted records count
inserted_count = 0

@task
def generate_battery_events(n=BATCH_SIZE):
    """Generates synthetic battery event data for InfluxDB."""
    logger = get_run_logger()
    points = []
    
    for _ in range(n):
        point = (
            Point("battery_events")
            .tag("battery_id", str(random.randint(1000, 9999)))
            .tag("device_id", str(random.randint(1, 100)))
            .tag("vehicle_id", str(random.randint(5000, 9999)))
            .tag("station_id", str(random.randint(1, 50)))
            .tag("alarm_id", str(random.randint(100, 9999)))
            .field("temperature_bms_pcb", random.uniform(25, 60))
            .field("temperature_internal", random.uniform(20, 50))
            .field("temperature_cell", random.uniform(20, 45))
            .field("c_fet_state", random.choice([0, 1]))
            .field("d_fet_state", random.choice([0, 1]))
            .field("balanced_start", random.choice([0, 1]))
            .field("charge_protection", random.choice([0, 1]))
            .field("discharge_protection", random.choice([0, 1]))
            .field("recovery_thresholds", random.uniform(10, 90))
            .field("nominal_capacity", random.uniform(50, 100))
            .field("charged_capacity", random.uniform(1, 50))
            .field("discharged_capacity", random.uniform(1, 50))
            .field("soc", random.uniform(10, 100))
            .field("cycle_count", random.randint(0, 1000))
            .field("mileage", random.uniform(0, 1000))
            .field("speed", random.uniform(0, 100))
            .field("current_flow", random.uniform(-10, 50))
            .field("latitude", random.uniform(-1.2, 1.5))
            .field("longitude", random.uniform(36.5, 38.5))
            .field("location_type", random.choice(["urban", "rural", "highway"]))
            .field("alert_message", random.choice(["Overheat", "Low SOC", "Discharge Fault"]))
            .field("historical_alerts", random.randint(0, 50))
            .field("severity_classification", random.choice(["low", "medium", "high"]))
            .field("response_protocol", random.choice(["manual", "automatic"]))
            .time(datetime.now(pytz.UTC), WritePrecision.NS)
        )
        points.append(point)
    
    logger.info(f"Generated {n} battery event records.")
    return points

@task
def write_to_influxdb(points):
    """Writes generated battery event data to InfluxDB."""
    logger = get_run_logger()
    
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api()
    
    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=points)
    
    logger.info(f"Inserted {len(points)} records into InfluxDB.")
    
    client.close()
    
    return len(points)

@flow(name="Battery Event Data Pipeline")
def insert_battery_event():
    """Prefect flow to generate and insert battery event data incrementally."""
    global inserted_count

    while inserted_count < TOTAL_RECORDS:
        points = generate_battery_events()
        inserted = write_to_influxdb(points)
        
        inserted_count += inserted
        print(f"Total records inserted: {inserted_count}/{TOTAL_RECORDS}")

        time.sleep(120) 

        if inserted_count >= TOTAL_RECORDS:
            print("Data generation complete! 100k records inserted.")
            break

# Run the Prefect flow
if __name__ == "__main__":
    insert_battery_event()

