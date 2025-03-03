from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Enum, DECIMAL
from sqlalchemy.orm import sessionmaker, declarative_base
from faker import Faker
import random
import time
from datetime import datetime
from prefect import task, flow, get_run_logger

# Database Connection Setup
DATABASE_URL = "mysql+pymysql://root:password@localhost/zembo"

engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)
Base = declarative_base()
fake = Faker()

# Predefined station names
STATION_NAMES = [
    "Kampala", "Gulu", "Mbarara", "Entebbe", "Hoima", 
    "Masaka", "Arua", "Wakiso", "Jinja", "Lira", 
    "Fort Portal", "Mbale", "Kasese", "Nansana", "Soroti"
]

# Define Schema
class Customer(Base):
    __tablename__ = 'customers'
    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_name = Column(String(255), nullable=False)
    customer_email = Column(String(255), unique=True, nullable=False)
    customer_address = Column(String(255))
    registration_date = Column(DateTime, default=datetime.utcnow)

class Order(Base):
    __tablename__ = 'orders'
    order_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, ForeignKey('customers.customer_id'))
    order_date = Column(DateTime, default=datetime.utcnow)
    order_status = Column(Enum('placed', 'shipped', 'delivered', 'cancelled'), nullable=False)
    total_amount = Column(DECIMAL(10, 2), nullable=False)
    battery_id = Column(Integer)

class BatterySwap(Base):
    __tablename__ = 'battery_swaps'
    swap_id = Column(Integer, primary_key=True, autoincrement=True)
    battery_id = Column(Integer, nullable=False)
    station_id = Column(Integer, nullable=False)
    station_name = Column(String(100), nullable=False)  # New column
    rider_id = Column(Integer, nullable=False)
    swap_timestamp = Column(DateTime, default=datetime.utcnow)
    battery_status = Column(Enum('depleted', 'charged', 'in_use'), nullable=False)

# Create Tables if they don't exist
Base.metadata.create_all(engine)

# Track unique emails
generated_emails = set()

@task
def insert_customers(batch_size=100):
    """Insert customers into MySQL in batches."""
    session = Session()
    logger = get_run_logger()
    
    customers = []
    for _ in range(batch_size):
        while True:
            email = fake.email()
            if email not in generated_emails:
                generated_emails.add(email)
                break

        customers.append(
            Customer(
                customer_name=fake.name(),
                customer_email=email,
                customer_address=fake.address(),
                registration_date=fake.date_time_between(start_date='-2y', end_date='now')
            )
        )

    session.add_all(customers)
    
    try:
        session.commit()
        logger.info(f"{batch_size} Customers inserted.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error inserting customers: {e}")
    finally:
        session.close()


@task
def insert_orders(batch_size=500):
    """Insert orders into MySQL in batches."""
    session = Session()
    logger = get_run_logger()
    
    customer_ids = [c.customer_id for c in session.query(Customer.customer_id).all()]
    statuses = ['placed', 'shipped', 'delivered', 'cancelled']
    
    orders = []
    for _ in range(batch_size):
        orders.append(
            Order(
                customer_id=random.choice(customer_ids),
                order_date=fake.date_time_between(start_date='-1y', end_date='now'),
                order_status=random.choice(statuses),
                total_amount=round(random.uniform(50.00, 5000.00), 2),
                battery_id=random.randint(1000, 9999)
            )
        )

    session.add_all(orders)
    
    try:
        session.commit()
        logger.info(f"{batch_size} Orders inserted.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error inserting orders: {e}")
    finally:
        session.close()


@task
def insert_battery_swaps(batch_size=1000):
    """Insert battery swaps into MySQL in batches."""
    session = Session()
    logger = get_run_logger()
    
    battery_ids = [random.randint(1000, 9999) for _ in range(batch_size)]
    
    swaps = []
    for _ in range(batch_size):
        station_id = random.randint(1, len(STATION_NAMES))  
        swaps.append(
            BatterySwap(
                battery_id=random.choice(battery_ids),
                station_id=station_id,  
                station_name=STATION_NAMES[station_id - 1],  
                rider_id=random.randint(1000, 9999),  
                swap_timestamp=fake.date_time_between(start_date='-1y', end_date='now'),
                battery_status=random.choice(['depleted', 'charged', 'in_use'])
            )
        )

    session.add_all(swaps)
    
    try:
        session.commit()
        logger.info(f"{batch_size} Battery swaps inserted.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error inserting battery swaps: {e}")
    finally:
        session.close()


@task
def check_record_count():
    """Check total records in database and return True if under 100,000."""
    session = Session()
    total_records = (
        session.query(Customer).count() + 
        session.query(Order).count() + 
        session.query(BatterySwap).count()
    )
    session.close()
    return total_records < 100000


@flow
def incremental_data_generator():
    """Prefect Flow to incrementally insert data every few minutes until 100k records are reached."""
    logger = get_run_logger()
    logger.info("Starting incremental data generation...")

    while check_record_count():
        insert_customers(100)
        insert_orders(500)
        insert_battery_swaps(1000)

        logger.info("Waiting before next batch...")
        time.sleep(120) 

    logger.info("100,000 records reached. Stopping data generation.")


# Run Prefect Flow
if __name__ == "__main__":
    incremental_data_generator()
