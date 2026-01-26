import pandas as pd
from sqlalchemy import create_engine
import os

def ingest_data():
    # Database connection parameters
    # Note: Using localhost:5433 because we are running this script from the HOST machine,
    # and the docker-compose maps 5433->5432.
    engine = create_engine('postgresql://postgres:postgres@localhost:5433/ny_taxi')
    
    # Ingest Green Taxi Data
    parquet_file = 'green_tripdata_2025-11.parquet'
    if os.path.exists(parquet_file):
        print("Reading parquet file...")
        df = pd.read_parquet(parquet_file)
        
        print("Creating table schema...")
        df.head(0).to_sql(name='green_taxi_trips', con=engine, if_exists='replace')
        
        print("Inserting data...")
        # Chunking might be necessary for very large files, but 1 month of green taxi data is usually manageable.
        # However, for safety and better feedback, let's just push it.
        df.to_sql(name='green_taxi_trips', con=engine, if_exists='append')
        print(f"Inserted {len(df)} rows into green_taxi_trips")
    else:
        print(f"File {parquet_file} not found.")

    # Ingest Zones Data
    csv_file = 'taxi_zone_lookup.csv'
    if os.path.exists(csv_file):
        print("Reading CSV file...")
        df_zones = pd.read_csv(csv_file)
        
        print("Inserting zones data...")
        df_zones.to_sql(name='zones', con=engine, if_exists='replace')
        print(f"Inserted {len(df_zones)} rows into zones")
    else:
        print(f"File {csv_file} not found.")

if __name__ == '__main__':
    ingest_data()
