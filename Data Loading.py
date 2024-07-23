import sqlite3
import pandas as pd

def create_database(db_name="nyc_taxi.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trips (
        pickup_datetime TEXT,
        dropoff_datetime TEXT,
        trip_distance REAL,
        fare_amount REAL,
        passenger_count INTEGER,
        trip_duration REAL,
        average_speed REAL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_agg (
        date TEXT,
        total_trips INTEGER,
        average_fare REAL
    )
    ''')
    
    conn.commit()
    return conn

def load_data_to_db(conn, csv_file, table_name):
    df = pd.read_csv(csv_file)
    df.to_sql(table_name, conn, if_exists='append', index=False)

if __name__ == "__main__":
    conn = create_database()
    
    # Load processed data into the database
    load_data_to_db(conn, "processed/nyc_taxi_data_2019.csv", "trips")
    load_data_to_db(conn, "processed/nyc_taxi_daily_agg_2019.csv", "daily_agg")
    
    conn.close()
