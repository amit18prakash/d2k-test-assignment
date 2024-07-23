import pandas as pd
import os

def load_data(year, month):
    file_path = f"data/{year}/yellow_tripdata_{year}-{month:02d}.csv"
    return pd.read_csv(file_path)

def clean_and_transform(df):
    # Remove rows with missing or corrupt data
    df = df.dropna()
    
    # Convert pickup and dropoff times to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    
    # Derive new columns
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60
    df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60)
    
    # Aggregate data to calculate total trips and average fare per day
    df['date'] = df['tpep_pickup_datetime'].dt.date
    daily_agg = df.groupby('date').agg(
        total_trips=('trip_distance', 'size'),
        average_fare=('fare_amount', 'mean')
    ).reset_index()
    
    return df, daily_agg

def process_year_data(year):
    all_data = []
    all_daily_agg = []
    
    for month in range(1, 13):
        print(f"Processing {year}-{month:02d}")
        df = load_data(year, month)
        df, daily_agg = clean_and_transform(df)
        
        all_data.append(df)
        all_daily_agg.append(daily_agg)
    
    full_data = pd.concat(all_data, ignore_index=True)
    full_daily_agg = pd.concat(all_daily_agg, ignore_index=True)
    
    return full_data, full_daily_agg

if __name__ == "__main__":
    full_data, full_daily_agg = process_year_data(2019)
    
    # Save processed data
    full_data.to_csv("processed/nyc_taxi_data_2019.csv", index=False)
    full_daily_agg.to_csv("processed/nyc_taxi_daily_agg_2019.csv", index=False)
