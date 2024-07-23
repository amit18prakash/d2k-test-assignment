import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def query_db(conn, query):
    return pd.read_sql_query(query, conn)

if __name__ == "__main__":
    conn = sqlite3.connect("nyc_taxi.db")
    
    # Peak hours for taxi usage
    peak_hours_query = '''
    SELECT strftime('%H', pickup_datetime) AS hour, COUNT(*) AS total_trips
    FROM trips
    GROUP BY hour
    ORDER BY total_trips DESC
    '''
    peak_hours = query_db(conn, peak_hours_query)
    
    plt.figure(figsize=(12, 6))
    plt.bar(peak_hours['hour'], peak_hours['total_trips'])
    plt.xlabel('Hour of the Day')
    plt.ylabel('Total Trips')
    plt.title('Peak Hours for Taxi Usage')
    plt.show()
    
    # Passenger count vs trip fare
    passenger_fare_query = '''
    SELECT passenger_count, AVG(fare_amount) AS average_fare
    FROM trips
    GROUP BY passenger_count
    '''
    passenger_fare = query_db(conn, passenger_fare_query)
    
    plt.figure(figsize=(12, 6))
    plt.bar(passenger_fare['passenger_count'], passenger_fare['average_fare'])
    plt.xlabel('Passenger Count')
    plt.ylabel('Average Fare')
    plt.title('Passenger Count vs Trip Fare')
    plt.show()
    
    # Trends in usage over the year
    trends_query = '''
    SELECT date, total_trips
    FROM daily_agg
    ORDER BY date
    '''
    trends = query_db(conn, trends_query)
    trends['date'] = pd.to_datetime(trends['date'])
    
    plt.figure(figsize=(12, 6))
    plt.plot(trends['date'], trends['total_trips'])
    plt.xlabel('Date')
    plt.ylabel('Total Trips')
    plt.title('Trends in Taxi Usage Over the Year')
    plt.show()
    
    conn.close()
