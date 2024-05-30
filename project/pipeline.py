import pandas as pd
import sqlite3
import requests
import io
import gzip
from io import BytesIO
import os

def fetch_data(url, compressed=False):
    print(f"Fetching data from {url}...")
    response = requests.get(url)
    if compressed:
        print("Decompressing data...")
        return gzip.decompress(response.content)
    else:
        return response.content.decode('utf-8')

def save_to_sqlite(df, db_path, table_name):
    print(f"Saving data to {table_name} table in {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table without auto-incremented primary key
    if table_name == 'accident':
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            month TEXT,
            crashes INTEGER
        );
        """)
    elif table_name == 'weather':
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY,
            month TEXT,
            tavg REAL,
            snow REAL,
            prcp REAL,
            wspd REAL
        );
        """)
    
    # Insert data into table
    for row in df.itertuples(index=False):
        if table_name == 'accident':
            cursor.execute(f"""
            INSERT INTO {table_name} (month, crashes) VALUES (?, ?)
            """, (row.month, row.crashes))
        elif table_name == 'weather':
            cursor.execute(f"""
            INSERT INTO {table_name} (month, tavg, snow, prcp, wspd) VALUES (?, ?, ?, ?, ?)
            """, (row.month, row.tavg, row.snow, row.prcp, row.wspd))
    
    conn.commit()
    conn.close()


def transform_accident_data(data):
    print("Transforming accident data...")
    
    # Read the CSV data into a DataFrame
    df = pd.read_csv(io.StringIO(data))
    
    # Convert 'Crash_Date' to datetime format
    df['Crash_Date'] = pd.to_datetime(df['Crash_Date'], format="%m/%d/%Y %I:%M:%S %p")
    
    # Filter for the year 2023
    df_2023 = df[df['Crash_Date'].dt.year == 2023]
    
    # Extract month from 'Crash_Date'
    df_2023['month'] = df_2023['Crash_Date'].dt.month_name().str.title().str[:3]
    
    # Define the chronological order of months
    month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    
    # Convert 'month' column to a categorical type with the defined order
    df_2023['month'] = pd.Categorical(df_2023['month'], categories=month_order, ordered=True)
    
    # Group by month and count occurrences
    monthly_data = df_2023['month'].value_counts().sort_index().reset_index()
    monthly_data.columns = ['month', 'crashes']
    
    return monthly_data

def transform_weather_data(data):
    print("Transforming weather data...")
    selected_columns = [0, 3, 4, 5, 8]
    df = pd.read_csv(BytesIO(data), header=None, usecols=selected_columns)
    df.columns = ['date', 'tavg', 'snow', 'prcp', 'wspd']
    df['date'] = pd.to_datetime(df['date'])
    df_2023 = df[df['date'].dt.year == 2023]
    df_2023 = df_2023.dropna()
    df_2023['month'] = df_2023['date'].dt.strftime('%b')
    monthly_avg = df_2023.groupby('month').mean().reset_index()
    months_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_avg['month'] = pd.Categorical(monthly_avg['month'], categories=months_order, ordered=True)
    monthly_avg = monthly_avg.sort_values('month')
    monthly_avg = monthly_avg.drop(columns=['date'], errors='ignore')
    monthly_avg[['tavg', 'snow', 'prcp', 'wspd']] = monthly_avg[['tavg', 'snow', 'prcp', 'wspd']].round(2)
    return monthly_avg

def main():
    # Ensure the ../data directory exists
    os.makedirs('../data', exist_ok=True)
    
    db_path = '../data/MADE.sqlite'
    
    # Process Accident data
    accident_url = "https://data.cityofchicago.org/api/views/gzaz-isa6/rows.csv"
    accident_data = fetch_data(accident_url)
    accident_df = transform_accident_data(accident_data)
    save_to_sqlite(accident_df, db_path, 'accident')
    print("Monthly accident data for the year 2023 has been saved to SQLite database.")
    
    # Process weather data
    weather_url = "https://bulk.meteostat.net/v2/hourly/72534.csv.gz"
    weather_data = fetch_data(weather_url, compressed=True)
    weather_df = transform_weather_data(weather_data)
    save_to_sqlite(weather_df, db_path, 'weather')
    print("Monthly averaged data for the year 2023 has been saved to SQLite database.")

if __name__ == "__main__":
    main()
