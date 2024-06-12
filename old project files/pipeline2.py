import pandas as pd
import sqlite3
import requests
import io
import gzip
from io import BytesIO
import os

def download_data(url, is_compressed=False):
    print(f"Downloading data from {url}...")
    response = requests.get(url)
    if is_compressed:
        print("Decompressing data...")
        return gzip.decompress(response.content)
    return response.content.decode('utf-8')

def store_in_sqlite(df, database_path, table):
    print(f"Storing data in {table} table in {database_path}...")
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    if table == 'accident':
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY,
            month TEXT,
            incidents INTEGER
        );
        """)
    elif table == 'weather':
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id INTEGER PRIMARY KEY,
            month TEXT,
            avg_temp REAL,
            snowfall REAL,
            precipitation REAL,
            wind_speed REAL
        );
        """)
    
    for row in df.itertuples(index=False):
        if table == 'accident':
            cursor.execute(f"""
            INSERT INTO {table} (month, incidents) VALUES (?, ?)
            """, (row.month, row.incidents))
        elif table == 'weather':
            cursor.execute(f"""
            INSERT INTO {table} (month, avg_temp, snowfall, precipitation, wind_speed) VALUES (?, ?, ?, ?, ?)
            """, (row.month, row.avg_temp, row.snowfall, row.precipitation, row.wind_speed))
    
    conn.commit()
    conn.close()

def process_accident_data(data):
    print("Processing accident data...")
    
    df = pd.read_csv(io.StringIO(data))
    df['Crash_Date'] = pd.to_datetime(df['Crash_Date'], format="%m/%d/%Y %I:%M:%S %p")
    df_2023 = df[df['Crash_Date'].dt.year == 2023]
    df_2023['month'] = df_2023['Crash_Date'].dt.month_name().str.title().str[:3]
    
    month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    df_2023['month'] = pd.Categorical(df_2023['month'], categories=month_order, ordered=True)
    
    monthly_data = df_2023['month'].value_counts().sort_index().reset_index()
    monthly_data.columns = ['month', 'incidents']
    
    return monthly_data

def process_weather_data(data):
    print("Processing weather data...")
    selected_columns = [0, 3, 4, 5, 8]
    df = pd.read_csv(BytesIO(data), header=None, usecols=selected_columns)
    df.columns = ['date', 'avg_temp', 'snowfall', 'precipitation', 'wind_speed']
    df['date'] = pd.to_datetime(df['date'])
    df_2023 = df[df['date'].dt.year == 2023]
    df_2023 = df_2023.dropna()
    df_2023['month'] = df_2023['date'].dt.strftime('%b')
    
    monthly_avg = df_2023.groupby('month').mean().reset_index()
    month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    monthly_avg['month'] = pd.Categorical(monthly_avg['month'], categories=month_order, ordered=True)
    monthly_avg = monthly_avg.sort_values('month')
    monthly_avg = monthly_avg.drop(columns=['date'], errors='ignore')
    monthly_avg[['avg_temp', 'snowfall', 'precipitation', 'wind_speed']] = monthly_avg[['avg_temp', 'snowfall', 'precipitation', 'wind_speed']].round(2)
    
    return monthly_avg

def execute_pipeline():
    os.makedirs('../data', exist_ok=True)
    
    database_path = '../data/MADE.sqlite'
    
    # Process accident data
    accident_url = "https://data.cityofchicago.org/api/views/gzaz-isa6/rows.csv"
    accident_data = download_data(accident_url)
    accident_df = process_accident_data(accident_data)
    store_in_sqlite(accident_df, database_path, 'accident')
    print("Monthly accident data for the year 2023 has been saved to SQLite database.")
    
    # Process weather data
    weather_url = "https://bulk.meteostat.net/v2/hourly/72534.csv.gz"
    weather_data = download_data(weather_url, is_compressed=True)
    weather_df = process_weather_data(weather_data)
    store_in_sqlite(weather_df, database_path, 'weather')
    print("Monthly averaged weather data for the year 2023 has been saved to SQLite database.")

if __name__ == "__main__":
    execute_pipeline()
