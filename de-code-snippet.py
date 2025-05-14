# SuperCourier - Mini ETL Pipeline
# Starter code for the Data Engineering mini-challenge

import sqlite3
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import random
import os

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('supercourier_mini_etl')

# Constants
DB_PATH = 'supercourier_mini.db'
WEATHER_PATH = 'weather_data.json'
OUTPUT_PATH = 'deliveries.csv'

# 1. FUNCTION TO GENERATE SQLITE DATABASE (you can modify as needed)
def create_sqlite_database():
    """
    Creates a simple SQLite database with a deliveries table
    """
    logger.info("Creating SQLite database...")
    
    # Remove database if it already exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create deliveries table
    cursor.execute('''
    CREATE TABLE deliveries (
        delivery_id INTEGER PRIMARY KEY,
        pickup_datetime TEXT,
        package_type TEXT,
        delivery_zone TEXT,
        recipient_id INTEGER
    )
    ''')
    
    # Available package types and delivery zones
    package_types = ['Small', 'Medium', 'Large', 'X-Large', 'Special']
    delivery_zones = ['Urban', 'Suburban', 'Rural', 'Industrial', 'Shopping Center']
    
    # Generate 1000 random deliveries
    deliveries = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # 3 months
    
    for i in range(1, 1001):
        # Random date within last 3 months
        timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Random selection of package type and zone
        package_type = random.choices(
            package_types, 
            weights=[25, 30, 20, 15, 10]  # Relative probabilities
        )[0]
        
        delivery_zone = random.choice(delivery_zones)
        
        # Add to list
        deliveries.append((
            i,  # delivery_id
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # pickup_datetime
            package_type,
            delivery_zone,
            random.randint(1, 100)  # fictional recipient_id
        ))
    
    # Insert data
    cursor.executemany(
        'INSERT INTO deliveries VALUES (?, ?, ?, ?, ?)',
        deliveries
    )
    
    # Commit and close
    conn.commit()
    conn.close()
    
    logger.info(f"Database created with {len(deliveries)} deliveries")
    return True

# 2. FUNCTION TO GENERATE WEATHER DATA
def generate_weather_data():
    """
    Generates fictional weather data for the last 3 months
    """
    logger.info("Generating weather data...")
    
    conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Snowy', 'Foggy']
    weights = [30, 25, 20, 15, 5, 5]  # Relative probabilities
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    weather_data = {}
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weather_data[date_str] = {}
        
        # For each day, generate weather for each hour
        for hour in range(24):
            # More continuity in conditions
            if hour > 0 and random.random() < 0.7:
                # 70% chance of keeping same condition as previous hour
                condition = weather_data[date_str].get(str(hour-1), 
                                                      random.choices(conditions, weights=weights)[0])
            else:
                condition = random.choices(conditions, weights=weights)[0]
            
            weather_data[date_str][str(hour)] = condition
        
        current_date += timedelta(days=1)
    
    # Save as JSON
    with open(WEATHER_PATH, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Weather data generated for period {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    return weather_data

# 3. EXTRACTION FUNCTIONS (to be completed)
def extract_sqlite_data():
    """
    Extracts delivery data from SQLite database
    """
    logger.info("Extracting data from SQLite database...")
    
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM deliveries"
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"Extraction complete: {len(df)} records")
    return df

def load_weather_data():
    """
    Loads weather data from JSON file
    """
    logger.info("Loading weather data...")
    
    with open(WEATHER_PATH, 'r', encoding='utf-8') as f:
        weather_data = json.load(f)
    
    logger.info(f"Weather data loaded for {len(weather_data)} days")
    return weather_data

# 4. TRANSFORMATION FUNCTIONS (to be completed by participants)


def enrich_with_weather(df, weather_data):
    """
    Enriches the DataFrame with weather conditions
    Enriches the DataFrame with new columns : Weekday, Hour
    Change feature names
    Delete one feature
    Add Distance feature to the dataframe
    """
    logger.info("Enriching with weather data...")
    
    # Convert date column to datetime
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    
    # Function to get weather for a given timestamp
    def get_weather(timestamp):
        date_str = timestamp.strftime('%Y-%m-%d')
        hour_str = str(timestamp.hour)
        
        try:
            return weather_data[date_str][hour_str]
        except KeyError:
            return None
    
    # Apply function to each row
    df['WeatherCondition'] = df['pickup_datetime'].apply(get_weather)

    #Create new features
    #Added 15/05/
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
    df['Hour'] = df['pickup_datetime'].dt.hour.astype(str)
    df['Weekday'] = df['pickup_datetime'].dt.day_name()
    
    #Change feature names
    df = df.rename(columns={'delivery_id': 'Delivery_ID',
                            'pickup_datetime' : 'Pickup_DateTime',
                            'package_type': 'Package_Type',
                            'delivery_zone' : 'Delivery_Zone',
                            'WeatherCondition' : 'Weather_Condition'
                              })

    #Delete 
    df = df.drop(['recipient_id'], axis=1)
    
    #Add distance feature
    df['Distance'] = np.random.uniform(5,100, size=len(df))
    df['Distance'] = df['Distance'].round(2)

    return df

#Function to calculate theoretical ajusted delivery time
def calculate_ajusted_theoretical_time(row):
    """
    Calculates the theoretical delivery time before random variation
    """
    # Initaliaze coeffs
    coeffs_package_type = {
        'Small': 1.0,
        'Medium': 1.2,
        'Large': 1.5,
        'X-Large': 2.0,
        'Special': 2.5
    }

    coeffs_zone = {
        'Urban': 1.2,
        'Suburban': 1.0,
        'Rural': 1.3,
        'Industrial': 0.9,
        'Shopping Center': 1.4
    }

    coeffs_weather = {
        'Sunny': 1.0,
        'Cloudy': 1.05,
        'Rainy': 1.2,
        'Windy': 1.1,
        'Snowy': 1.8,
        'Foggy': 1.3,
        'Unknown': 1.0
    }
    # base_theorical_time : 30 + distance * 0.8 minutes
    base_theorical_time = 30 + row['Distance'] * 0.8
        
    # Apply package factor
    adjusted_theorical_time = base_theorical_time * coeffs_package_type[row['Package_Type']]
        
        
    adjusted_theorical_time = adjusted_theorical_time * coeffs_zone[row['Delivery_Zone']]
        
    # Apply time of day factor
    hour = int(row['Hour'])
    if 7 <= hour < 10:  # Morning peak
        adjusted_theorical_time *= 1.3
    elif 16 <= hour < 19:  # Evening peak
        adjusted_theorical_time *= 1.4
        
    # Apply day of week factor
    weekday = row['Weekday']
    if weekday in ['Monday', 'Tuesday', 'Wednesday', 'Thursday','Friday']:
        adjusted_theorical_time *= 1.2
    elif weekday in ['Saturday', 'Sunday']:
        adjusted_theorical_time *= 0.9

    # Apply weather factor
    weather_factor = coeffs_weather.get(row.get('Weather_Condition', 'Unknown'), 1.0)
    adjusted_theorical_time *= weather_factor

    return adjusted_theorical_time

def transform_data(df_deliveries, weather_data):
    """
    Main data transformation function
    To be completed by participants
    """
  
    logger.info("Transforming data...")
    # 1 Enrich with weather data
    df_deliveries = enrich_with_weather(df_deliveries, weather_data)

    # 2. Calculate "ajusted theoritical delivery time"
    logger.info("Calculate ajusted theorical delivery time...")
    df_deliveries['ajusted_theoretical_time'] = df_deliveries.apply(calculate_ajusted_theoretical_time, axis=1)

    # 3. Calculate Delay threshold
    logger.info("Calculate delay threshold...")
    df_deliveries['Delay_threshold'] = df_deliveries['ajusted_theoretical_time'] * 1.2

    # 4. Calculate actual delivery time (with random variation)
    logger.info("Calculate actual delivery time...")
    df_deliveries['Actual_Delivery_Time'] = df_deliveries['ajusted_theoretical_time'].apply(
        lambda x: round(max(10, np.random.normal(x, x * 0.15)), 1)
    )

    # 5. Determine Status
    logger.info("Determine the status of the delivery...")
    df_deliveries['Status'] = df_deliveries.apply(
        lambda row: 'Delayed' if row['Actual_Delivery_Time'] > row['Delay_threshold'] else 'On-time',
        axis=1
    )

    logger.info("End of the transformation.")
    
    cols = ['Delivery_ID', 'Pickup_DateTime', 'Weekday', 'Hour', 'Package_Type', 'Distance','Delivery_Zone',
                        'Weather_Condition', 'Actual_Delivery_Time', 'Status' ]
    
    df_deliveries = df_deliveries[cols]

    return df_deliveries  # Return transformed DataFrame

# Validation dataframe

def validation_dataframe(df, required_columns):
    """
    Validate the dataframe
    all columns are present
    no null values
    """
    logger.info("Start validation of the dataframe...")
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    if df[required_columns].isnull().any().any():
        raise ValueError("The dataframe contains null values")

    logger.info("Validation of the dataframe completed.")

# 5. LOADING FUNCTION (to be completed)
def save_results(df):
    """
    Saves the final DataFrame to CSV
    """
    try:
        logger.info("Saving results...")
    
        # Validation dataframe
        expected_columns = ['Delivery_ID', 'Pickup_DateTime', 'Weekday', 'Hour', 'Package_Type', 'Distance','Delivery_Zone',
                        'Weather_Condition', 'Actual_Delivery_Time', 'Status' ]
    
        validation_dataframe(df, expected_columns)

        # delete CSV if exists
        if os.path.exists(OUTPUT_PATH):
            logger.warning(f"Existing file detected {OUTPUT_PATH}, deletion in progress...")
            os.remove(OUTPUT_PATH)
        
        # Save to CSV
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Results saved to  {OUTPUT_PATH}")
        return True
    except Exception as e:
        logger.error(f"Error when saving data : {e}")
        return False

# MAIN FUNCTION
def run_pipeline():
    """
    Runs the ETL pipeline end-to-end
    """
    try:
        logger.info("Starting SuperCourier ETL pipeline")
        
        # Step 1: Generate data sources
        create_sqlite_database()
        weather_data = generate_weather_data()
        
        # Step 2: Extraction
        df_deliveries = extract_sqlite_data()
        
        # Step 3: Transformation
        df_transformed = transform_data(df_deliveries, weather_data)
        
        # Step 4: Loading
        save_results(df_transformed)
        
        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during pipeline execution: {str(e)}")
        return False

# Main entry point
if __name__ == "__main__":
    run_pipeline()
