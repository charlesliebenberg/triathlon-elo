#!/usr/bin/env python3
"""
Script to pre-generate monthly ratings data as a static JSON file.
This script should be run before starting the web app to ensure
the data is ready to be served quickly.
"""

import os
import sys
import json
import time
from datetime import datetime

# Add parent directory to path so we can import from there
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

# Configure database path
DB_PATH = os.environ.get('TRIATHLON_DB_PATH', os.path.join(parent_dir, 'data', 'triathlon.db'))

# Import the function from monthly_top_ratings.py
try:
    from monthly_top_ratings import get_monthly_top_ratings
except ImportError as e:
    print(f"Error importing monthly_top_ratings module: {e}")
    sys.exit(1)

def get_monthly_ratings_json(db_path):
    """
    Fetch monthly ratings data and convert to JSON-serializable format.
    
    Args:
        db_path (str): Path to the SQLite database file
        
    Returns:
        list: A list of monthly data objects ready for JSON serialization
    """
    start_time = time.time()
    print(f"Fetching data from database: {db_path}")
    
    data_by_month = get_monthly_top_ratings(db_path)
    
    print(f"Data fetched in {time.time() - start_time:.2f} seconds")
    print(f"Processing {len(data_by_month)} months of data")
    
    # Transform into a list of objects
    output = []
    for month_key, athletes in data_by_month.items():
        athlete_list = []
        for row in athletes:
            # Unpack all fields, now including profile_image_url
            athlete_id, name, date, rating, event, profile_image_url = row
            # This is a placeholder - in a real app, you'd get this from the DB
            country_code = "USA"
            
            # Use profile_image_url if available, otherwise use a placeholder
            image_url = profile_image_url if profile_image_url else f"/static/img/athletes/{athlete_id}.jpg"
            
            athlete_list.append({
                "id": athlete_id,
                "name": name,
                "date": date,
                "rating": float(rating),  # Ensure it's a float for JSON
                "event": event,
                "country": country_code,
                "flag": f"/static/img/flags/{country_code.lower()}.png",
                "profile_image": image_url
            })
        output.append({
            "month": month_key,
            "athletes": athlete_list
        })
    
    # Sort by month chronologically
    output.sort(key=lambda x: x["month"])
    
    print(f"Data processing complete in {time.time() - start_time:.2f} seconds total")
    return output

def save_data_to_json(data, file_path):
    """
    Save data to a JSON file.
    
    Args:
        data: The data to save
        file_path (str): Path to the output JSON file
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Data saved to {file_path}")
    print(f"File size: {os.path.getsize(file_path) / 1024:.2f} KB")

if __name__ == "__main__":
    # Output file path
    static_data_path = os.path.join(os.path.dirname(__file__), 'static', 'data')
    output_file = os.path.join(static_data_path, 'monthly_ratings.json')
    
    print(f"Generating static data file: {output_file}")
    print(f"Using database: {DB_PATH}")
    
    # Generate the data
    data = get_monthly_ratings_json(DB_PATH)
    
    # Save to file
    save_data_to_json(data, output_file)
    
    print("Data generation complete, you can now start the web app")