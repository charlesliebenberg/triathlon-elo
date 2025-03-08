import os
import sys
import json
import time
from flask import Flask, jsonify, render_template, send_from_directory

app = Flask(__name__)

# Path to the static data file
STATIC_DATA_FILE = os.path.join(app.static_folder, 'data', 'monthly_ratings.json')

def get_monthly_ratings_json():
    """Load pre-generated monthly ratings data from the static JSON file."""
    start_time = time.time()
    
    try:
        if not os.path.exists(STATIC_DATA_FILE):
            app.logger.error(f"Static data file not found: {STATIC_DATA_FILE}")
            app.logger.error("Please run generate_data.py before starting the app")
            return []
        
        with open(STATIC_DATA_FILE, 'r') as f:
            data = json.load(f)
        
        app.logger.info(f"Data loaded from {STATIC_DATA_FILE} in {time.time() - start_time:.4f} seconds")
        return data
    
    except Exception as e:
        app.logger.error(f"Error loading static data: {str(e)}")
        return []

@app.route("/")
def index():
    """Render the main visualization page."""
    return render_template("index.html")

@app.route("/monthly-data")
def monthly_data():
    """API endpoint that returns the monthly ratings data as JSON."""
    try:
        data = get_monthly_ratings_json()
        if not data:
            return jsonify({"error": "No data available", "message": "Please run generate_data.py first"}), 500
        return jsonify(data)
    except Exception as e:
        app.logger.error(f"Server error: {str(e)}")
        return jsonify({"error": "Server error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)