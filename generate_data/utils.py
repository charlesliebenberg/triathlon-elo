import requests
import json
import time
from datetime import datetime
import random
import os

# API Key and base URL
API_KEY = "def808e44acc15fa2d8b96005075478e"
BASE_URL = "https://api.triathlon.org/v1"

# Headers for API requests
HEADERS = {
    "apikey": API_KEY
}

# Constants
CURRENT_YEAR = datetime.now().year
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

def pretty_print(data):
    """Print JSON data in a more readable format"""
    if data:
        print(json.dumps(data, indent=2))
    else:
        print("No data available")

def load_data_from_json(filename):
    """
    Load data from a JSON file
    
    Params:
        filename (str): The filename to load from
        
    Returns:
        dict: The loaded data or None if file doesn't exist
    """
    # Check if the path already includes the data directory
    if os.path.dirname(filename) == DATA_DIR or os.path.isabs(filename):
        filepath = filename
    else:
        # Prepend the data directory
        filepath = os.path.join(DATA_DIR, filename)
        
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File {filepath} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {filepath}.")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def save_data_to_json(data, filename):
    """
    Save collected data to a JSON file
    
    Params:
        data (dict): The data to save
        filename (str): The filename to save to
    """
    # Check if the path already includes the data directory
    if os.path.dirname(filename) == DATA_DIR or os.path.isabs(filename):
        filepath = filename
    else:
        # Prepend the data directory
        filepath = os.path.join(DATA_DIR, filename)
        
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"\nData saved to {filepath}")
        return True
    except Exception as e:
        print(f"Error saving data: {e}")
        return False

def is_elite_men_program(program_name):
    """
    Check if a program name represents an Elite Men's competition
    
    Params:
        program_name (str): The name of the program
        
    Returns:
        bool: True if it's an Elite Men's program, False otherwise
    """
    if not program_name:
        return False
    
    program_name = program_name.strip().lower()
    
    # Exact matches - prioritize these
    valid_elite_men_programs = [
        "elite men", 
        "men elite", 
        "men's elite", 
        "elite men's"
    ]
    
    # If it's an exact match, return immediately
    if program_name in valid_elite_men_programs:
        return True
    
    # Special case: when it's just "men" with no other gender or age category
    if program_name == "men":
        return True
        
    # Check for elite men with more flexible pattern matching
    if ("elite" in program_name and "men" in program_name) or \
       ("championship" in program_name and "men" in program_name):
        # Exclude programs with other categories or qualifiers
        exclusions = ["junior", "u23", "para", "youth", "age", "relay", "mixed", "team", "women"]
        for exclusion in exclusions:
            if exclusion in program_name:
                return False
        return True
    print(program_name)    
    # If we get here, it's not a match
    return False

def make_api_request(url, params=None, max_retries=3, base_delay=1.0):
    """
    Make an API request with improved retry logic using exponential backoff
    
    Params:
        url (str): The URL to request
        params (dict): Optional parameters for the request
        max_retries (int): Maximum number of retries
        base_delay (float): Base delay in seconds between retries (will increase exponentially)
        
    Returns:
        dict: The JSON response data if successful, None otherwise
    """
    for retry in range(max_retries + 1):  # +1 to include the initial attempt
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "success":
                    return data
                else:
                    error_msg = f"API Error: {data.get('message')}"
                    print(error_msg)
            elif response.status_code == 429:  # Too Many Requests
                print(f"Rate limited (429) for URL {url}")
            else:
                print(f"HTTP Error: {response.status_code} for URL {url}")
                if response.text:
                    print(response.text[:200])  # Print first 200 chars of response
            
            # Exit early if this was the last retry attempt
            if retry >= max_retries:
                break
                
            # Calculate delay with exponential backoff and jitter
            delay = base_delay * (2 ** retry) + (random.random() * 0.5)
            print(f"  Retrying in {delay:.2f}s (attempt {retry+2}/{max_retries+1})")
            time.sleep(delay)
            
        except requests.RequestException as e:
            print(f"Request error: {e} for URL {url}")
            
            # Exit early if this was the last retry attempt
            if retry >= max_retries:
                break
                
            # Calculate delay with exponential backoff and jitter
            delay = base_delay * (2 ** retry) + (random.random() * 0.5)
            print(f"  Retrying in {delay:.2f}s (attempt {retry+2}/{max_retries+1})")
            time.sleep(delay)
        except Exception as e:
            print(f"Unexpected error: {e} for URL {url}")
            
            # Exit early if this was the last retry attempt
            if retry >= max_retries:
                break
                
            # Calculate delay with exponential backoff and jitter
            delay = base_delay * (2 ** retry) + (random.random() * 0.5)
            print(f"  Retrying in {delay:.2f}s (attempt {retry+2}/{max_retries+1})")
            time.sleep(delay)
    
    return None 