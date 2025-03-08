import sys
import time
import asyncio
import aiohttp
from datetime import datetime
from collections import defaultdict
import os

# Directory paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")

# Import local modules
try:
    from .utils import (
        BASE_URL, HEADERS, load_data_from_json, save_data_to_json, make_api_request
    )
except ImportError:
    # Fallback for direct script execution
    from utils import (
        BASE_URL, HEADERS, load_data_from_json, save_data_to_json, make_api_request
    )

# Async version of make_api_request
async def async_make_api_request(url, params=None, session=None):
    """
    Make an async API request and return the response data
    
    Params:
        url (str): The URL to request
        params (dict): Optional query parameters
        session (aiohttp.ClientSession): Optional session to reuse
        
    Returns:
        dict: Response data from the API or None if error
    """
    if session is None:
        # Create a new session if one wasn't provided
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            return await _perform_async_request(session, url, params)
    else:
        # Use the provided session
        return await _perform_async_request(session, url, params)

async def _perform_async_request(session, url, params=None):
    """Helper function to perform the actual async request"""
    try:
        async with session.get(url, params=params, timeout=30) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"API Error: Status {response.status} for URL {url}")
                return None
    except Exception as e:
        print(f"Request Error: {e} for URL {url}")
        return None

async def get_athlete_details_async(athlete_id, session=None):
    """
    Async version to get detailed information about a specific athlete
    
    Params:
        athlete_id (int): ID of the athlete
        session (aiohttp.ClientSession): Session to reuse for requests
    
    Returns:
        dict: Response data from the API
    """
    url = f"{BASE_URL}/athletes/{athlete_id}"
    
    print(f"👤 Fetching details for athlete ID: {athlete_id}")
    data = await async_make_api_request(url, session=session)
    
    if not data:
        return None
    
    return data.get("data", {})

def get_athlete_details(athlete_id):
    """
    Get detailed information about a specific athlete
    
    Params:
        athlete_id (int): ID of the athlete
    
    Returns:
        dict: Response data from the API
    """
    url = f"{BASE_URL}/athletes/{athlete_id}"
    
    print(f"👤 Fetching details for athlete ID: {athlete_id}")
    data = make_api_request(url)
    
    if not data:
        return None
    
    return data.get("data", {})

def extract_athlete_results_from_data(athlete_id, results_data, min_year, max_year):
    """
    Extract all results for a specific athlete from the existing results data
    
    Params:
        athlete_id (int): ID of the athlete
        results_data (dict): The loaded results data from results_data.json
        min_year (int): Start year to filter results
        max_year (int): End year to filter results
    
    Returns:
        dict: Dictionary of results grouped by year
    """
    print(f"📊 Extracting results for athlete ID: {athlete_id} from existing data")
    
    # Extract events data
    events = results_data.get("events", {})
    print(f"  Events count: {len(events)}")
    
    # OPTIMIZATION: Create lookup dictionaries using string keys only for consistency
    event_date_lookup = {}
    event_title_lookup = {}
    
    # Process events into a consistent lookup structure
    for event_id_key, event_data in events.items():
        # Get the date and title from the event
        event_date = event_data.get("date", "")
        event_title = event_data.get("title", "Unknown Event")
        
        # Always store as string keys for consistent lookup
        str_key = str(event_id_key)
        event_date_lookup[str_key] = event_date
        event_title_lookup[str_key] = event_title
    
    # Print a sample of the lookup data to verify
    print("  Sample from event lookup:")
    sample_keys = list(event_date_lookup.keys())[:5]
    for key in sample_keys:
        print(f"  - Event ID {key} ({type(key).__name__}): {event_date_lookup[key]}")
    
    # Get results data
    results = results_data.get("results", [])
    total_results = len(results)
    print(f"  Total results in dataset: {total_results}")
    
    # Print a sample of event IDs from the results
    print("  Sample from results data:")
    for i, result in enumerate(results[:3]):
        event_id = result.get("event_id")
        print(f"  - Result {i+1} event_id: {event_id} ({type(event_id).__name__})")
    
    # Test lookup with a sample event ID from results
    if results:
        sample_event_id = results[0].get("event_id")
        print(f"  Testing lookup with event ID {sample_event_id}:")
        # Convert to string for consistent lookup
        str_sample_event_id = str(sample_event_id)
        if str_sample_event_id in event_date_lookup:
            print(f"  - String lookup success: {event_date_lookup[str_sample_event_id]}")
        else:
            print(f"  - String lookup failed")
    
    # OPTIMIZATION: Normalize athlete_id to string for consistent comparison
    athlete_id_str = str(athlete_id)
    
    # OPTIMIZATION: Pre-filter results for the target athlete (drastically reduces iterations)
    athlete_results = []
    for result in results:
        result_athlete_id = result.get("athlete_id")
        if result_athlete_id is not None and str(result_athlete_id) == athlete_id_str:
            athlete_results.append(result)
    
    print(f"  Found {len(athlete_results)} initial results for athlete {athlete_id}")
    
    year_results = defaultdict(list)
    athlete_results_count = 0
    
    # Process only the pre-filtered results
    for result in athlete_results:
        # Get the event ID to find the date
        event_id = result.get("event_id")
        if not event_id:
            continue
            
        # Always convert to string for lookup
        event_id_str = str(event_id)
        
        # Get event date from our lookup dictionary
        event_date = event_date_lookup.get(event_id_str)
                    
        # Skip if no event date found
        if not event_date:
            continue
            
        # Extract year from date string
        try:
            year = int(event_date.split("-")[0])
        except (ValueError, IndexError, AttributeError):
            continue
            
        # Skip results outside our target year range
        if not year or year < min_year or year > max_year:
            continue
            
        # Get event title
        event_title = event_title_lookup.get(event_id_str, "Unknown Event")
        
        # Add to the results for this year
        year_str = str(year)
        
        # Debug the matching result
        print(f"  ✓ Found result: {event_title}, Position {result.get('position')}, Date {event_date}")
        
        year_results[year_str].append({
            "event_id": event_id,
            "event_title": event_title,
            "event_date": event_date,
            "prog_name": result.get("prog_name"),
            "position": result.get("position"),
            "total_time": result.get("total_time"),
            "points_earned": result.get("points", 0)
        })
        athlete_results_count += 1
    
    print(f"  ✅ Found {athlete_results_count} total results for athlete {athlete_id}")
        
    return year_results

async def process_athlete_data_async(athlete_id, results_data, min_year, max_year, max_retries=2, session=None):
    """
    Async version to process data for a specific athlete, including details and results for relevant years
    
    Params:
        athlete_id (int): ID of the athlete
        results_data (dict): The loaded results data from results_data.json
        min_year (int): Minimum year to collect results for
        max_year (int): Maximum year to collect results for
        max_retries (int): Maximum number of API call retries
        session (aiohttp.ClientSession): Session to reuse for requests
        
    Returns:
        dict: Processed athlete data
    """
    # Basic structure
    athlete_data = {
        "id": athlete_id,
        "details": None,
        "yearly_results": {},
        "performance_metrics": {
            "total_events": 0,
            "best_position": None,
            "avg_position": None,
            "total_points": 0
        }
    }
    
    # Get athlete details with retry logic
    details = None
    for retry in range(max_retries):
        try:
            details = await get_athlete_details_async(athlete_id, session)
            if details:
                break
            
            if retry < max_retries - 1:
                print(f"  ⚠️ Retry {retry+1}/{max_retries} for athlete {athlete_id} details...")
                await asyncio.sleep(0.5)  # Reduced retry delay using asyncio.sleep
        except Exception as e:
            print(f"  ⚠️ Error getting details for athlete {athlete_id}: {e}")
            if retry < max_retries - 1:
                print(f"  ⚠️ Retry {retry+1}/{max_retries}...")
                await asyncio.sleep(0.5)  # Reduced retry delay using asyncio.sleep
    
    if details:
        athlete_data["details"] = {
            "first_name": details.get("athlete_first"),
            "last_name": details.get("athlete_last"),
            "full_name": details.get("athlete_title"),
            "gender": details.get("athlete_gender"),
            "country": details.get("athlete_country_name"),
            "noc": details.get("athlete_noc"),
            "year_of_birth": details.get("athlete_yob"),
            "profile_image": details.get("athlete_profile_image")
        }
        
        # Add name to the main athlete data if available
        if details.get("athlete_title"):
            athlete_data["name"] = details.get("athlete_title")
    else:
        print(f"⚠️ Could not get details for athlete {athlete_id} after {max_retries} retries")
        return None
    
    # If this isn't a man, skip
    if athlete_data["details"]["gender"] != "male":
        print(f"⚠️ Skipping non-male athlete: {athlete_data['name']}")
        return None
    
    # Get all results for the athlete from existing data
    year_results = extract_athlete_results_from_data(athlete_id, results_data, min_year, max_year)
    
    # Process all results
    all_positions = []
    
    # Add results to athlete data and calculate metrics
    for year_str, results in year_results.items():
        if results:
            athlete_data["yearly_results"][year_str] = results
            athlete_data["performance_metrics"]["total_events"] += len(results)
            
            # Process positions for this year
            for result in results:
                position = result.get("position")
                if position and isinstance(position, int):
                    all_positions.append(position)
    
    # Calculate performance metrics
    if all_positions:
        athlete_data["performance_metrics"]["best_position"] = min(all_positions)
        athlete_data["performance_metrics"]["avg_position"] = sum(all_positions) / len(all_positions)
    
    # Add success metric - simple calculation based on positions
    total_points = 0
    for _, results in athlete_data["yearly_results"].items():
        for result in results:
            position = result.get("position")
            if position and isinstance(position, int):
                # Simple scoring system
                if position == 1:
                    points = 25
                elif position == 2:
                    points = 20
                elif position == 3:
                    points = 16
                elif position == 4:
                    points = 13
                elif position == 5:
                    points = 11
                elif position <= 10:
                    points = 11 - (position - 5)
                else:
                    points = 0
                
                total_points += points
                result["points_earned"] = points
    
    athlete_data["performance_metrics"]["total_points"] = total_points
    
    return athlete_data

def process_athlete_data(athlete_id, results_data, min_year, max_year, max_retries=2):
    """
    Process data for a specific athlete, including details and results for relevant years
    
    Params:
        athlete_id (int): ID of the athlete
        results_data (dict): The loaded results data from results_data.json
        min_year (int): Minimum year to collect results for
        max_year (int): Maximum year to collect results for
        max_retries (int): Maximum number of API call retries
        
    Returns:
        dict: Processed athlete data
    """
    # Basic structure
    athlete_data = {
        "id": athlete_id,
        "details": None,
        "yearly_results": {},
        "performance_metrics": {
            "total_events": 0,
            "best_position": None,
            "avg_position": None,
            "total_points": 0
        }
    }
    
    # OPTIMIZATION: Reduced max_retries from 3 to 2
    # Get athlete details with retry logic
    details = None
    for retry in range(max_retries):
        try:
            details = get_athlete_details(athlete_id)
            if details:
                break
            
            if retry < max_retries - 1:
                print(f"  ⚠️ Retry {retry+1}/{max_retries} for athlete {athlete_id} details...")
                time.sleep(0.5)  # OPTIMIZATION: Reduced retry delay from 1s to 0.5s
        except Exception as e:
            print(f"  ⚠️ Error getting details for athlete {athlete_id}: {e}")
            if retry < max_retries - 1:
                print(f"  ⚠️ Retry {retry+1}/{max_retries}...")
                time.sleep(0.5)  # OPTIMIZATION: Reduced retry delay from 1s to 0.5s
    
    if details:
        athlete_data["details"] = {
            "first_name": details.get("athlete_first"),
            "last_name": details.get("athlete_last"),
            "full_name": details.get("athlete_title"),
            "gender": details.get("athlete_gender"),
            "country": details.get("athlete_country_name"),
            "noc": details.get("athlete_noc"),
            "year_of_birth": details.get("athlete_yob"),
            "profile_image": details.get("athlete_profile_image")
        }
        
        # Add name to the main athlete data if available
        if details.get("athlete_title"):
            athlete_data["name"] = details.get("athlete_title")
    else:
        print(f"⚠️ Could not get details for athlete {athlete_id} after {max_retries} retries")
        return None
    
    # If this isn't a man, skip
    if athlete_data["details"]["gender"] != "male":
        print(f"⚠️ Skipping non-male athlete: {athlete_data['name']}")
        return None
    
    # Get all results for the athlete from existing data
    year_results = extract_athlete_results_from_data(athlete_id, results_data, min_year, max_year)
    
    # Process all results
    all_positions = []
    
    # Add results to athlete data and calculate metrics
    for year_str, results in year_results.items():
        if results:
            athlete_data["yearly_results"][year_str] = results
            athlete_data["performance_metrics"]["total_events"] += len(results)
            
            # Process positions for this year
            for result in results:
                position = result.get("position")
                if position and isinstance(position, int):
                    all_positions.append(position)
    
    # Calculate performance metrics
    if all_positions:
        athlete_data["performance_metrics"]["best_position"] = min(all_positions)
        athlete_data["performance_metrics"]["avg_position"] = sum(all_positions) / len(all_positions)
    
    # Add success metric - simple calculation based on positions
    total_points = 0
    for _, results in athlete_data["yearly_results"].items():
        for result in results:
            position = result.get("position")
            if position and isinstance(position, int):
                # Simple scoring system
                if position == 1:
                    points = 25
                elif position == 2:
                    points = 20
                elif position == 3:
                    points = 16
                elif position == 4:
                    points = 13
                elif position == 5:
                    points = 11
                elif position <= 10:
                    points = 11 - (position - 5)
                else:
                    points = 0
                
                total_points += points
                result["points_earned"] = points
    
    athlete_data["performance_metrics"]["total_points"] = total_points
    
    return athlete_data

async def process_athletes_batch_async(athlete_ids_batch, results_data, min_year, max_year, data, 
                                     success_count, error_count, session=None):
    """
    Process a batch of athletes concurrently
    
    Params:
        athlete_ids_batch (list): Batch of athlete IDs to process
        results_data (dict): The loaded results data from results_data.json
        min_year (int): Minimum year to collect results for
        max_year (int): Maximum year to collect results for
        data (dict): Dictionary to store athlete data
        success_count (int): Counter for successful athletes
        error_count (int): Counter for error athletes
        session (aiohttp.ClientSession): Session to reuse for requests
        
    Returns:
        tuple: (success_count, error_count) - Updated counters
    """
    # Create tasks for each athlete in the batch
    tasks = []
    for athlete_id in athlete_ids_batch:
        # Stagger the requests slightly
        await asyncio.sleep(0.05)
        tasks.append(
            process_athlete_async(athlete_id, results_data, min_year, max_year, data, session)
        )
    
    # Wait for all athlete processing in this batch to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results and update counters
    for result in results:
        if isinstance(result, Exception):
            # Handle exception from the task
            print(f"❌ Error in athlete processing: {result}")
            error_count += 1
        elif result is True:
            # Success
            success_count += 1
        else:
            # Error or skipped
            error_count += 1
    
    return success_count, error_count

async def process_athlete_async(athlete_id, results_data, min_year, max_year, data, session=None):
    """
    Process a single athlete asynchronously
    
    Params:
        athlete_id (int): ID of the athlete
        results_data (dict): The loaded results data
        min_year (int): Minimum year to collect results for
        max_year (int): Maximum year to collect results for
        data (dict): Dictionary to store athlete data
        session (aiohttp.ClientSession): Session to reuse for requests
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"Processing athlete {athlete_id}")
    
    try:
        # Get athlete data
        athlete_data = await process_athlete_data_async(athlete_id, results_data, min_year, max_year, session=session)
        
        if athlete_data:
            data["athletes"][str(athlete_id)] = athlete_data
            print(f"✅ Added athlete: {athlete_data.get('name', athlete_id)}")
            return True
        else:
            print(f"⚠️ Skipping athlete {athlete_id}: No data available")
            return False
    except Exception as e:
        print(f"❌ Error processing athlete {athlete_id}: {e}")
        return False

async def collect_athletes_data_async(input_file="results_data.json", output_file="athletes_data.json"):
    """
    Async version to collect data for all athletes found in the results data
    
    Params:
        input_file (str): File with results data
        output_file (str): File to save athlete data to
        
    Returns:
        dict: Collected athlete data
    """
    # Make paths absolute with data directory if needed
    if not os.path.isabs(input_file) and not os.path.dirname(input_file):
        input_file = os.path.join(DATA_DIR, input_file)
    
    if not os.path.isabs(output_file) and not os.path.dirname(output_file):
        output_file = os.path.join(DATA_DIR, output_file)
    
    print(f"\n🏊‍♂️🚴‍♂️🏃‍♂️ ELITE MEN'S ATHLETES COLLECTOR (ASYNC) 🏊‍♂️🚴‍♂️🏃‍♂️")
    print(f"Loading results from {input_file}")
    print(f"Will save athlete data to {output_file}")
    
    # Load results data
    results_data = load_data_from_json(input_file)
    if not results_data:
        print(f"Error: Could not load results data from {input_file}")
        return None
    
    # Debug data structure
    print(f"Results data keys: {list(results_data.keys())}")
    print(f"Events count: {len(results_data.get('events', {}))}")
    
    # Debug: Print structure of a sample event
    events = results_data.get("events", {})
    if events:
        sample_event_id = next(iter(events))
        sample_event = events[sample_event_id]
        print("\n📅 Sample event structure:")
        for key, value in sample_event.items():
            if isinstance(value, dict) or isinstance(value, list):
                print(f"  {key}: {type(value).__name__} with {len(value)} items")
            else:
                print(f"  {key}: {value}")
    
    # Extract all unique athlete IDs from the actual results
    print("\n🔍 Extracting athlete IDs from results data...")
    athlete_ids_from_results = set()
    
    # Check first few results to debug structure
    results = results_data.get("results", [])
    if results:
        for i, result in enumerate(results[:5]):
            print(f"Result {i+1} structure:")
            for key, value in result.items():
                print(f"  {key}: {value} ({type(value).__name__})")
            athlete_id = result.get("athlete_id")
            if athlete_id:
                athlete_ids_from_results.add(athlete_id)
    
    # Add the rest of the athlete IDs
    for result in results[5:]:
        athlete_id = result.get("athlete_id")
        if athlete_id:
            athlete_ids_from_results.add(athlete_id)
    
    # Convert to list and sort
    athlete_ids = sorted(list(athlete_ids_from_results))
    print(f"Found {len(athlete_ids)} unique athlete IDs in the results data")
    
    # Extract year range
    year_range = results_data.get("metadata", {}).get("year_range", "")
    min_year, max_year = 2000, datetime.now().year
    
    # Try to parse year range
    if year_range:
        try:
            years = year_range.split("-")
            min_year = int(years[0])
            max_year = int(years[1])
        except (ValueError, IndexError):
            print(f"Could not parse year range: {year_range}, using defaults")
    
    print(f"\nProcessing {len(athlete_ids)} athletes from {min_year} to {max_year}")
    
    # Initialize data structure
    data = {
        "athletes": {},
        "metadata": {
            "date_collected": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "year_range": f"{min_year}-{max_year}",
            "athlete_count": 0,
            "success_count": 0,
            "error_count": 0
        }
    }
    
    # Record start time
    start_time = time.time()
    
    # Process all athletes in the dataset
    total_athletes = len(athlete_ids)
    processed_count = 0
    success_count = 0
    error_count = 0
    
    print(f"Processing all {total_athletes} athletes in batches of 25")
    
    # Create a connection pool with aiohttp session to reuse
    conn = aiohttp.TCPConnector(limit=10)  # Limit concurrent connections
    async with aiohttp.ClientSession(headers=HEADERS, connector=conn) as session:
        # Process athletes in batches of 25
        batch_size = 75
        for i in range(0, total_athletes, batch_size):
            # Get the next batch of athletes
            batch = athlete_ids[i:i+batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_athletes + batch_size - 1) // batch_size
            
            print(f"\n📊 Processing batch {batch_num}/{total_batches} ({len(batch)} athletes)")
            
            # Process the batch concurrently
            batch_success, batch_error = await process_athletes_batch_async(
                batch, results_data, min_year, max_year, data, success_count, error_count, session
            )
            
            # Update counters
            success_count = batch_success
            error_count = batch_error
            processed_count += len(batch)
            
            # Save progress after each batch
            print(f"💾 Saving intermediate progress after {processed_count} athletes...")
            # Update metadata
            data["metadata"]["athlete_count"] = len(data["athletes"])
            data["metadata"]["success_count"] = success_count
            data["metadata"]["error_count"] = error_count
            data["metadata"]["progress"] = f"{processed_count}/{total_athletes}"
            data["metadata"]["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Save intermediate results
            intermediate_file = f"{output_file}.progress"
            save_data_to_json(data, intermediate_file)
            
            # Add a pause between batches
            if i + batch_size < total_athletes:
                print(f"⏳ Brief pause between batches...")
                await asyncio.sleep(1.0)
    
    # Update metadata
    data["metadata"]["athlete_count"] = len(data["athletes"])
    data["metadata"]["success_count"] = success_count
    data["metadata"]["error_count"] = error_count
    data["metadata"]["completed_athletes"] = processed_count
    data["metadata"]["total_athletes"] = total_athletes
    
    # Calculate processing time
    total_time = time.time() - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    data["metadata"]["processing_time"] = f"{minutes}m {seconds}s"
    
    # Print summary
    print("\n📊 Athletes Collection Summary:")
    print(f"- Years: {min_year}-{max_year}")
    print(f"- Athletes: {data['metadata']['athlete_count']}")
    print(f"- Processing Time: {minutes}m {seconds}s")
    
    # Save to file
    save_data_to_json(data, output_file)
    
    return data

def collect_athletes_data(input_file="results_data.json", output_file="athletes_data.json"):
    """
    Collect data for all athletes found in the results data
    
    Params:
        input_file (str): File with results data
        output_file (str): File to save athlete data to
        
    Returns:
        dict: Collected athlete data
    """
    # Make paths absolute with data directory if needed
    if not os.path.isabs(input_file) and not os.path.dirname(input_file):
        input_file = os.path.join(DATA_DIR, input_file)
    
    if not os.path.isabs(output_file) and not os.path.dirname(output_file):
        output_file = os.path.join(DATA_DIR, output_file)
    
    print(f"\n🏊‍♂️🚴‍♂️🏃‍♂️ ELITE MEN'S ATHLETES COLLECTOR 🏊‍♂️🚴‍♂️🏃‍♂️")
    print(f"Loading results from {input_file}")
    print(f"Will save athlete data to {output_file}")
    
    # Load results data
    results_data = load_data_from_json(input_file)
    if not results_data:
        print(f"Error: Could not load results data from {input_file}")
        return None
    
    # Debug data structure
    print(f"Results data keys: {list(results_data.keys())}")
    print(f"Events count: {len(results_data.get('events', {}))}")
    
    # Debug: Print structure of a sample event
    events = results_data.get("events", {})
    if events:
        sample_event_id = next(iter(events))
        sample_event = events[sample_event_id]
        print("\n📅 Sample event structure:")
        for key, value in sample_event.items():
            if isinstance(value, dict) or isinstance(value, list):
                print(f"  {key}: {type(value).__name__} with {len(value)} items")
            else:
                print(f"  {key}: {value}")
    
    # Extract all unique athlete IDs from the actual results
    print("\n🔍 Extracting athlete IDs from results data...")
    athlete_ids_from_results = set()
    
    # Check first few results to debug structure
    results = results_data.get("results", [])
    if results:
        for i, result in enumerate(results[:5]):
            print(f"Result {i+1} structure:")
            for key, value in result.items():
                print(f"  {key}: {value} ({type(value).__name__})")
            athlete_id = result.get("athlete_id")
            if athlete_id:
                athlete_ids_from_results.add(athlete_id)
    
    # Add the rest of the athlete IDs
    for result in results[5:]:
        athlete_id = result.get("athlete_id")
        if athlete_id:
            athlete_ids_from_results.add(athlete_id)
    
    # Convert to list and sort
    athlete_ids = sorted(list(athlete_ids_from_results))
    print(f"Found {len(athlete_ids)} unique athlete IDs in the results data")
    
    # Extract year range
    year_range = results_data.get("metadata", {}).get("year_range", "")
    min_year, max_year = 2000, datetime.now().year
    
    # Try to parse year range
    if year_range:
        try:
            years = year_range.split("-")
            min_year = int(years[0])
            max_year = int(years[1])
        except (ValueError, IndexError):
            print(f"Could not parse year range: {year_range}, using defaults")
    
    print(f"\nProcessing {len(athlete_ids)} athletes from {min_year} to {max_year}")
    
    # Initialize data structure
    data = {
        "athletes": {},
        "metadata": {
            "date_collected": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "year_range": f"{min_year}-{max_year}",
            "athlete_count": 0,
            "success_count": 0,
            "error_count": 0
        }
    }
    
    # Record start time
    start_time = time.time()
    
    # Process all athletes in the dataset
    total_athletes = len(athlete_ids)
    processed_count = 0
    success_count = 0
    error_count = 0
    
    print(f"Processing all {total_athletes} athletes")
    
    # OPTIMIZATION: For adaptive delay tracking
    last_api_response_time = time.time()
    consecutive_fast_responses = 0
    
    try:
        for i, athlete_id in enumerate(athlete_ids):
            print(f"[{i+1}/{total_athletes}] Processing athlete {athlete_id}")
            
            try:
                # Get athlete data
                athlete_data = process_athlete_data(athlete_id, results_data, min_year, max_year)
                
                if athlete_data:
                    data["athletes"][str(athlete_id)] = athlete_data
                    print(f"✅ Added athlete: {athlete_data.get('name', athlete_id)}")
                    success_count += 1
                else:
                    print(f"⚠️ Skipping athlete {athlete_id}: No data available")
                    error_count += 1
            except Exception as e:
                print(f"❌ Error processing athlete {athlete_id}: {e}")
                error_count += 1
                
            processed_count += 1
            
            # Save progress every 50 athletes in case of interruption
            if processed_count % 50 == 0:
                print(f"💾 Saving intermediate progress after {processed_count} athletes...")
                # Update metadata
                data["metadata"]["athlete_count"] = len(data["athletes"])
                data["metadata"]["success_count"] = success_count
                data["metadata"]["error_count"] = error_count
                data["metadata"]["progress"] = f"{processed_count}/{total_athletes}"
                data["metadata"]["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Save intermediate results
                intermediate_file = f"{output_file}.progress"
                save_data_to_json(data, intermediate_file)
            
            # OPTIMIZATION: Implement dynamic rate limiting approach
            if i < total_athletes - 1:
                # Basic delay - significantly reduced
                delay = 0.1
                
                # Every 20 athletes, add a slightly longer delay
                if (i + 1) % 20 == 0:
                    print(f"📊 Progress: {i+1}/{total_athletes} athletes processed ({((i+1)/total_athletes*100):.1f}%)")
                    time.sleep(0.5)  # Short consolidated break
                
                # Every 100 athletes, do a checkpoint with slightly longer pause
                elif (i + 1) % 100 == 0:
                    elapsed_time = time.time() - start_time
                    minutes = int(elapsed_time // 60)
                    seconds = int(elapsed_time % 60)
                    print(f"🔄 Checkpoint: {i+1}/{total_athletes} athletes processed in {minutes}m {seconds}s")
                    time.sleep(2)  # Reduced from 10s to 2s
                else:
                    # Standard small delay between requests
                    time.sleep(delay)
    
    except KeyboardInterrupt:
        print("\n⚠️ Process interrupted by user. Saving progress...")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}. Saving progress...")
    
    # Update metadata
    data["metadata"]["athlete_count"] = len(data["athletes"])
    data["metadata"]["success_count"] = success_count
    data["metadata"]["error_count"] = error_count
    data["metadata"]["completed_athletes"] = processed_count
    data["metadata"]["total_athletes"] = total_athletes
    
    # Calculate processing time
    total_time = time.time() - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    data["metadata"]["processing_time"] = f"{minutes}m {seconds}s"
    
    # Print summary
    print("\n📊 Athletes Collection Summary:")
    print(f"- Years: {min_year}-{max_year}")
    print(f"- Athletes: {data['metadata']['athlete_count']}")
    print(f"- Processing Time: {minutes}m {seconds}s")
    
    # Save to file
    save_data_to_json(data, output_file)
    
    return data

async def main_async():
    """Async main function for the athletes collector script"""
    # Default values
    input_file = "results_data.json"
    output_file = "athletes_data.json"
    
    # Process command line arguments
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Collect data using async version
    await collect_athletes_data_async(input_file, output_file)

def main():
    """Main function for the athletes collector script"""
    # Default values
    input_file = "results_data.json"
    output_file = "athletes_data.json"
    
    # Process command line arguments
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    
    # Run the async main using asyncio
    asyncio.run(main_async())

if __name__ == "__main__":
    main() 