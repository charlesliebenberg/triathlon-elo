import sys
import time
import os
import asyncio
import aiohttp
from datetime import datetime
import random

# Directory paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")

# Import local modules
try:
    from .utils import (
        BASE_URL, HEADERS, is_elite_men_program,
        save_data_to_json, load_data_from_json, make_api_request
    )
except ImportError:
    # Fallback for direct script execution
    from utils import (
        BASE_URL, HEADERS, is_elite_men_program,
        save_data_to_json, load_data_from_json, make_api_request
    )

# Async version of make_api_request
async def async_make_api_request(url, params=None, session=None, max_retries=3, base_delay=1.0):
    """
    Make an async API request and return the response data
    
    Params:
        url (str): The URL to request
        params (dict): Optional query parameters
        session (aiohttp.ClientSession): Optional session to reuse
        max_retries (int): Maximum number of retry attempts for failed requests
        base_delay (float): Base delay in seconds between retries (will increase exponentially)
        
    Returns:
        dict: Response data from the API or None if error
    """
    if session is None:
        # Create a new session if one wasn't provided
        async with aiohttp.ClientSession(headers=HEADERS) as session:
            return await _perform_async_request(session, url, params, max_retries, base_delay)
    else:
        # Use the provided session
        return await _perform_async_request(session, url, params, max_retries, base_delay)

async def _perform_async_request(session, url, params=None, max_retries=3, base_delay=1.0):
    """
    Helper function to perform the actual async request with retry logic
    
    Params:
        session (aiohttp.ClientSession): Session to use for the request
        url (str): The URL to request
        params (dict): Optional query parameters
        max_retries (int): Maximum number of retry attempts for failed requests 
        base_delay (float): Base delay in seconds between retries (will increase exponentially)
    """
    retry_count = 0
    last_error = None
    last_response_text = None
    
    while retry_count <= max_retries:
        try:
            async with session.get(url, params=params, timeout=30) as response:
                # Try to read the response text for logging purposes
                try:
                    response_text = await response.text()
                    last_response_text = response_text[:300] if response_text else None  # Store first 300 chars
                except Exception as e:
                    response_text = f"[Error reading response: {e}]"
                    last_response_text = response_text
                
                if response.status == 200:
                    try:
                        data = await response.json()
                        # Check if API returned an error status despite HTTP 200
                        if isinstance(data, dict) and data.get("status") == "error":
                            error_msg = f"API returned error: {data.get('message', 'No message')} for URL {url}"
                            print(error_msg)
                            last_error = Exception(error_msg)
                            
                            # Retry on API errors as well
                            retry_delay = base_delay * (2 ** retry_count) + (random.random() * 0.5)
                            print(f"  Retrying on API error in {retry_delay:.2f}s (attempt {retry_count+1}/{max_retries+1})")
                            await asyncio.sleep(retry_delay)
                            retry_count += 1
                            continue
                        return data
                    except Exception as e:
                        error_msg = f"JSON decode error: {e} for URL {url}. Response: {response_text[:100]}..."
                        print(error_msg)
                        last_error = Exception(error_msg)
                        # Retry on JSON decode errors
                        retry_delay = base_delay * (2 ** retry_count) + (random.random() * 0.5)
                        print(f"  Retrying in {retry_delay:.2f}s (attempt {retry_count+1}/{max_retries+1})")
                        await asyncio.sleep(retry_delay)
                        retry_count += 1
                        continue
                elif response.status == 429:  # Too Many Requests
                    retry_delay = base_delay * (2 ** retry_count) + (random.random() * 0.5)
                    print(f"Rate limited (429). Retrying in {retry_delay:.2f}s (attempt {retry_count+1}/{max_retries+1})")
                    print(f"  Response: {response_text[:100]}...")
                    await asyncio.sleep(retry_delay)
                    retry_count += 1
                    continue
                else:
                    error_msg = f"API Error: Status {response.status} for URL {url}"
                    print(error_msg)
                    print(f"  Response: {response_text[:200]}...")
                    last_error = Exception(f"{error_msg} - {response_text[:100]}")
                    
                    # Only retry on 5xx server errors or specific 4xx client errors
                    if response.status >= 500 or response.status in [408, 429]:
                        retry_delay = base_delay * (2 ** retry_count) + (random.random() * 0.5)
                        print(f"  Retrying in {retry_delay:.2f}s (attempt {retry_count+1}/{max_retries+1})")
                        await asyncio.sleep(retry_delay)
                        retry_count += 1
                        continue
                    # For 4xx errors, we'll retry at least once to handle transient issues
                    elif retry_count == 0 and response.status >= 400 and response.status < 500:
                        retry_delay = base_delay * (2 ** retry_count) + (random.random() * 0.5)
                        print(f"  Retrying 4xx error once in {retry_delay:.2f}s (attempt {retry_count+1}/{max_retries+1})")
                        await asyncio.sleep(retry_delay)
                        retry_count += 1
                        continue
                    return None
        except aiohttp.ClientError as e:
            error_msg = f"Request Error: {e} for URL {url}"
            print(error_msg)
            last_error = e
            
            # Add exponential backoff with jitter for retries
            retry_delay = base_delay * (2 ** retry_count) + (random.random() * 0.5)
            print(f"  Retrying in {retry_delay:.2f}s (attempt {retry_count+1}/{max_retries+1})")
            await asyncio.sleep(retry_delay)
            retry_count += 1
        except Exception as e:
            error_msg = f"Unexpected Error: {e} for URL {url}"
            print(error_msg)
            last_error = e
            
            # Add exponential backoff with jitter for retries
            retry_delay = base_delay * (2 ** retry_count) + (random.random() * 0.5)
            print(f"  Retrying in {retry_delay:.2f}s (attempt {retry_count+1}/{max_retries+1})")
            await asyncio.sleep(retry_delay)
            retry_count += 1
    
    # If we've exhausted all retries, log the final error and return None
    if last_error:
        error_detail = f" Response: {last_response_text}" if last_response_text else ""
        print(f"  All {max_retries+1} attempts failed for URL {url}. Last error: {last_error}.{error_detail}")
    return None

async def get_events_async(start_date=None, end_date=None, page=1, per_page=10, session=None, max_retries=3, base_delay=1.0):
    """
    Async version to get events with pagination
    
    Params:
        start_date (str): Start date in format YYYY-MM-DD
        end_date (str): End date in format YYYY-MM-DD
        page (int): Page number
        per_page (int): Number of results per page
        session (aiohttp.ClientSession): Session to reuse for requests
        max_retries (int): Maximum number of retry attempts for failed requests
        base_delay (float): Base delay in seconds between retries (will increase exponentially)
    
    Returns:
        list: List of event dictionaries
    """
    url = f"{BASE_URL}/events"
    params = {"page": page, "per_page": per_page}
    
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date
    
    print(f"🏁 Fetching events (page {page}) with params: {params}")
    
    data = await async_make_api_request(url, params, session, max_retries, base_delay)
    if not data:
        return []
    
    # Extract just the events data
    events = data.get("data", [])
    
    # Print pagination info
    total = data.get("total", 0)
    last_page = data.get("last_page", 0)
    if page == 1:
        print(f"  Found {total} total events across {last_page} pages")
    
    return events

async def get_all_events_async(start_date=None, end_date=None, max_pages=30, session=None, max_retries=3, base_delay=1.0):
    """
    Async version to get all events in a date range. Handles pagination automatically.
    
    Params:
        start_date (str): Start date in format YYYY-MM-DD
        end_date (str): End date in format YYYY-MM-DD
        max_pages (int): Maximum number of pages to fetch to avoid excessive API calls
        session (aiohttp.ClientSession): Session to reuse for requests
        max_retries (int): Maximum number of retry attempts for failed requests
        base_delay (float): Base delay in seconds between retries (will increase exponentially)
    
    Returns:
        list: List of event dictionaries
    """
    all_events = []
    page = 1
    per_page = 10  # Starting with 10 per page
    
    while True:
        # Check if we've reached the maximum page limit
        if page > max_pages:
            print(f"Reached maximum page limit of {max_pages}. Stopping event collection.")
            break
            
        events_page = await get_events_async(
            start_date=start_date, 
            end_date=end_date, 
            page=page, 
            per_page=per_page,
            session=session,
            max_retries=max_retries,
            base_delay=base_delay
        )
        
        if not events_page:
            # No more events or API error
            break
            
        all_events.extend(events_page)
        
        # Either go to next page or exit if no more pages
        if len(events_page) < per_page:
            # We got fewer results than requested, so this is the last page
            break
            
        # Go to next page
        page += 1
        
        # After first page, increase per_page for efficiency if we have many pages
        if page == 2:
            per_page = 25  # Get more results per page to reduce number of API calls
        
        # Small delay between page fetches
        await asyncio.sleep(0.2)
    
    return all_events

async def get_event_details_async(event_id, session=None, max_retries=3, base_delay=1.0):
    """
    Async version to get details about a specific event
    
    Args:
        event_id: The ID of the event to get details for
        session (aiohttp.ClientSession): Session to reuse for requests
        max_retries (int): Maximum number of retry attempts for failed requests
        base_delay (float): Base delay in seconds between retries (will increase exponentially)
        
    Returns:
        dict: Event details and programs, formatted for database
    """
    url = f"{BASE_URL}/events/{event_id}"
    data = await async_make_api_request(url, session=session, max_retries=max_retries, base_delay=base_delay)
    
    if not data:
        return None
        
    event_data = data["data"]
    
    # Extract relevant event details
    event_details = {
        "id": event_id,
        "title": event_data.get("event_title"),
        "date": event_data.get("event_date"),
        "location": {
            "country": event_data.get("event_country_name"),
            "venue": event_data.get("event_venue_name")
        },
        "programs": []
    }
    
    # Extract programs
    if "programs" in event_data:
        for program in event_data["programs"]:
            event_details["programs"].append({
                "prog_id": program.get("prog_id"),
                "prog_name": program.get("prog_name"),
                "prog_distance": program.get("prog_distance")
            })
    
    return event_details

async def get_event_results_async(event_id, prog_id, session=None, max_retries=3, base_delay=1.0):
    """
    Async version to get full results for a specific event program
    
    Params:
        event_id (int): ID of the event
        prog_id (int): ID of the program within the event
        session (aiohttp.ClientSession): Session to reuse for requests
        max_retries (int): Maximum number of retry attempts for failed requests
        base_delay (float): Base delay in seconds between retries (will increase exponentially)
    
    Returns:
        list: List of results for the event
    """
    url = f"{BASE_URL}/events/{event_id}/programs/{prog_id}/results"
    
    print(f"🏁 Fetching results for event ID: {event_id}, program ID: {prog_id}")
    data = await async_make_api_request(url, session=session, max_retries=max_retries, base_delay=base_delay)
    
    if not data:
        return []
    
    result_data = data.get("data", {})
    
    # Extract results from the dictionary response
    if isinstance(result_data, dict) and 'results' in result_data:
        results = result_data.get('results', [])
        return results
    
    # Handle list of strings (field names only)
    if isinstance(result_data, list) and len(result_data) > 0 and all(isinstance(item, str) for item in result_data):
        print(f"Warning: Event results returned field names only: {result_data[:5]}")
        return []
    
    return result_data if isinstance(result_data, list) else []

def process_event_results(event_id, prog_name, prog_id, results_data, processed_results):
    """
    Process results from an event and add to the processed results
    This function doesn't need to be async as it's just processing local data
    
    Params:
        event_id (int): The event ID
        prog_name (str): The program name
        prog_id (int): The program ID
        results_data (list): Raw results data
        processed_results (list): List to add processed results to
        
    Returns:
        set: Set of athlete IDs found in the results
    """
    athlete_ids = set()
    
    # Filter out non-numeric positions or conversion errors
    valid_results = []
    for result in results_data:
        try:
            position = int(result.get("position", "DNF"))
            result["position"] = position
            valid_results.append(result)
        except (ValueError, TypeError):
            # Skip results with non-numeric positions
            pass
    
    if not valid_results:
        print(f"  No valid results found for program {prog_id}")
        return athlete_ids
    
    # Process each result
    for result in valid_results:
        athlete_id = result.get("athlete_id")
        if not athlete_id:
            continue
            
        # Convert athlete ID to integer if it's a string
        if isinstance(athlete_id, str):
            try:
                athlete_id = int(athlete_id)
            except ValueError:
                continue
        
        # Basic processed result structure
        processed_result = {
            "athlete_id": athlete_id,
            "event_id": event_id,
            "prog_id": prog_id,
            "prog_name": prog_name,
            "position": result.get("position"),
            "total_time": result.get("total_time"),
            "points": result.get("points", 0),
            "athlete_name": result.get("athlete_name"),
            "country_code": result.get("country_code"),
            "event_date": result.get("event_date")
        }
        
        # Add to processed results
        processed_results.append(processed_result)
        
        # Add athlete ID to set
        athlete_ids.add(athlete_id)
    
    return athlete_ids

async def process_event_async(i, total_events, event, events_data, results_data, all_athlete_ids, session, max_retries=3, base_delay=1.0):
    """
    Process a single event asynchronously
    
    Params:
        i (int): Event index for logging
        total_events (int): Total number of events for logging
        event (dict): Event data
        events_data (dict): Dictionary to store event details
        results_data (list): List to store processed results
        all_athlete_ids (set): Set to store all athlete IDs
        session (aiohttp.ClientSession): Session to reuse for requests
        max_retries (int): Maximum number of retry attempts for failed requests
        base_delay (float): Base delay in seconds between retries
    """
    event_id = event.get("event_id")
    event_title = event.get("event_title")
    event_date = event.get("event_date", "Unknown date")
    
    print(f"[{i+1}/{total_events}] Processing event: {event_title} (ID: {event_id}, Date: {event_date})")
    
    # Get detailed event information
    event_details = await get_event_details_async(event_id, session, max_retries, base_delay)
    if not event_details:
        print(f"  Skipping event {event_id}: Could not get details")
        # Still add minimal event info to events_data to track failed events
        events_data[event_id] = {
            "id": event_id,
            "title": event_title,
            "date": event_date,
            "status": "failed_to_fetch",
            "programs": []
        }
        return
        
    # Check for elite men programs
    elite_men_programs = []
    for program in event_details.get("programs", []):
        prog_name = program.get("prog_name", "")
        prog_id = program.get("prog_id")
        if is_elite_men_program(prog_name) and prog_id:
            elite_men_programs.append((prog_name, prog_id))
    
    if not elite_men_programs:
        print(f"  No Elite Men programs found in event {event_id}")
        # Add to events_data with status
        events_data[event_id] = event_details
        events_data[event_id]["status"] = "no_elite_men_programs"
        return
    
    print(f"  Found {len(elite_men_programs)} Elite Men programs")
    events_data[event_id] = event_details
    events_data[event_id]["status"] = "processed"
    
    # For each Elite Men program, get results (sequentially for each program within the event)
    # This avoids overwhelming the API with too many concurrent requests for the same event
    for prog_name, prog_id in elite_men_programs:
        print(f"  Getting results for program: {prog_name}")
        
        results = await get_event_results_async(event_id, prog_id, session, max_retries, base_delay)
        if not results:
            print(f"  No results found for program {prog_id}")
            continue
            
        print(f"  Found {len(results)} raw results")
        
        # Process results and add to data containers
        # This is synchronous as it's just processing local data
        athlete_ids = process_event_results(
            event_id, prog_name, prog_id, results, results_data
        )
        
        all_athlete_ids.update(athlete_ids)
        
        print(f"  Processed {len(athlete_ids)} athletes from program {prog_id}")
        
        # Add a small delay between program result fetches
        await asyncio.sleep(0.2)

# Add a new function to handle specific date ranges for recent or problem years
async def collect_results_for_date_range(start_date, end_date, description="Custom period", max_events=None, session=None, max_retries=3, base_delay=1.0):
    """
    Collect results for a specific date range - useful for filling gaps or re-fetching problem periods
    
    Params:
        start_date (str): Start date in format YYYY-MM-DD
        end_date (str): End date in format YYYY-MM-DD
        description (str): Description of this date range for logging
        max_events (int): Optional maximum number of events to process
        session (aiohttp.ClientSession): Session to reuse for requests
        max_retries (int): Maximum number of retry attempts
        base_delay (float): Base delay for retries
        
    Returns:
        tuple: (events_data, results_data, athlete_ids)
    """
    print(f"\n📅 Collecting Elite Men's results for {description}: {start_date} to {end_date}")
    
    # Get events for this date range
    events = await get_all_events_async(
        start_date=start_date, 
        end_date=end_date, 
        session=session, 
        max_retries=max_retries, 
        base_delay=base_delay
    )
    
    if not events:
        print(f"No events found for {start_date} to {end_date}")
        return {}, [], set()
    
    print(f"Found {len(events)} events for {start_date} to {end_date}")
    
    # Limit events if max_events is specified
    if max_events and len(events) > max_events:
        print(f"Limiting to {max_events} events")
        events = events[:max_events]
    
    # Initialize data containers
    events_data = {}  # event_id -> event_details
    results_data = []  # list of processed results
    all_athlete_ids = set()  # set of all athlete IDs found
    
    # Process events concurrently for this date range
    event_tasks = []
    for i, event in enumerate(events):
        event_id = event.get("event_id")
        # Add a small delay to stagger the initial requests
        await asyncio.sleep(0.1 * (i % 3))  # Stagger by 0, 0.1, or 0.2 seconds
        event_tasks.append(process_event_async(i, len(events), event, events_data, results_data, all_athlete_ids, session, max_retries, base_delay))
    
    # Wait for all event processing to complete
    await asyncio.gather(*event_tasks)
    
    return events_data, results_data, all_athlete_ids

async def collect_results_for_year_async(year, max_events=None, session=None, max_retries=3, base_delay=1.0):
    """
    Async version to collect all Elite Men's results for a specific year,
    breaking the year into quarters to reduce API load and avoid rate limiting
    
    Params:
        year (int): The year to collect results for
        max_events (int): Optional maximum number of events to process
        session (aiohttp.ClientSession): Session to reuse for requests
        max_retries (int): Maximum number of retry attempts for failed requests
        base_delay (float): Base delay in seconds between retries
        
    Returns:
        tuple: (events_data, results_data, athlete_ids)
    """
    print(f"\n📅 Collecting Elite Men's results for year {year}")
    
    # Set up quarters for the year to process in smaller chunks
    quarters = [
        (f"{year}-01-01", f"{year}-03-31"),  # Q1
        (f"{year}-04-01", f"{year}-06-30"),  # Q2
        (f"{year}-07-01", f"{year}-09-30"),  # Q3
        (f"{year}-10-01", f"{year}-12-31")   # Q4
    ]
    
    # For years in the future, only include quarters up to current date
    current_date = datetime.now()
    if year == current_date.year:
        current_quarter = (current_date.month - 1) // 3
        quarters = quarters[:current_quarter + 1]  # Only include up to current quarter
        # Update end date of current quarter to today
        if quarters:
            quarters[-1] = (quarters[-1][0], current_date.strftime("%Y-%m-%d"))
    
    # Initialize data containers
    events_data = {}  # event_id -> event_details
    results_data = []  # list of processed results
    all_athlete_ids = set()  # set of all athlete IDs found
    all_events = []
    
    # Process each quarter
    for i, (start_date, end_date) in enumerate(quarters):
        print(f"\n  Processing Q{i+1}: {start_date} to {end_date}")
        
        # Get events for this quarter
        quarter_events = await get_all_events_async(
            start_date=start_date, 
            end_date=end_date, 
            session=session, 
            max_retries=max_retries, 
            base_delay=base_delay
        )
        
        if not quarter_events:
            print(f"  No events found for {start_date} to {end_date}")
            continue
        
        print(f"  Found {len(quarter_events)} events in Q{i+1}")
        all_events.extend(quarter_events)
        
        # Small delay between quarters to avoid overwhelming the API
        if i < len(quarters) - 1:
            await asyncio.sleep(1.0)
    
    if not all_events:
        print(f"No events found for {year}")
        return {}, [], set()
    
    print(f"Found {len(all_events)} total events for {year}")
    
    # Limit events if max_events is specified
    if max_events and len(all_events) > max_events:
        print(f"Limiting to {max_events} events")
        all_events = all_events[:max_events]
    
    # Process events concurrently for this year using tasks
    event_tasks = []
    for i, event in enumerate(all_events):
        event_id = event.get("event_id")
        # Add a small delay to stagger the initial requests
        await asyncio.sleep(0.1 * (i % 3))  # Stagger by 0, 0.1, or 0.2 seconds
        event_tasks.append(process_event_async(i, len(all_events), event, events_data, results_data, all_athlete_ids, session, max_retries, base_delay))
    
    # Wait for all event processing to complete
    await asyncio.gather(*event_tasks)
    
    return events_data, results_data, all_athlete_ids

async def collect_results_data_async(start_year=2000, end_year=None, output_file="results_data.json", max_retries=3, base_delay=1.0):
    """
    Async version to collect Elite Men's results from a range of years
    
    Params:
        start_year (int): First year to collect data from
        end_year (int): Last year to collect data from (defaults to current year)
        output_file (str): File to save data to
        max_retries (int): Maximum number of retry attempts for failed requests
        base_delay (float): Base delay in seconds between retries
        
    Returns:
        dict: Collected data
    """
    # Make path absolute with data directory if needed
    if not os.path.isabs(output_file) and not os.path.dirname(output_file):
        output_file = os.path.join(DATA_DIR, output_file)
    
    print(f"\n🏊‍♂️🚴‍♂️🏃‍♂️ ELITE MEN'S RESULTS COLLECTOR (ASYNC) 🏊‍♂️🚴‍♂️🏃‍♂️")
    print(f"Will save results to {output_file}")
    
    # Set default end_year to current year if not specified
    if end_year is None:
        end_year = datetime.now().year
    
    print(f"Collecting data from {start_year} to {end_year}")
    print(f"Using max_retries={max_retries}, base_delay={base_delay}s for API requests")
    
    # Initialize data structure
    data = {
        "events": {},
        "results": [],
        "athlete_ids": set(),
        "metadata": {
            "date_collected": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "year_range": f"{start_year}-{end_year}",
            "event_count": 0,
            "result_count": 0,
            "athlete_count": 0,
            "failed_events": 0,
            "retried_years": []
        }
    }
    
    # Record start time
    start_time = time.time()
    
    # Create a connection pool with aiohttp session to reuse
    conn = aiohttp.TCPConnector(limit=10)  # Limit concurrent connections
    timeout = aiohttp.ClientTimeout(total=60)  # Increase total timeout
    async with aiohttp.ClientSession(headers=HEADERS, connector=conn, timeout=timeout) as session:
        # Process all years without separating recent from older years
        years = list(range(start_year, end_year + 1))
        
        # Track years with significant failures
        years_with_failures = []
        failure_threshold = 5  # Consider a year as failing if it has more than this many failed events
        
        print(f"\n⏳ PHASE 1: Initial processing of years {years[0]}-{years[-1]}")
        # Process years in parallel
        year_tasks = []
        for year in years:
            # Stagger the start times slightly to avoid all requests hitting at once
            await asyncio.sleep(0.5)
            year_tasks.append(collect_results_for_year_async(
                year, 
                session=session, 
                max_retries=max_retries, 
                base_delay=base_delay
            ))
        
        # Wait for all year tasks to complete
        year_results = await asyncio.gather(*year_tasks)
        
        # Process results
        for i, (year_events, year_results_data, year_athlete_ids) in enumerate(year_results):
            year = years[i]
            # Add to data structure
            data["events"].update(year_events)
            data["results"].extend(year_results_data)
            data["athlete_ids"].update(year_athlete_ids)
            
            # Count failed events
            failed_events = sum(1 for event in year_events.values() if event.get("status") == "failed_to_fetch")
            
            print(f"Year {year}: {len(year_events)} events ({failed_events} failed), {len(year_results_data)} results, {len(year_athlete_ids)} athletes")
            data["metadata"]["failed_events"] += failed_events
            
            # Check if this year had significant failures
            if failed_events > failure_threshold:
                years_with_failures.append(year)
                print(f"  ⚠️ Year {year} had {failed_events} failed events - will retry with higher retry values")
            
            # Save intermediate results after each year
            # Convert athlete_ids to list before saving
            data_to_save = data.copy()
            data_to_save["athlete_ids"] = list(data["athlete_ids"])
            
            intermediate_file = f"interim_{year}_{os.path.basename(output_file)}"
            # Make sure the intermediate file is saved in the data directory
            if not os.path.isabs(intermediate_file) and not os.path.dirname(intermediate_file):
                intermediate_file = os.path.join(DATA_DIR, intermediate_file)
                
            save_data_to_json(data_to_save, intermediate_file)
            print(f"Saved progress for {year} to {intermediate_file}")
        
        # PHASE 2: Retry years with significant failures
        if years_with_failures:
            print(f"\n🔄 PHASE 2: Retrying {len(years_with_failures)} years with higher retry values")
            print(f"Years to retry: {years_with_failures}")
            
            # Increased retry parameters for problem years
            retry_max_retries = max(max_retries * 2, 8)  # Double the retries with a minimum of 8
            retry_base_delay = base_delay * 1.5  # Increase base delay by 50%
            
            print(f"Using retry_max_retries={retry_max_retries}, retry_base_delay={retry_base_delay}s")
            
            # Track results from retries
            retry_events_data = {}
            retry_results_data = []
            retry_athlete_ids = set()
            
            # Process retry years sequentially to maximize success
            for year in years_with_failures:
                print(f"\n🔍 Retrying year {year} with {retry_max_retries} retries")
                # Small delay before starting retry
                await asyncio.sleep(2.0)
                
                # Collect results for this year with higher retry values
                year_events, year_results_data, year_athlete_ids = await collect_results_for_year_async(
                    year,
                    session=session,
                    max_retries=retry_max_retries,
                    base_delay=retry_base_delay
                )
                
                # Count failed events after retry
                failed_events_after_retry = sum(1 for event in year_events.values() if event.get("status") == "failed_to_fetch")
                
                print(f"Retry of year {year}: {len(year_events)} events ({failed_events_after_retry} failed), {len(year_results_data)} results, {len(year_athlete_ids)} athletes")
                
                # Add to retry data containers
                retry_events_data.update(year_events)
                retry_results_data.extend(year_results_data)
                retry_athlete_ids.update(year_athlete_ids)
                
                # Update the original data to replace the year's data with retry data
                # First, remove original events for this year from data["events"]
                original_event_ids = [event_id for event_id, event in data["events"].items() 
                                     if event.get("date", "").startswith(str(year))]
                
                for event_id in original_event_ids:
                    if event_id in data["events"]:
                        del data["events"][event_id]
                
                # Remove original results for this year from data["results"]
                data["results"] = [result for result in data["results"] 
                                  if not (result.get("event_date", "").startswith(str(year)))]
                
                # Add the retry data for this year
                data["events"].update(year_events)
                data["results"].extend(year_results_data)
                data["athlete_ids"].update(year_athlete_ids)
                
                # Update metadata failed_events count
                data["metadata"]["failed_events"] = data["metadata"]["failed_events"] - failed_events + failed_events_after_retry
                
                # Add to retried years list in metadata
                if year not in data["metadata"]["retried_years"]:
                    data["metadata"]["retried_years"].append(year)
                
                # Save interim retry results
                data_to_save = data.copy()
                data_to_save["athlete_ids"] = list(data["athlete_ids"])
                retry_file = f"retry_{year}_{os.path.basename(output_file)}"
                # Make sure the retry file is saved in the data directory
                if not os.path.isabs(retry_file) and not os.path.dirname(retry_file):
                    retry_file = os.path.join(DATA_DIR, retry_file)
                save_data_to_json(data_to_save, retry_file)
                print(f"Saved retry results for {year} to {retry_file}")
                
                # Small delay between retried years
                await asyncio.sleep(1.0)
            
            # Print retry summary
            print("\n📊 Retry Summary:")
            print(f"- Years retried: {data['metadata']['retried_years']}")
            print(f"- Events: {len(retry_events_data)}")
            print(f"- Results: {len(retry_results_data)}")
            print(f"- Athletes: {len(retry_athlete_ids)}")
    
    # Convert set to list for JSON serialization
    data["athlete_ids"] = list(data["athlete_ids"])
    
    # Update metadata
    data["metadata"]["event_count"] = len(data["events"])
    data["metadata"]["result_count"] = len(data["results"])
    data["metadata"]["athlete_count"] = len(data["athlete_ids"])
    
    # Calculate processing time
    total_time = time.time() - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    data["metadata"]["processing_time"] = f"{minutes}m {seconds}s"
    
    # Print summary
    print("\n📊 Final Results Collection Summary:")
    print(f"- Years: {start_year}-{end_year}")
    print(f"- Years retried: {data['metadata']['retried_years']}")
    print(f"- Events: {data['metadata']['event_count']} (Failed: {data['metadata']['failed_events']})")
    print(f"- Results: {data['metadata']['result_count']}")
    print(f"- Athletes: {data['metadata']['athlete_count']}")
    print(f"- Processing Time: {minutes}m {seconds}s")
    
    # Save to file
    save_data_to_json(data, output_file)
    print(f"\n✅ Final combined data saved to {output_file}")
    
    return data

async def main_async():
    """
    Main function for asynchronous execution
    """
    # Create the data directory if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)

    # Default values
    start_year = 2000
    end_year = datetime.now().year
    output_file = "results_data.json"
    max_retries = 4
    base_delay = 1.0
    custom_mode = False
    custom_period = None
    
    print("\n🔧 ELITE MEN'S RESULTS COLLECTOR 🔧")
    print("Usage: python results_collector.py [start_year] [end_year] [output_file] [max_retries] [base_delay]")
    print("       python results_collector.py --custom YYYY-MM-DD YYYY-MM-DD [description] [output_file] [max_retries]")
    
    # Process command line arguments
    if len(sys.argv) > 1:
        # Check for custom mode
        if sys.argv[1].lower() == '--custom':
            custom_mode = True
            if len(sys.argv) > 3:
                try:
                    # Parse start and end dates
                    start_date = sys.argv[2]
                    end_date = sys.argv[3]
                    # Validate dates
                    datetime.strptime(start_date, "%Y-%m-%d")
                    datetime.strptime(end_date, "%Y-%m-%d")
                    custom_period = (start_date, end_date)
                    
                    # Optional description
                    description = "Custom period" if len(sys.argv) <= 4 else sys.argv[4]
                    
                    # Optional output file
                    if len(sys.argv) > 5:
                        output_file = sys.argv[5]
                        # Make sure the output file is saved in the data directory
                        if not os.path.isabs(output_file) and not os.path.dirname(output_file):
                            output_file = os.path.join(DATA_DIR, output_file)
                    else:
                        output_file = os.path.join(DATA_DIR, f"results_{start_date}_to_{end_date}.json")
                    
                    # Optional max retries
                    if len(sys.argv) > 6:
                        try:
                            max_retries = int(sys.argv[6])
                        except ValueError:
                            print(f"Invalid max_retries: {sys.argv[6]}. Using default: {max_retries}")
                except Exception as e:
                    print(f"Error parsing custom dates: {e}")
                    print("Format should be: python results_collector.py --custom YYYY-MM-DD YYYY-MM-DD [description] [output_file] [max_retries]")
                    sys.exit(1)
            else:
                print("Custom mode requires start and end dates in YYYY-MM-DD format")
                sys.exit(1)
        else:
            # Regular year-based mode
            try:
                start_year = int(sys.argv[1])
            except ValueError:
                print(f"Invalid start year: {sys.argv[1]}. Using default: {start_year}")
    
    if not custom_mode and len(sys.argv) > 2:
        try:
            end_year = int(sys.argv[2])
        except ValueError:
            print(f"Invalid end year: {sys.argv[2]}. Using default: {end_year}")
    
    if not custom_mode and len(sys.argv) > 3:
        output_file = sys.argv[3]
        
    if not custom_mode and len(sys.argv) > 4:
        try:
            max_retries = int(sys.argv[4])
        except ValueError:
            print(f"Invalid max_retries: {sys.argv[4]}. Using default: {max_retries}")
            
    if not custom_mode and len(sys.argv) > 5:
        try:
            base_delay = float(sys.argv[5])
        except ValueError:
            print(f"Invalid base_delay: {sys.argv[5]}. Using default: {base_delay}")
    
    # Collect data
    if custom_mode and custom_period:
        start_date, end_date = custom_period
        print(f"\n⚡ Running in CUSTOM MODE for period: {start_date} to {end_date}")
        
        # Create a connection pool with aiohttp session to reuse
        conn = aiohttp.TCPConnector(limit=10)  # Limit concurrent connections
        timeout = aiohttp.ClientTimeout(total=60)  # Increase total timeout
        async with aiohttp.ClientSession(headers=HEADERS, connector=conn, timeout=timeout) as session:
            events_data, results_data, athlete_ids = await collect_results_for_date_range(
                start_date, 
                end_date, 
                description, 
                session=session,
                max_retries=max_retries,
                base_delay=base_delay
            )
            
            # Create data structure
            data = {
                "events": events_data,
                "results": results_data,
                "athlete_ids": list(athlete_ids),  # Convert set to list for JSON serialization
                "metadata": {
                    "date_collected": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "date_range": f"{start_date} to {end_date}",
                    "description": description,
                    "event_count": len(events_data),
                    "result_count": len(results_data),
                    "athlete_count": len(athlete_ids),
                    "failed_events": sum(1 for event in events_data.values() if event.get("status") == "failed_to_fetch")
                }
            }
            
            # Save to file
            save_data_to_json(data, output_file)
            
            # Print summary
            print("\n📊 Results Collection Summary:")
            print(f"- Period: {start_date} to {end_date}")
            print(f"- Events: {data['metadata']['event_count']} (Failed: {data['metadata']['failed_events']})")
            print(f"- Results: {data['metadata']['result_count']}")
            print(f"- Athletes: {data['metadata']['athlete_count']}")
    else:
        # Run standard year-based collection
        print(f"\n⚡ Running in STANDARD MODE for years: {start_year} to {end_year}")
        await collect_results_data_async(start_year, end_year, output_file, max_retries, base_delay)

def main():
    """Main function for the results collector script"""
    # Simply run the async main function
    asyncio.run(main_async())

if __name__ == "__main__":
    main() 