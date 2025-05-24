import os
import json
import sys
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

# Directory paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")

def load_env_variables():
    """Load environment variables from .env file"""
    load_dotenv()
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("Error: SUPABASE_URL or SUPABASE_KEY not found in .env file")
        return None, None
    
    return supabase_url, supabase_key

def get_supabase_client():
    """Create a Supabase client"""
    supabase_url, supabase_key = load_env_variables()
    if not supabase_url or not supabase_key:
        return None
    
    try:
        print(f"Connecting to Supabase at {supabase_url}")
        # Create client without any additional parameters that could cause issues
        supabase = create_client(supabase_url, supabase_key)
        return supabase
    except Exception as e:
        print(f"Error connecting to Supabase: {e}")
        return None

def create_tables_if_needed(supabase):
    """
    Check if tables exist and create them if needed
    Note: This requires that you create the SQL functions in Supabase dashboard first
    """
    print("Note: Tables need to be created via Supabase dashboard or SQL editor.")
    print("Please make sure these tables exist: athletes, events, results, athlete_ratings, elo_timeline, head_to_head, head_to_head_meetings, metadata")
    
    return True  # Assume tables exist or will be created manually

def clear_tables(supabase, clear_existing=True):
    """Clear all data from tables if requested"""
    if not clear_existing:
        print("Skipping data clearing - incremental update mode")
        return
    
    try:
        print("Clearing existing data from tables...")
        
        # Define table deletion order to handle foreign key constraints
        # Delete from child tables first, then parent tables
        tables_in_order = [
            "head_to_head_meetings",  # References head_to_head, athletes, events
            "results",                # References athletes, events
            "athlete_ratings",        # References athletes, events
            "elo_timeline",           # References athletes
            "head_to_head",           # References athletes
            "events",                 # Referenced by other tables
            "athletes",               # Referenced by other tables
            "metadata"                # No constraints
        ]
        
        # Define primary key columns for each table
        table_columns = {
            "athletes": "athlete_id",
            "events": "event_id",
            "results": "result_id",
            "athlete_ratings": "rating_id",
            "elo_timeline": "timeline_id",
            "head_to_head_meetings": "meeting_id",
            "head_to_head": "pair_id",
            "metadata": "key"
        }
        
        # Clear data using DELETE operation with proper WHERE clauses
        for table in tables_in_order:
            try:
                column = table_columns[table]
                print(f"  - Clearing table: {table}")
                
                # For text columns (like pair_id, key), use IS NOT NULL
                if column in ["pair_id", "key"]:
                    supabase.table(table).delete().not_.is_(column, "null").execute()
                # For numeric ID columns, use >= 0
                else:
                    supabase.table(table).delete().gte(column, 0).execute()
                    
            except Exception as e:
                print(f"  - Error clearing table {table}: {e}")
    except Exception as e:
        print(f"Error clearing tables: {e}")

def insert_athletes_data(supabase, athletes_data):
    """Insert athletes data into Supabase"""
    try:
        added = 0
        updated = 0
        
        # Process in batches to avoid issues with large datasets
        batch_size = 100
        athlete_batch = []
        
        for athlete_id, athlete_info in athletes_data.items():
            details = athlete_info.get("details", {})
            athlete_id = int(athlete_id)
            
            athlete_data = {
                "athlete_id": athlete_id,
                "first_name": details.get("first_name"),
                "last_name": details.get("last_name"),
                "full_name": details.get("full_name"),
                "nationality": details.get("country"),
                "gender": details.get("gender"),
                "birth_year": details.get("year_of_birth")
            }
            
            athlete_batch.append(athlete_data)
            
            if len(athlete_batch) >= batch_size:
                # Upsert batch (insert or update if exists)
                supabase.table("athletes").upsert(athlete_batch).execute()
                added += len(athlete_batch)
                athlete_batch = []
        
        # Insert any remaining athletes
        if athlete_batch:
            supabase.table("athletes").upsert(athlete_batch).execute()
            added += len(athlete_batch)
        
        print(f"  - {added} athletes processed")
        return added, 0
    except Exception as e:
        print(f"Error inserting athletes data: {e}")
        return 0, 0

def insert_events_data(supabase, events_data):
    """Insert events data into Supabase"""
    try:
        added = 0
        
        # Process in batches to avoid issues with large datasets
        batch_size = 100
        event_batch = []
        
        for event_id, event_info in events_data.items():
            event_id = int(event_id)
            
            # Determine event importance
            event_title = event_info.get("title", "").lower()
            importance = 1  # Default to local
            
            if "olympic" in event_title or "olympics" in event_title or "world championship" in event_title:
                importance = 5  # Olympic/World Championship
            elif any(term in event_title for term in ["world cup", "world series", "wtcs", "wts", "grand final"]):
                importance = 4  # World level
            elif any(term in event_title for term in ["continental", "european championship", "ironman", "70.3"]):
                importance = 3  # Major
            elif any(term in event_title for term in ["national", "cup", "series"]):
                importance = 2  # Regional
            
            event_data = {
                "event_id": event_id,
                "title": event_info.get("title"),
                "date": event_info.get("date"),
                "importance": importance
            }
            
            event_batch.append(event_data)
            
            if len(event_batch) >= batch_size:
                # Upsert batch (insert or update if exists)
                supabase.table("events").upsert(event_batch).execute()
                added += len(event_batch)
                event_batch = []
        
        # Insert any remaining events
        if event_batch:
            supabase.table("events").upsert(event_batch).execute()
            added += len(event_batch)
        
        print(f"  - {added} events processed")
        return added, 0
    except Exception as e:
        print(f"Error inserting events data: {e}")
        return 0, 0

def insert_results_data(supabase, results_data):
    """Insert results data into Supabase"""
    try:
        added = 0
        skipped = 0
        
        # Process in batches to avoid issues with large datasets
        batch_size = 100
        result_batch = []
        seen_keys = set()  # Track unique (athlete_id, event_id) combinations
        
        for result in results_data:
            athlete_id = result.get("athlete_id")
            event_id = result.get("event_id")
            
            # Validate required fields
            if athlete_id is not None and event_id is not None:
                # Skip if already in current batch
                batch_key = (athlete_id, event_id)
                if batch_key in seen_keys:
                    skipped += 1
                    continue
                
                seen_keys.add(batch_key)
                
                result_data = {
                    "athlete_id": athlete_id,
                    "event_id": event_id,
                    "position": result.get("position"),
                    "total_time": result.get("total_time"),
                    "points": result.get("points", 0)
                }
                
                result_batch.append(result_data)
                
                if len(result_batch) >= batch_size:
                    # Upsert batch (insert or update if exists)
                    try:
                        supabase.table("results").upsert(
                            result_batch, 
                            on_conflict="athlete_id,event_id"
                        ).execute()
                        added += len(result_batch)
                        result_batch = []
                        seen_keys.clear()  # Reset for next batch
                    except Exception as batch_error:
                        print(f"Error inserting results batch: {batch_error}")
                        skipped += len(result_batch)
                        result_batch = []
                        seen_keys.clear()  # Reset for next batch
            else:
                skipped += 1
        
        # Insert any remaining results
        if result_batch:
            try:
                supabase.table("results").upsert(
                    result_batch, 
                    on_conflict="athlete_id,event_id"
                ).execute()
                added += len(result_batch)
            except Exception as batch_error:
                print(f"Error inserting final results batch: {batch_error}")
                skipped += len(result_batch)
        
        print(f"  - {added} results processed, {skipped} skipped (missing required fields or duplicates)")
        return added, 0
    except Exception as e:
        print(f"Error inserting results data: {e}")
        return 0, 0

def insert_athlete_ratings(supabase, ratings_data):
    """Insert athlete ratings data into Supabase with complete history"""
    try:
        added = 0
        skipped = 0
        
        # Process in batches to avoid issues with large datasets
        batch_size = 100
        rating_batch = []
        seen_keys = set()  # Track unique (athlete_id, rating_date) combinations in current batch
        
        for athlete_id, rating_info in ratings_data.items():
            if athlete_id is not None:
                athlete_id = int(athlete_id)
                current_date = datetime.now().strftime("%Y-%m-%d")
                
                # Insert current rating if not already in batch
                batch_key = (athlete_id, current_date)
                if batch_key not in seen_keys:
                    seen_keys.add(batch_key)
                    rating_data = {
                        "athlete_id": athlete_id,
                        "rating_value": rating_info.get("current", 1500),
                        "rating_date": current_date,
                        "races_completed": rating_info.get("races_completed", 0),
                        "event_id": None  # No specific event for current rating
                    }
                    rating_batch.append(rating_data)
                
                # Insert complete rating history if available
                if "history" in rating_info:
                    # Sort history entries by date for consistency
                    history_entries = sorted(rating_info["history"], key=lambda x: x.get("date", ""))
                    
                    # Add initial rating as first history entry if not already included
                    if history_entries and "initial" in rating_info:
                        first_date = history_entries[0].get("date", "")
                        if first_date:
                            # Add the initial rating as a history point (if not in batch)
                            batch_key = (athlete_id, first_date)
                            if batch_key not in seen_keys:
                                seen_keys.add(batch_key)
                                initial_data = {
                                    "athlete_id": athlete_id,
                                    "rating_value": rating_info.get("initial", 1500),
                                    "rating_date": first_date,  # Use the same date as the first entry
                                    "races_completed": 0,
                                    "event_id": None
                                }
                                rating_batch.append(initial_data)
                    
                    # Process all history entries
                    for entry in history_entries:
                        entry_date = entry.get("date", "")
                        event_id = entry.get("event_id")
                        
                        # Skip entries with no date
                        if not entry_date:
                            continue
                        
                        # Skip if we already have an entry for this athlete and date
                        batch_key = (athlete_id, entry_date)
                        if batch_key in seen_keys:
                            continue
                            
                        seen_keys.add(batch_key)
                        
                        # Add rating history entry with the new rating
                        history_data = {
                            "athlete_id": athlete_id,
                            "rating_value": entry.get("new_elo", 1500),
                            "rating_date": entry_date,
                            "races_completed": entry.get("races_completed", 0),
                            "event_id": event_id
                        }
                        
                        rating_batch.append(history_data)
                        
                        # Process batch if it's full
                        if len(rating_batch) >= batch_size:
                            try:
                                supabase.table("athlete_ratings").upsert(
                                    rating_batch, 
                                    on_conflict="athlete_id,rating_date"
                                ).execute()
                                added += len(rating_batch)
                                rating_batch = []
                                seen_keys.clear()  # Reset seen keys for new batch
                            except Exception as batch_error:
                                print(f"Error inserting rating batch: {batch_error}")
                                skipped += len(rating_batch)
                                rating_batch = []
                                seen_keys.clear()  # Reset seen keys for new batch
            else:
                skipped += 1
        
        # Insert any remaining ratings
        if rating_batch:
            try:
                supabase.table("athlete_ratings").upsert(
                    rating_batch, 
                    on_conflict="athlete_id,rating_date"
                ).execute()
                added += len(rating_batch)
            except Exception as batch_error:
                print(f"Error inserting final rating batch: {batch_error}")
                skipped += len(rating_batch)
        
        print(f"  - {added} athlete rating entries processed, {skipped} skipped")
        return added, 0
    except Exception as e:
        print(f"Error inserting athlete ratings data: {e}")
        return 0, 0

def insert_elo_timeline(supabase, elo_timeline_data):
    """Insert ELO timeline data into Supabase"""
    try:
        added = 0
        skipped = 0
        
        # Process in batches to avoid issues with large datasets
        batch_size = 100
        timeline_batch = []
        seen_keys = set()  # Track unique (athlete_id, date) combinations
        
        for athlete_id, timeline_info in elo_timeline_data.items():
            if athlete_id is not None:
                athlete_id = int(athlete_id)
                
                # Process each point in the timeline
                for point in timeline_info.get("timeline", []):
                    date = point.get("date")
                    elo = point.get("elo")
                    
                    if not date or elo is None:
                        skipped += 1
                        continue
                    
                    # Skip if we already have an entry for this athlete and date in current batch
                    batch_key = (athlete_id, date)
                    if batch_key in seen_keys:
                        continue
                        
                    seen_keys.add(batch_key)
                    
                    # Add timeline entry
                    timeline_data = {
                        "athlete_id": athlete_id,
                        "date": date,
                        "elo_value": elo
                    }
                    
                    timeline_batch.append(timeline_data)
                    
                    # Process batch if it's full
                    if len(timeline_batch) >= batch_size:
                        try:
                            supabase.table("elo_timeline").upsert(
                                timeline_batch, 
                                on_conflict="athlete_id,date"
                            ).execute()
                            added += len(timeline_batch)
                            timeline_batch = []
                            seen_keys.clear()  # Reset seen keys for new batch
                        except Exception as batch_error:
                            print(f"Error inserting timeline batch: {batch_error}")
                            skipped += len(timeline_batch)
                            timeline_batch = []
                            seen_keys.clear()  # Reset seen keys for new batch
            else:
                skipped += 1
        
        # Insert any remaining timeline points
        if timeline_batch:
            try:
                supabase.table("elo_timeline").upsert(
                    timeline_batch, 
                    on_conflict="athlete_id,date"
                ).execute()
                added += len(timeline_batch)
            except Exception as batch_error:
                print(f"Error inserting final timeline batch: {batch_error}")
                skipped += len(timeline_batch)
        
        print(f"  - {added} timeline entries processed, {skipped} skipped")
        return added, 0
    except Exception as e:
        print(f"Error inserting ELO timeline data: {e}")
        return 0, 0

def insert_head_to_head_data(supabase, head_to_head_data):
    """Insert head-to-head data into Supabase"""
    try:
        added_pairs = 0
        added_meetings = 0
        skipped = 0
        
        # Step 1: First insert ALL pairs before inserting any meetings
        batch_size = 100
        pair_batch = []
        all_inserted_pairs = set()  # Track all successfully inserted pairs
        
        # Process all pairs first
        for pair_id, h2h_info in head_to_head_data.items():
            # Add the pair information
            pair_data = {
                "pair_id": pair_id,
                "athlete1_id": h2h_info.get("athlete1_id"),
                "athlete2_id": h2h_info.get("athlete2_id"),
                "athlete1_name": h2h_info.get("athlete1_name"),
                "athlete2_name": h2h_info.get("athlete2_name"),
                "encounters": h2h_info.get("encounters", 0),
                "athlete1_wins": h2h_info.get("athlete1_wins", 0),
                "athlete2_wins": h2h_info.get("athlete2_wins", 0)
            }
            
            pair_batch.append(pair_data)
            
            # Process pairs batch if it's full
            if len(pair_batch) >= batch_size:
                try:
                    supabase.table("head_to_head").upsert(
                        pair_batch, 
                        on_conflict="pair_id"
                    ).execute()
                    
                    # Track successfully inserted pairs
                    for pair in pair_batch:
                        all_inserted_pairs.add(pair["pair_id"])
                        
                    added_pairs += len(pair_batch)
                    pair_batch = []
                except Exception as batch_error:
                    print(f"Error inserting head-to-head pairs batch: {batch_error}")
                    skipped += len(pair_batch)
                    pair_batch = []
        
        # Insert any remaining pairs
        if pair_batch:
            try:
                supabase.table("head_to_head").upsert(
                    pair_batch, 
                    on_conflict="pair_id"
                ).execute()
                
                # Track successfully inserted pairs
                for pair in pair_batch:
                    all_inserted_pairs.add(pair["pair_id"])
                    
                added_pairs += len(pair_batch)
            except Exception as batch_error:
                print(f"Error inserting final head-to-head pairs batch: {batch_error}")
                skipped += len(pair_batch)
        
        print(f"  - {added_pairs} head-to-head pairs processed")
        
        # Step 2: Now process all meetings, but only for pairs that were successfully inserted
        meeting_batch = []
        seen_meeting_keys = set()  # Track unique meetings
        
        # Process meetings for all successfully inserted pairs
        for pair_id, h2h_info in head_to_head_data.items():
            # Skip if this pair wasn't successfully inserted
            if pair_id not in all_inserted_pairs:
                continue
                
            # Process all meetings for this pair
            for meeting in h2h_info.get("meetings", []):
                # Create a unique key for this meeting based on available data
                event_id = meeting.get("event_id")
                prog_id = meeting.get("prog_id")
                winner_id = meeting.get("winner_id")
                loser_id = meeting.get("loser_id")
                
                meeting_key = (pair_id, event_id, prog_id, winner_id, loser_id)
                
                # Skip if we've already seen this meeting in the current batch
                if meeting_key in seen_meeting_keys:
                    continue
                    
                seen_meeting_keys.add(meeting_key)
                
                meeting_data = {
                    "pair_id": pair_id,
                    "event_id": event_id,
                    "event_title": meeting.get("event_title"),
                    "event_date": meeting.get("event_date"),
                    "prog_id": prog_id,
                    "winner_id": winner_id,
                    "winner_position": meeting.get("winner_position"),
                    "loser_id": loser_id,
                    "loser_position": meeting.get("loser_position")
                }
                
                meeting_batch.append(meeting_data)
                
                # Process meetings batch if it's full
                if len(meeting_batch) >= batch_size:
                    try:
                        supabase.table("head_to_head_meetings").upsert(
                            meeting_batch
                        ).execute()
                        added_meetings += len(meeting_batch)
                        meeting_batch = []
                        seen_meeting_keys.clear()  # Reset seen meetings for new batch
                    except Exception as batch_error:
                        print(f"Error inserting meetings batch: {batch_error}")
                        skipped += len(meeting_batch)
                        meeting_batch = []
                        seen_meeting_keys.clear()  # Reset seen meetings for new batch
        
        # Insert any remaining meetings
        if meeting_batch:
            try:
                supabase.table("head_to_head_meetings").upsert(
                    meeting_batch
                ).execute()
                added_meetings += len(meeting_batch)
            except Exception as batch_error:
                print(f"Error inserting final meetings batch: {batch_error}")
                skipped += len(meeting_batch)
        
        print(f"  - {added_meetings} meetings processed, {skipped} skipped")
        return added_pairs + added_meetings, 0
    except Exception as e:
        print(f"Error inserting head-to-head data: {e}")
        return 0, 0

def insert_metadata(supabase, metadata):
    """Insert metadata into Supabase"""
    try:
        added = 0
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        metadata_batch = []
        seen_keys = set()  # Track unique keys
        
        for key, value in metadata.items():
            # Skip duplicates in the same batch
            if key in seen_keys:
                continue
                
            seen_keys.add(key)
            
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            
            metadata_data = {
                "key": key,
                "value": str(value),
                "updated_at": current_time
            }
            
            metadata_batch.append(metadata_data)
        
        # Add upload timestamp if not already in the batch
        if "upload_date" not in seen_keys:
            upload_data = {
                "key": "upload_date",
                "value": current_time,
                "updated_at": current_time
            }
            metadata_batch.append(upload_data)
        
        # Insert all metadata at once
        if metadata_batch:
            supabase.table("metadata").upsert(metadata_batch, on_conflict="key").execute()
            added = len(metadata_batch)
        
        print(f"  - {added} metadata entries processed")
        return added, 0
    except Exception as e:
        print(f"Error inserting metadata: {e}")
        return 0, 0

def upload_data_to_supabase(data_file=None, clear_existing=True, data=None):
    """
    Main function to upload data to Supabase
    
    Args:
        data_file (str): Path to JSON file with data to upload (optional if data is provided)
        clear_existing (bool): Whether to clear existing data in Supabase
        data (dict): Data to upload directly (optional, used if data_file not provided)
        
    Returns:
        tuple: (records_added, records_updated) or None if failed
    """
    try:
        # Load data if not provided directly
        if data is None:
            if data_file:
                # If data_file path is not absolute, assume it's in the data directory
                if not os.path.isabs(data_file) and not os.path.dirname(data_file):
                    data_file = os.path.join(DATA_DIR, data_file)
                data = load_data_from_json(data_file)
                if not data:
                    print(f"Error: Could not load data from {data_file}")
                    return None
            else:
                print("Error: Neither data_file nor data provided")
                return None
        
        # Get Supabase client
        supabase = get_supabase_client()
        if not supabase:
            return None
        
        print("Connected to Supabase successfully")
        
        # Create tables if needed
        create_tables_if_needed(supabase)
        
        # Clear existing data if requested
        clear_tables(supabase, clear_existing)
        
        # Upload data to Supabase
        print("Uploading data to Supabase...")
        
        # Insert athletes data
        athletes_added, _ = insert_athletes_data(supabase, data.get("athletes", {}))
        
        # Insert events data
        events_added, _ = insert_events_data(supabase, data.get("events", {}))
        
        # Insert results data
        results_added, _ = insert_results_data(supabase, data.get("results", []))
        
        # Insert athlete ratings data
        ratings_added, _ = insert_athlete_ratings(supabase, data.get("athlete_elo", {}))
        
        # Insert ELO timeline data
        timeline_added, _ = insert_elo_timeline(supabase, data.get("elo_timeline", {}))
        
        # Insert head-to-head data
        h2h_added, _ = insert_head_to_head_data(supabase, data.get("head_to_head", {}))
        
        # Insert metadata
        metadata_added, _ = insert_metadata(supabase, data.get("metadata", {}))
        
        records_added = athletes_added + events_added + results_added + ratings_added + timeline_added + h2h_added + metadata_added
        
        print(f"Supabase upload complete. {records_added} records processed.")
        return records_added, 0
        
    except Exception as e:
        print(f"Error uploading data to Supabase: {e}")
        return None

def load_data_from_json(file_path):
    """
    Load data from a JSON file
    
    Args:
        file_path (str): Path to the JSON file
        
    Returns:
        dict: The loaded data or None if file doesn't exist
    """
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File {file_path} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {file_path}.")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def main():
    """Main function for the Supabase uploader script"""
    # Default values
    data_file = "analyzed_data.json"
    clear_existing = True
    create_empty = False
    
    # Process command line arguments
    if len(sys.argv) > 1:
        # Check for help flag
        if sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print("Triathlon Supabase Uploader")
            print("Usage:")
            print("  python supabase_uploader.py [data_file] [y/n]")
            print("  python supabase_uploader.py --create")
            print("\nOptions:")
            print("  data_file       Path to JSON data file (default: analyzed_data.json)")
            print("  y/n             Clear existing data (y) or incremental update (n)")
            print("  --create        Create empty tables in Supabase")
            print("  --help, -h      Show this help message")
            return
        
        # Check for --create flag to create empty database
        if sys.argv[1] == "--create":
            create_empty = True
            data_file = None
            print("Creating empty tables in Supabase")
        else:
            data_file = sys.argv[1]
    
    # Add option for non-interactive mode with a second parameter
    if len(sys.argv) > 2 and sys.argv[2].lower() in ('y', 'yes'):
        clear_existing = True
        print("Running in non-interactive mode with full data replacement")
    elif len(sys.argv) > 2 and sys.argv[2].lower() in ('n', 'no'):
        clear_existing = False
        print("Running in non-interactive mode with incremental update")
    else:
        # Ask if existing data should be cleared
        clear_existing = False
        while True:
            user_input = input("\nDo you want to clear all existing data before uploading new data? (y/n, default=n): ")
            if user_input.lower() in ('', 'n', 'no'):
                clear_existing = False
                break
            elif user_input.lower() in ('y', 'yes'):
                clear_existing = True
                confirm = input("⚠️ WARNING: This will delete ALL existing data in Supabase. Are you sure? (y/n): ")
                if confirm.lower() in ('y', 'yes'):
                    break
                else:
                    print("Operation cancelled.")
                    return
            else:
                print("Invalid input. Please enter 'y' or 'n'.")
    
    # If creating empty database, provide minimal data structure
    if create_empty:
        empty_data = {
            "athletes": {},
            "events": {},
            "results": [],
            "athlete_elo": {},
            "metadata": {
                "date_created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "created_by": "supabase_uploader.py --create"
            }
        }
        upload_data_to_supabase(clear_existing=clear_existing, data=empty_data)
    else:
        # Upload data from file to Supabase
        upload_data_to_supabase(data_file=data_file, clear_existing=clear_existing)

if __name__ == "__main__":
    main() 