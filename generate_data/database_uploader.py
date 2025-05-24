import sqlite3
import json
import os
import sys
from datetime import datetime
import time

# Directory paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")

def load_data_from_json(file_path):
    """
    Load data from a JSON file
    
    Args:
        file_path (str): Path to the JSON file
        
    Returns:
        dict: The loaded data or None if file doesn't exist
    """
    # Check if the path already includes the data directory
    if os.path.dirname(file_path) == DATA_DIR or os.path.isabs(file_path):
        full_path = file_path
    else:
        # Prepend the data directory
        full_path = os.path.join(DATA_DIR, file_path)
        
    try:
        with open(full_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File {full_path} not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {full_path}.")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None

def create_database_tables(conn):
    """Create simplified database tables for the triathlon data"""
    cursor = conn.cursor()
    
    # Create athletes table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS athletes (
        athlete_id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        full_name TEXT,
        nationality TEXT,
        gender TEXT,
        birth_year INTEGER
    )
    ''')
    
    # Create events table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        event_id INTEGER PRIMARY KEY,
        title TEXT,
        date TEXT,
        importance INTEGER
    )
    ''')
    
    # Create results table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS results (
        result_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER,
        athlete_id INTEGER,
        position INTEGER,
        total_time TEXT,
        points INTEGER,
        FOREIGN KEY (event_id) REFERENCES events(event_id),
        FOREIGN KEY (athlete_id) REFERENCES athletes(athlete_id)
    )
    ''')
    
    # Create athlete ratings table with history
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS athlete_ratings (
        rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
        athlete_id INTEGER,
        rating_value REAL DEFAULT 1500,
        rating_date TEXT,
        races_completed INTEGER DEFAULT 0,
        event_id INTEGER,
        FOREIGN KEY (athlete_id) REFERENCES athletes(athlete_id),
        FOREIGN KEY (event_id) REFERENCES events(event_id)
    )
    ''')
    
    # Create metadata table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT
    )
    ''')
    
    # Commit all table creations
    conn.commit()
    return True

def optimize_database(conn):
    """Configure SQLite for optimal performance"""
    cursor = conn.cursor()
    
    # Set pragmas for better performance
    cursor.execute('PRAGMA synchronous = OFF')
    cursor.execute('PRAGMA journal_mode = MEMORY')
    cursor.execute('PRAGMA temp_store = MEMORY')
    cursor.execute('PRAGMA cache_size = 10000')
    
    conn.commit()

def insert_athletes_data(conn, athletes_data):
    """Insert athletes data using bulk operations"""
    cursor = conn.cursor()
    
    # Ensure database tables exist
    create_database_tables(conn)
    
    # Fetch existing athletes for faster lookup
    cursor.execute("SELECT athlete_id FROM athletes")
    existing_athletes = {row[0] for row in cursor.fetchall()}
    
    # Prepare data for bulk operations
    new_athletes = []
    update_athletes = []
    
    for athlete_id, athlete_info in athletes_data.items():
        details = athlete_info.get("details", {})
        athlete_id = int(athlete_id)  # Ensure ID is an integer
        
        if athlete_id in existing_athletes:
            # For update
            update_athletes.append((
                details.get("first_name"),
                details.get("last_name"),
                details.get("full_name"),
                details.get("country"),
                details.get("gender"),
                details.get("year_of_birth"),
                athlete_id
            ))
        else:
            # For insert
            new_athletes.append((
                athlete_id,
                details.get("first_name"),
                details.get("last_name"),
                details.get("full_name"),
                details.get("country"),
                details.get("gender"),
                details.get("year_of_birth")
            ))
    
    # Execute bulk operations
    if new_athletes:
        cursor.executemany('''
        INSERT INTO athletes (
            athlete_id, first_name, last_name, full_name, 
            nationality, gender, birth_year
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', new_athletes)
    
    if update_athletes:
        cursor.executemany('''
        UPDATE athletes SET
            first_name = ?,
            last_name = ?,
            full_name = ?,
            nationality = ?,
            gender = ?,
            birth_year = ?
        WHERE athlete_id = ?
        ''', update_athletes)
    
    conn.commit()
    print(f"  - {len(update_athletes)} athletes updated, {len(new_athletes)} athletes added")
    return len(new_athletes), len(update_athletes)

def insert_events_data(conn, events_data):
    """Insert events data using bulk operations"""
    cursor = conn.cursor()
    
    # Ensure database tables exist
    create_database_tables(conn)
    
    # Fetch existing events for faster lookup
    cursor.execute("SELECT event_id FROM events")
    existing_events = {row[0] for row in cursor.fetchall()}
    
    # Prepare data for bulk operations
    new_events = []
    update_events = []
    
    for event_id, event_info in events_data.items():
        event_id = int(event_id)  # Ensure ID is an integer
        
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
        
        if event_id in existing_events:
            # For update
            update_events.append((
                event_info.get("title"),
                event_info.get("date"),
                importance,
                event_id
            ))
        else:
            # For insert
            new_events.append((
                event_id,
                event_info.get("title"),
                event_info.get("date"),
                importance
            ))
    
    # Execute bulk operations
    if new_events:
        cursor.executemany('''
        INSERT INTO events (
            event_id, title, date, importance
        ) VALUES (?, ?, ?, ?)
        ''', new_events)
    
    if update_events:
        cursor.executemany('''
        UPDATE events SET
            title = ?,
            date = ?,
            importance = ?
        WHERE event_id = ?
        ''', update_events)
    
    conn.commit()
    print(f"  - {len(update_events)} events updated, {len(new_events)} events added")
    return len(new_events), len(update_events)

def insert_results_data(conn, results_data):
    """Insert results data using bulk operations"""
    cursor = conn.cursor()
    
    # Ensure database tables exist
    create_database_tables(conn)
    
    # Get all valid athlete IDs and event IDs for validation
    cursor.execute("SELECT athlete_id FROM athletes")
    valid_athlete_ids = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT event_id FROM events")
    valid_event_ids = {row[0] for row in cursor.fetchall()}
    
    # Build a set of existing results for faster lookup
    cursor.execute("SELECT athlete_id, event_id FROM results")
    existing_results = {(row[0], row[1]) for row in cursor.fetchall()}
    
    # Prepare data for bulk operations
    new_results = []
    update_results = []
    skipped_results = 0
    
    for result in results_data:
        athlete_id = result.get("athlete_id")
        event_id = result.get("event_id")
        
        # Validate IDs exist in their respective tables
        if (athlete_id is not None and event_id is not None and 
            athlete_id in valid_athlete_ids and event_id in valid_event_ids):
            
            key = (athlete_id, event_id)
            
            if key in existing_results:
                # For update
                update_results.append((
                    result.get("position"),
                    result.get("total_time"),
                    result.get("points", 0),
                    athlete_id,
                    event_id
                ))
            else:
                # For insert
                new_results.append((
                    athlete_id,
                    event_id, 
                    result.get("position"),
                    result.get("total_time"),
                    result.get("points", 0)
                ))
        else:
            skipped_results += 1
    
    # Execute bulk operations
    if new_results:
        cursor.executemany('''
        INSERT INTO results (
            athlete_id, event_id, position, total_time, points
        ) VALUES (?, ?, ?, ?, ?)
        ''', new_results)
    
    if update_results:
        cursor.executemany('''
        UPDATE results SET
            position = ?,
            total_time = ?,
            points = ?
        WHERE athlete_id = ? AND event_id = ?
        ''', update_results)
    
    conn.commit()
    print(f"  - {len(update_results)} results updated, {len(new_results)} results added, {skipped_results} skipped (invalid IDs)")
    return len(new_results), len(update_results)

def insert_athlete_ratings(conn, ratings_data):
    """Insert athlete ratings history data"""
    cursor = conn.cursor()
    
    # Ensure database tables exist
    create_database_tables(conn)
    
    # Get all valid athlete IDs for validation
    cursor.execute("SELECT athlete_id FROM athletes")
    valid_athlete_ids = {row[0] for row in cursor.fetchall()}
    
    # Get all valid event IDs for validation (for linking ratings to events)
    cursor.execute("SELECT event_id FROM events")
    valid_event_ids = {row[0] for row in cursor.fetchall()}
    
    # Fetch existing ratings for deduplication
    cursor.execute("SELECT athlete_id, rating_date FROM athlete_ratings")
    existing_ratings = {(row[0], row[1]) for row in cursor.fetchall()}
    
    # Prepare data for bulk operations
    new_ratings = []
    skipped_ratings = 0
    
    for athlete_id, rating_info in ratings_data.items():
        if athlete_id is not None:
            athlete_id = int(athlete_id)
            
            # Validate athlete ID exists
            if athlete_id in valid_athlete_ids:
                current_date = datetime.now().strftime("%Y-%m-%d")
                
                # Insert current rating
                if (athlete_id, current_date) not in existing_ratings:
                    new_ratings.append((
                        athlete_id,
                        rating_info.get("current", 1500),
                        current_date,
                        rating_info.get("races_completed", 0),
                        None  # No specific event for current rating
                    ))
                
                # Insert rating history if available
                if "history" in rating_info:
                    for entry in rating_info["history"]:
                        entry_date = entry.get("date", "")
                        event_id = entry.get("event_id")
                        
                        # Skip entries with no date
                        if not entry_date:
                            continue
                            
                        # Validate event ID if present
                        if event_id is not None and event_id not in valid_event_ids:
                            event_id = None  # Set to None if invalid
                            
                        # Skip if we already have a rating for this athlete on this date
                        if (athlete_id, entry_date) in existing_ratings:
                            continue
                            
                        # Add rating history entry
                        new_ratings.append((
                            athlete_id,
                            entry.get("new_elo", 1500),
                            entry_date,
                            0,  # races_completed not tracked in history
                            event_id
                        ))
            else:
                skipped_ratings += 1
    
    # Execute bulk operations
    if new_ratings:
        cursor.executemany('''
        INSERT INTO athlete_ratings (
            athlete_id, rating_value, rating_date, races_completed, event_id
        ) VALUES (?, ?, ?, ?, ?)
        ''', new_ratings)
    
    conn.commit()
    print(f"  - {len(new_ratings)} athlete rating history entries added, {skipped_ratings} skipped (invalid IDs)")
    return len(new_ratings), 0

def insert_metadata(conn, metadata):
    """Insert metadata for tracking database updates"""
    cursor = conn.cursor()
    
    # Ensure database tables exist
    create_database_tables(conn)
    
    metadata_entries = []
    
    for key, value in metadata.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        metadata_entries.append((key, str(value), datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    # Add upload timestamp
    metadata_entries.append(('upload_date', datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    # Bulk insert
    cursor.executemany('''
    INSERT OR REPLACE INTO metadata (key, value, updated_at)
    VALUES (?, ?, ?)
    ''', metadata_entries)
    
    conn.commit()
    return len(metadata_entries), 0

def clear_database_tables(conn):
    """Clear all data from the database tables"""
    cursor = conn.cursor()
    
    # Get all tables except sqlite_sequence
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'")
    tables = [table[0] for table in cursor.fetchall()]
    
    # Disable foreign key constraints temporarily
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    # Clear all tables
    cleared_tables = 0
    for table in tables:
        cursor.execute(f"DELETE FROM {table}")
        cleared_tables += 1
    
    # Check if sqlite_sequence exists before trying to delete from it
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'")
    if cursor.fetchone():
        # Reset the autoincrement counters
        cursor.execute("DELETE FROM sqlite_sequence")
    
    # Re-enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")
    
    conn.commit()
    print(f"Cleared data from {cleared_tables} tables")
    return cleared_tables

def upload_data_to_database(data_file=None, db_file="triathlon.db", clear_existing=True, data=None):
    """
    Main function to upload data to the database
    
    Args:
        data_file (str): Path to JSON file with data to upload (optional if data is provided)
        db_file (str): Path to SQLite database file
        clear_existing (bool): Whether to clear existing data in database
        data (dict): Data to upload directly (optional, used if data_file not provided)
        
    Returns:
        tuple: (records_added, records_updated) or None if failed
    """
    try:
        # Make DB path absolute if it's not already
        if not os.path.isabs(db_file):
            db_file = os.path.join(ROOT_DIR, db_file)
            
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
        
        # Check if database file exists
        db_exists = os.path.exists(db_file)
        
        # Create database directory if needed
        db_dir = os.path.dirname(db_file)
        if db_dir and not os.path.exists(db_dir):
            try:
                os.makedirs(db_dir)
                print(f"Created directory: {db_dir}")
            except OSError as e:
                print(f"Error creating directory {db_dir}: {e}")
                return None
        
        # Connect to database (SQLite will create the file if it doesn't exist)
        print(f"{'Opening existing' if db_exists else 'Creating new'} database: {db_file}")
        conn = sqlite3.connect(db_file)
        
        # Optimize database for better performance
        optimize_database(conn)
        
        # Clear existing data if requested
        if clear_existing:
            print("Clearing existing data...")
            clear_database_tables(conn)
        
        # Ensure required database tables exist
        print("Creating database schema...")
        create_database_tables(conn)
        
        # Upload data to database
        print("Uploading data to database...")
        
        # Insert athletes data
        athletes_added, athletes_updated = insert_athletes_data(conn, data.get("athletes", {}))
        
        # Insert events data
        events_added, events_updated = insert_events_data(conn, data.get("events", {}))
        
        # Insert results data
        results_added, results_updated = insert_results_data(conn, data.get("results", []))
        
        # Insert athlete ratings data (only adds entries, doesn't update)
        ratings_added, _ = insert_athlete_ratings(conn, data.get("athlete_elo", {}))
        
        # Insert metadata
        metadata_added, metadata_updated = insert_metadata(conn, data.get("metadata", {}))
        
        # Close database connection
        conn.close()
        
        records_added = athletes_added + events_added + results_added + ratings_added + metadata_added
        records_updated = athletes_updated + events_updated + results_updated + metadata_updated
        
        print(f"Database upload complete. Added {records_added} records, updated {records_updated} records.")
        return records_added, records_updated
        
    except Exception as e:
        print(f"Error uploading data to database: {e}")
        return None

def main():
    """Main function for the database uploader script"""
    # Default values
    data_file = "analyzed_data.json"
    db_file = "triathlon.db"
    clear_existing = True
    create_empty = False
    
    # Process command line arguments
    if len(sys.argv) > 1:
        # Check for help flag
        if sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print("Triathlon Database Uploader")
            print("Usage:")
            print("  python database_uploader.py [data_file] [db_file] [y/n]")
            print("  python database_uploader.py --create [db_file]")
            print("\nOptions:")
            print("  data_file       Path to JSON data file (default: analyzed_data.json)")
            print("  db_file         Path to SQLite database file (default: triathlon.db)")
            print("  y/n             Clear existing data (y) or incremental update (n)")
            print("  --create        Create an empty database with the required schema")
            print("  --help, -h      Show this help message")
            return
        
        # Check for --create flag to create empty database
        if sys.argv[1] == "--create":
            create_empty = True
            data_file = None
            # If db_file specified after --create flag
            if len(sys.argv) > 2:
                db_file = sys.argv[2]
            print(f"Creating empty database: {db_file}")
        else:
            data_file = sys.argv[1]
    
    if len(sys.argv) > 2 and not create_empty:
        db_file = sys.argv[2]
    
    # Add option for non-interactive mode with a third parameter
    if len(sys.argv) > 3 and sys.argv[3].lower() in ('y', 'yes'):
        clear_existing = True
        print("Running in non-interactive mode with full data replacement")
    elif len(sys.argv) > 3 and sys.argv[3].lower() in ('n', 'no'):
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
                confirm = input("⚠️ WARNING: This will delete ALL existing data in the database. Are you sure? (y/n): ")
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
                "created_by": "database_uploader.py --create"
            }
        }
        upload_data_to_database(db_file=db_file, clear_existing=clear_existing, data=empty_data)
    else:
        # Upload data from file to database
        upload_data_to_database(data_file, db_file, clear_existing)

if __name__ == "__main__":
    main() 