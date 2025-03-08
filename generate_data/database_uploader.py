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
    """Create the database tables for the triathlon data"""
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
        age INTEGER,
        birth_date TEXT
    )
    ''')
    
    # Create events table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS events (
        event_id INTEGER PRIMARY KEY,
        title TEXT,
        date TEXT,
        location TEXT,
        country TEXT,
        event_type TEXT,
        distance TEXT,
        year INTEGER,
        importance INTEGER
    )
    ''')
    
    # Create results table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS results (
        result_id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER,
        prog_id INTEGER,
        athlete_id INTEGER,
        position INTEGER,
        swim_time TEXT,
        bike_time TEXT,
        run_time TEXT,
        total_time TEXT,
        points INTEGER,
        FOREIGN KEY (event_id) REFERENCES events(event_id),
        FOREIGN KEY (athlete_id) REFERENCES athletes(athlete_id)
    )
    ''')
    
    # Create head-to-head table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS head_to_head (
        pair_id TEXT PRIMARY KEY,
        athlete1_id INTEGER,
        athlete2_id INTEGER,
        athlete1_name TEXT,
        athlete2_name TEXT,
        encounters INTEGER,
        athlete1_wins INTEGER,
        athlete2_wins INTEGER,
        FOREIGN KEY (athlete1_id) REFERENCES athletes(athlete_id),
        FOREIGN KEY (athlete2_id) REFERENCES athletes(athlete_id)
    )
    ''')
    
    # Create head-to-head meetings table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS head_to_head_meetings (
        meeting_id INTEGER PRIMARY KEY AUTOINCREMENT,
        pair_id TEXT,
        event_id INTEGER,
        event_title TEXT,
        event_date TEXT,
        prog_id INTEGER,
        winner_id INTEGER,
        winner_position INTEGER,
        loser_id INTEGER,
        loser_position INTEGER,
        FOREIGN KEY (pair_id) REFERENCES head_to_head(pair_id),
        FOREIGN KEY (event_id) REFERENCES events(event_id),
        FOREIGN KEY (winner_id) REFERENCES athletes(athlete_id),
        FOREIGN KEY (loser_id) REFERENCES athletes(athlete_id)
    )
    ''')
    
    # Create athlete ELO table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS athlete_elo (
        athlete_elo_id INTEGER PRIMARY KEY AUTOINCREMENT,
        athlete_id INTEGER UNIQUE,
        initial_elo REAL DEFAULT 1500,
        current_elo REAL DEFAULT 1500,
        races_completed INTEGER DEFAULT 0,
        FOREIGN KEY (athlete_id) REFERENCES athletes(athlete_id)
    )
    ''')
    
    # Create ELO history table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS elo_history (
        history_id INTEGER PRIMARY KEY AUTOINCREMENT,
        athlete_id INTEGER,
        event_id INTEGER,
        event_date TEXT,
        event_name TEXT,
        event_importance INTEGER,
        prog_id INTEGER,
        position INTEGER,
        status TEXT,
        old_elo REAL,
        new_elo REAL,
        old_rd REAL,
        new_rd REAL,
        old_volatility REAL,
        new_volatility REAL,
        elo_change REAL,
        opponents_faced INTEGER,
        FOREIGN KEY (athlete_id) REFERENCES athletes(athlete_id),
        FOREIGN KEY (event_id) REFERENCES events(event_id)
    )
    ''')
    
    # Create metadata table for tracking database versions, etc.
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT
    )
    ''')
    
    # Commit all table creations
    conn.commit()
    
    # Check and upgrade schema if needed
    return check_and_upgrade_schema(conn)

def optimize_database(conn):
    """Configure SQLite for optimal performance with large datasets"""
    cursor = conn.cursor()
    
    # Set pragmas for better performance
    cursor.execute('PRAGMA synchronous = OFF')  # Don't sync to disk on every write
    cursor.execute('PRAGMA journal_mode = MEMORY')  # Keep journal in memory for better performance
    cursor.execute('PRAGMA temp_store = MEMORY')  # Store temp tables in memory
    cursor.execute('PRAGMA cache_size = 10000')  # Use more memory for caching
    cursor.execute('PRAGMA page_size = 4096')  # Optimal page size
    cursor.execute('PRAGMA mmap_size = 30000000000')  # Use memory mapping for better performance
    
    conn.commit()

def insert_athletes_data(conn, athletes_data):
    """Insert athletes data using bulk operations"""
    cursor = conn.cursor()
    
    # Ensure database tables exist with the correct schema
    create_database_tables(conn)
    
    count = 0
    
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
                details.get("gender"),
                details.get("country"),
                details.get("noc"),
                details.get("year_of_birth"),
                details.get("profile_image"),
                athlete_id
            ))
        else:
            # For insert
            new_athletes.append((
                athlete_id,
                details.get("first_name"),
                details.get("last_name"),
                details.get("full_name"),
                details.get("gender"),
                details.get("country"),
                details.get("noc"),
                details.get("year_of_birth"),
                details.get("profile_image")
            ))
        count += 1
    
    # Execute bulk operations
    if new_athletes:
        cursor.executemany('''
        INSERT INTO athletes (
            athlete_id, first_name, last_name, full_name, 
            gender, country, noc, year_of_birth, profile_image_url
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', new_athletes)
    
    if update_athletes:
        cursor.executemany('''
        UPDATE athletes SET
            first_name = ?,
            last_name = ?,
            full_name = ?,
            gender = ?,
            country = ?,
            noc = ?,
            year_of_birth = ?,
            profile_image_url = ?
        WHERE athlete_id = ?
        ''', update_athletes)
    
    conn.commit()
    print(f"  - {len(update_athletes)} athletes updated, {len(new_athletes)} athletes added")
    return len(new_athletes), len(update_athletes)

def insert_events_data(conn, events_data):
    """Insert events and programs data using bulk operations"""
    cursor = conn.cursor()
    
    # Ensure database tables exist with the correct schema
    create_database_tables(conn)
    
    event_count = 0
    program_count = 0
    
    # Fetch existing events and programs for faster lookup
    cursor.execute("SELECT event_id FROM events")
    existing_events = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT program_id FROM programs")
    existing_programs = {row[0] for row in cursor.fetchall()}
    
    # Prepare data for bulk operations
    new_events = []
    update_events = []
    new_programs = []
    update_programs = []
    
    for event_id, event_info in events_data.items():
        event_id = int(event_id)  # Ensure ID is an integer
        
        if event_id in existing_events:
            # For update
            update_events.append((
                event_info.get("title"),
                event_info.get("date"),
                event_info.get("location", {}).get("country"),
                event_info.get("location", {}).get("venue"),
                event_id
            ))
        else:
            # For insert
            new_events.append((
                event_id,
                event_info.get("title"),
                event_info.get("date"),
                event_info.get("location", {}).get("country"),
                event_info.get("location", {}).get("venue")
            ))
        event_count += 1
        
        # Prepare program data
        for program in event_info.get("programs", []):
            program_id = program.get("prog_id")
            if program_id is not None:
                program_id = int(program_id)  # Ensure ID is an integer
                
                if program_id in existing_programs:
                    # For update
                    update_programs.append((
                        event_id,
                        program.get("prog_name"),
                        program.get("prog_distance"),
                        program_id
                    ))
                else:
                    # For insert
                    new_programs.append((
                        program_id,
                        event_id,
                        program.get("prog_name"),
                        program.get("prog_distance")
                    ))
                program_count += 1
    
    # Execute bulk operations
    if new_events:
        cursor.executemany('''
        INSERT INTO events (
            event_id, title, event_date, country, venue
        ) VALUES (?, ?, ?, ?, ?)
        ''', new_events)
    
    if update_events:
        cursor.executemany('''
        UPDATE events SET
            title = ?,
            event_date = ?,
            country = ?,
            venue = ?
        WHERE event_id = ?
        ''', update_events)
    
    if new_programs:
        cursor.executemany('''
        INSERT INTO programs (
            program_id, event_id, program_name, program_distance
        ) VALUES (?, ?, ?, ?)
        ''', new_programs)
    
    if update_programs:
        cursor.executemany('''
        UPDATE programs SET
            event_id = ?,
            program_name = ?,
            program_distance = ?
        WHERE program_id = ?
        ''', update_programs)
    
    conn.commit()
    print(f"  - {len(update_events)} events updated, {len(new_events)} events added")
    print(f"  - {len(update_programs)} programs updated, {len(new_programs)} programs added")
    return event_count, program_count

def insert_results_data(conn, results_data):
    """Insert results data using bulk operations"""
    cursor = conn.cursor()
    
    # Ensure database tables exist with the correct schema
    create_database_tables(conn)
    
    # Build a set of existing results for faster lookup
    cursor.execute("SELECT athlete_id, event_id, program_id FROM results")
    existing_results = {(row[0], row[1], row[2]) for row in cursor.fetchall()}
    
    # Prepare data for bulk operations
    new_results = []
    update_results = []
    
    for result in results_data:
        athlete_id = result.get("athlete_id")
        event_id = result.get("event_id")
        program_id = result.get("prog_id")
        
        if athlete_id is not None and event_id is not None and program_id is not None:
            key = (athlete_id, event_id, program_id)
            
            if key in existing_results:
                # For update
                update_results.append((
                    result.get("position"),
                    result.get("total_time"),
                    result.get("points", 0),
                    athlete_id,
                    event_id,
                    program_id
                ))
            else:
                # For insert
                new_results.append((
                    athlete_id,
                    event_id,
                    program_id,
                    result.get("position"),
                    result.get("total_time"),
                    result.get("points", 0)
                ))
    
    # Execute bulk operations
    if new_results:
        cursor.executemany('''
        INSERT INTO results (
            athlete_id, event_id, program_id, position, total_time, points
        ) VALUES (?, ?, ?, ?, ?, ?)
        ''', new_results)
    
    if update_results:
        cursor.executemany('''
        UPDATE results SET
            position = ?,
            total_time = ?,
            points = ?
        WHERE athlete_id = ? AND event_id = ? AND program_id = ?
        ''', update_results)
    
    conn.commit()
    print(f"  - {len(update_results)} results updated, {len(new_results)} results added")
    return len(new_results), len(update_results)

def insert_head_to_head_data_optimized(conn, head_to_head_data, clear_existing=True):
    """Insert head-to-head data using optimized approach for large datasets"""
    cursor = conn.cursor()
    
    # Ensure database tables exist with the correct schema
    create_database_tables(conn)
    
    # If clear_existing is True, delete existing data for a clean slate
    # This is much faster than checking each record
    if clear_existing:
        cursor.execute("DELETE FROM head_to_head_meetings")
        cursor.execute("DELETE FROM head_to_head")
        conn.commit()
        print("  - Cleared existing head-to-head data for faster processing")
    
    # Prepare for bulk insert
    h2h_batch = []
    meetings_batch = []
    batch_size = 10000  # Process in batches to avoid memory issues
    h2h_count = 0
    meeting_count = 0
    
    print(f"  - Processing {len(head_to_head_data)} head-to-head records...")
    
    # Process head-to-head records in batches
    for pair_id, h2h_info in head_to_head_data.items():
        # Prepare head-to-head record
        h2h_batch.append((
            pair_id,
            h2h_info.get("athlete1_id"),
            h2h_info.get("athlete2_id"),
            h2h_info.get("athlete1_name"),
            h2h_info.get("athlete2_name"),
            h2h_info.get("encounters", 0),
            h2h_info.get("athlete1_wins", 0),
            h2h_info.get("athlete2_wins", 0)
        ))
        h2h_count += 1
        
        # Prepare meeting records
        for meeting in h2h_info.get("meetings", []):
            meetings_batch.append((
                pair_id,
                meeting.get("event_id"),
                meeting.get("event_title"),
                meeting.get("event_date"),
                meeting.get("prog_id"),
                meeting.get("winner_id"),
                meeting.get("winner_position"),
                meeting.get("loser_id"),
                meeting.get("loser_position")
            ))
            meeting_count += 1
        
        # Insert in batches to avoid memory issues
        if len(h2h_batch) >= batch_size:
            cursor.executemany('''
            INSERT OR REPLACE INTO head_to_head (
                pair_id, athlete1_id, athlete2_id, athlete1_name, 
                athlete2_name, encounters, athlete1_wins, athlete2_wins
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', h2h_batch)
            h2h_batch = []
            print(f"  - Inserted batch of {batch_size} head-to-head records ({h2h_count} total)")
        
        if len(meetings_batch) >= batch_size:
            cursor.executemany('''
            INSERT INTO head_to_head_meetings (
                pair_id, event_id, event_title, event_date, prog_id,
                winner_id, winner_position, loser_id, loser_position
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', meetings_batch)
            meetings_batch = []
            print(f"  - Inserted batch of {batch_size} meeting records ({meeting_count} total)")
            
            # Commit after each batch to avoid transaction getting too large
            conn.commit()
    
    # Insert any remaining records
    if h2h_batch:
        cursor.executemany('''
        INSERT OR REPLACE INTO head_to_head (
            pair_id, athlete1_id, athlete2_id, athlete1_name, 
            athlete2_name, encounters, athlete1_wins, athlete2_wins
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', h2h_batch)
    
    if meetings_batch:
        cursor.executemany('''
        INSERT INTO head_to_head_meetings (
            pair_id, event_id, event_title, event_date, prog_id,
            winner_id, winner_position, loser_id, loser_position
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', meetings_batch)
    
    conn.commit()
    print(f"  - Processed {h2h_count} head-to-head records with {meeting_count} meetings")
    return h2h_count, meeting_count

def insert_elo_data(conn, elo_data):
    """Insert ELO ratings data into database using bulk operations"""
    cursor = conn.cursor()
    
    # Ensure database tables exist with the correct schema
    create_database_tables(conn)
    
    # Fetch existing ELO records for faster lookup
    cursor.execute("SELECT athlete_id FROM athlete_elo")
    existing_elo = {row[0] for row in cursor.fetchall()}
    
    # Build a set for faster lookups of history entries
    cursor.execute("SELECT athlete_id, event_id, event_date FROM elo_history")
    existing_history = {(row[0], row[1] or f"monthly_{row[2]}") for row in cursor.fetchall()}
    
    # Prepare data for bulk operations
    new_elo = []
    update_elo = []
    history_batch = []
    batch_size = 10000
    history_count = 0
    
    for athlete_id, elo_info in elo_data.items():
        if athlete_id is not None:
            athlete_id = int(athlete_id)
            
            if athlete_id in existing_elo:
                # For update
                update_elo.append((
                    elo_info.get("initial", 1500),
                    elo_info.get("current", 1500),
                    elo_info.get("races_completed", 0),
                    athlete_id
                ))
            else:
                # For insert
                new_elo.append((
                    athlete_id,
                    elo_info.get("initial", 1500),
                    elo_info.get("current", 1500),
                    elo_info.get("races_completed", 0)
                ))
            
            # Process history entries in batches
            for history_entry in elo_info.get("history", []):
                event_id = history_entry.get("event_id")
                
                # Generate a unique key for existing history check
                # For monthly updates (event_id is None), use the date as part of the key
                if event_id is None and history_entry.get("date"):
                    key = (athlete_id, f"monthly_{history_entry.get('date')}")
                else:
                    key = (athlete_id, event_id)
                
                # Check if this history entry already exists
                if key not in existing_history:
                    # Convert event_importance from string to integer
                    event_importance = history_entry.get("event_importance", "")
                    if isinstance(event_importance, str):
                        # Map string importance to integer values
                        importance_map = {
                            "olympic": 5,
                            "world": 4,
                            "major": 3,
                            "regional": 2,
                            "local": 1,
                            "monthly": 0,  # For monthly updates
                            "": 0  # Default
                        }
                        event_importance = importance_map.get(event_importance.lower(), 0)
                    
                    # Format date properly if it's a datetime object or None
                    entry_date = history_entry.get("date")
                    if entry_date is None:
                        entry_date = ""
                    elif not isinstance(entry_date, str):
                        # Try to convert to string if it's another type
                        try:
                            entry_date = str(entry_date)
                        except:
                            entry_date = ""
                    
                    # Get opponent information
                    opponents_faced = history_entry.get("opponents_faced", 0)
                    
                    # Get Glicko-2 specific fields with fallbacks
                    old_elo = history_entry.get("old_elo", 0)
                    new_elo_val = history_entry.get("new_elo", 0)
                    elo_change = history_entry.get("change", new_elo_val - old_elo)
                    
                    old_rd = history_entry.get("old_rd", 350.0)  
                    new_rd = history_entry.get("new_rd", 350.0)
                    
                    old_volatility = history_entry.get("old_volatility", 0.06)
                    new_volatility = history_entry.get("new_volatility", 0.06)
                    
                    status = history_entry.get("status", "")
                    
                    # Create the history batch entry with new Glicko-2 fields
                    history_batch.append((
                        athlete_id,
                        event_id,
                        entry_date,
                        history_entry.get("event_name", ""),
                        event_importance,
                        history_entry.get("prog_id"),
                        history_entry.get("position"),
                        status,
                        old_elo,
                        new_elo_val,
                        old_rd,
                        new_rd,
                        old_volatility,
                        new_volatility,
                        elo_change,
                        opponents_faced
                    ))
                    history_count += 1
                
                # Insert in batches to avoid memory issues
                if len(history_batch) >= batch_size:
                    cursor.executemany('''
                    INSERT INTO elo_history (
                        athlete_id, event_id, event_date, event_name, event_importance, prog_id, position,
                        status, old_elo, new_elo, old_rd, new_rd, old_volatility, new_volatility, 
                        elo_change, opponents_faced
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', history_batch)
                    history_batch = []
                    print(f"  - Inserted batch of {batch_size} ELO history entries ({history_count} total)")
                    conn.commit()
    
    # Execute bulk operations
    if new_elo:
        cursor.executemany('''
        INSERT INTO athlete_elo (
            athlete_id, initial_elo, current_elo, races_completed
        ) VALUES (?, ?, ?, ?)
        ''', new_elo)
    
    if update_elo:
        cursor.executemany('''
        UPDATE athlete_elo SET
            initial_elo = ?,
            current_elo = ?,
            races_completed = ?
        WHERE athlete_id = ?
        ''', update_elo)
    
    # Insert any remaining history entries
    if history_batch:
        cursor.executemany('''
        INSERT INTO elo_history (
            athlete_id, event_id, event_date, event_name, event_importance, prog_id, position,
            status, old_elo, new_elo, old_rd, new_rd, old_volatility, new_volatility, 
            elo_change, opponents_faced
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', history_batch)
    
    conn.commit()
    print(f"  - {len(update_elo)} athlete ELO records updated, {len(new_elo)} added")
    print(f"  - {history_count} ELO history entries added")
    return len(new_elo) + len(update_elo), history_count

def insert_metadata(conn, metadata):
    """Insert metadata for tracking database updates"""
    cursor = conn.cursor()
    
    # Ensure database tables exist with the correct schema
    create_database_tables(conn)
    
    metadata_entries = []
    
    for key, value in metadata.items():
        if isinstance(value, (dict, list)):
            value = json.dumps(value)
        metadata_entries.append((key, str(value)))
    
    # Add upload timestamp
    metadata_entries.append(('upload_date', datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    
    # Bulk insert
    cursor.executemany('''
    INSERT OR REPLACE INTO metadata (key, value)
    VALUES (?, ?)
    ''', metadata_entries)
    
    conn.commit()
    return len(metadata_entries), 0  # Return a tuple (records_added, records_updated)

def verify_upload(conn):
    """Verify the database upload with summary counts"""
    cursor = conn.cursor()
    results = {}
    
    # Get counts from each table
    tables = [
        "athletes", "events", "programs", "results", 
        "head_to_head", "head_to_head_meetings", 
        "athlete_elo", "elo_history"
    ]
    
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        results[table] = count
    
    # Get metadata
    cursor.execute("SELECT key, value FROM metadata")
    metadata = {row[0]: row[1] for row in cursor.fetchall()}
    
    return results, metadata

def clear_database_tables(conn):
    """Clear all data from the database tables"""
    cursor = conn.cursor()
    
    # Get all tables except sqlite_sequence (which stores autoincrement values)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'")
    tables = [table[0] for table in cursor.fetchall()]
    
    # Disable foreign key constraints temporarily
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    # Clear all tables
    cleared_tables = 0
    for table in tables:
        cursor.execute(f"DELETE FROM {table}")
        cleared_tables += 1
    
    # Reset the autoincrement counters
    cursor.execute("DELETE FROM sqlite_sequence")
    
    # Re-enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")
    
    conn.commit()
    print(f"Cleared data from {cleared_tables} tables")
    return cleared_tables

def check_and_upgrade_schema(conn):
    """Check if the schema needs to be upgraded and perform upgrades as needed"""
    cursor = conn.cursor()
    upgrades_performed = []
    
    try:
        # Drop the elo_timeline table if it exists (we're removing it since it duplicates elo_history)
        cursor.execute("DROP TABLE IF EXISTS elo_timeline")
        upgrades_performed.append("Removed elo_timeline table (functionality merged into elo_history)")
        
        # Check if the athlete_elo table needs to be upgraded
        cursor.execute("PRAGMA table_info(athlete_elo)")
        athlete_elo_columns = {row[1] for row in cursor.fetchall()}
        
        # Check if races_completed column exists in athlete_elo
        if "races_completed" not in athlete_elo_columns:
            print("Adding races_completed column to athlete_elo table")
            cursor.execute("ALTER TABLE athlete_elo ADD COLUMN races_completed INTEGER DEFAULT 0")
            upgrades_performed.append("Added races_completed column to athlete_elo table")
        
        # Check if the elo_history table needs to be upgraded
        cursor.execute("PRAGMA table_info(elo_history)")
        existing_columns = {row[1] for row in cursor.fetchall()}
        
        # Check if we need to rebuild the table completely
        missing_columns = []
        
        # Check for missing columns in elo_history - updated for Glicko-2
        required_columns = {
            "event_name", "event_importance", "prog_id", "position", "status",
            "old_elo", "new_elo", "old_rd", "new_rd", "old_volatility", "new_volatility",
            "elo_change", "opponents_faced"
        }
        
        for col in required_columns:
            if col not in existing_columns:
                missing_columns.append(col)
        
        # If there are missing columns, we need to rebuild the table
        if missing_columns:
            print(f"Upgrading elo_history table - missing columns: {', '.join(missing_columns)}")
            try:
                # Backup existing data if the table exists
                if len(existing_columns) > 0:
                    cursor.execute("CREATE TABLE IF NOT EXISTS elo_history_backup AS SELECT * FROM elo_history")
                    
                    # Drop the existing table
                    cursor.execute("DROP TABLE IF EXISTS elo_history")
                
                # Create the table with the new schema - updated for Glicko-2
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS elo_history (
                    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    athlete_id INTEGER,
                    event_id INTEGER,
                    event_date TEXT,
                    event_name TEXT,
                    event_importance INTEGER,
                    prog_id INTEGER,
                    position INTEGER,
                    status TEXT,
                    old_elo REAL,
                    new_elo REAL,
                    old_rd REAL,
                    new_rd REAL,
                    old_volatility REAL,
                    new_volatility REAL,
                    elo_change REAL,
                    opponents_faced INTEGER,
                    FOREIGN KEY (athlete_id) REFERENCES athletes(athlete_id),
                    FOREIGN KEY (event_id) REFERENCES events(event_id)
                )
                ''')
                
                # Copy existing data back if we backed it up
                if len(existing_columns) > 0:
                    # Get list of columns from the backup table
                    cursor.execute("PRAGMA table_info(elo_history_backup)")
                    backup_columns = [row[1] for row in cursor.fetchall()]
                    
                    # Copy data preserving compatible columns - updated for Glicko-2
                    compatible_columns = [col for col in backup_columns if col in existing_columns and col in [
                        "history_id", "athlete_id", "event_id", "event_date", "event_name", 
                        "event_importance", "prog_id", "position", "old_elo", "new_elo", "elo_change", "opponents_faced"
                    ]]
                    if compatible_columns:
                        columns_str = ", ".join(compatible_columns)
                        try:
                            cursor.execute(f'''
                            INSERT INTO elo_history ({columns_str})
                            SELECT {columns_str} FROM elo_history_backup
                            ''')
                            print(f"  - Restored data for columns: {columns_str}")
                        except sqlite3.Error as e:
                            print(f"  - Error restoring data: {e}")
                    
                    # Drop the backup table to save space
                    cursor.execute("DROP TABLE IF EXISTS elo_history_backup")
                
                upgrades_performed.append("Upgraded elo_history table with new fields")
            except sqlite3.Error as e:
                print(f"Error upgrading elo_history table: {e}")
        
        conn.commit()
        return upgrades_performed
    except Exception as e:
        print(f"Error during schema upgrade: {e}")
        return upgrades_performed

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
        
        print(f"Opening database connection to {db_file}")
        conn = sqlite3.connect(db_file)
        
        # Optimize database for better performance
        optimize_database(conn)
        
        # Clear existing data if requested
        if clear_existing:
            print("Clearing existing data...")
            clear_database_tables(conn)
        
        # Ensure required database tables exist
        print("Checking database schema...")
        upgrades = create_database_tables(conn)
        if upgrades:
            print(f"Schema upgrades performed: {', '.join(upgrades)}")
        
        # Upload data to database
        print("Uploading data to database...")
        
        # Insert athletes data
        athletes_added, athletes_updated = insert_athletes_data(conn, data.get("athletes", {}))
        
        # Insert events data
        events_added, events_updated = insert_events_data(conn, data.get("events", {}))
        
        # Insert results data
        results_added, results_updated = insert_results_data(conn, data.get("results", []))
        
        # Insert head-to-head data
        h2h_added, h2h_meetings = insert_head_to_head_data_optimized(conn, data.get("head_to_head", {}), clear_existing=False)
        
        # Insert ELO data
        elo_added, elo_history_added = insert_elo_data(conn, data.get("athlete_elo", {}))
        
        # Insert metadata
        metadata_added, metadata_updated = insert_metadata(conn, data.get("metadata", {}))
        
        # Fix any missing data in ELO history records
        print("Checking and fixing ELO history data...")
        fixed_records = fix_elo_history_data(conn)
        
        # Optimize database after data upload
        print("Optimizing database...")
        optimize_database(conn)
        
        # Close database connection
        conn.close()
        
        records_added = athletes_added + events_added + results_added + h2h_added + elo_added + metadata_added
        records_updated = athletes_updated + events_updated + results_updated + elo_history_added + metadata_updated
        
        print(f"Database upload complete. Added {records_added} records, updated {records_updated} records.")
        print(f"Fixed {fixed_records} ELO history records with missing data.")
        return records_added, records_updated
        
    except Exception as e:
        print(f"Error uploading data to database: {e}")
        return None

def main():
    """Main function for the database uploader script"""
    # Default values
    data_file = "analyzed_data.json"
    db_file = "triathlon.db"
    
    # Process command line arguments
    if len(sys.argv) > 1:
        data_file = sys.argv[1]
    
    if len(sys.argv) > 2:
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
    
    # Upload data to database
    upload_data_to_database(data_file, db_file, clear_existing)

def fix_elo_history_data(conn):
    """Fix any existing elo_history records with missing or incorrect data"""
    cursor = conn.cursor()
    print("Checking for missing data in elo_history records...")
    
    # First check if we have records with missing data - adapted for new Glicko-2 schema
    cursor.execute("""
    SELECT COUNT(*) FROM elo_history 
    WHERE event_name = '' OR event_importance = 0 OR old_rd IS NULL OR new_rd IS NULL 
    OR old_volatility IS NULL OR new_volatility IS NULL
    """)
    missing_data_count = cursor.fetchone()[0]
    
    if missing_data_count == 0:
        print("No elo_history records with missing data found.")
        return 0
    
    print(f"Found {missing_data_count} elo_history records with potentially missing data.")
    
    # Get the events data to fill in missing event names
    cursor.execute("SELECT event_id, title FROM events")
    event_names = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Check for records with missing event names
    cursor.execute("SELECT history_id, event_id FROM elo_history WHERE event_name = '' AND event_id IS NOT NULL")
    missing_names = cursor.fetchall()
    
    name_updates = 0
    if missing_names:
        updates = []
        for history_id, event_id in missing_names:
            if event_id in event_names:
                updates.append((event_names[event_id], history_id))
        
        if updates:
            cursor.executemany("UPDATE elo_history SET event_name = ? WHERE history_id = ?", updates)
            name_updates = len(updates)
            print(f"Updated {name_updates} records with missing event names.")
    
    # Check for missing event importance values
    cursor.execute("""
    SELECT history_id, event_name FROM elo_history 
    WHERE event_importance = 0 AND event_name != ''
    """)
    missing_importance = cursor.fetchall()
    
    importance_updates = 0
    if missing_importance:
        # Define the importance mapping
        importance_map = {
            "olympic": 5,
            "world": 4,
            "major": 3,
            "regional": 2,
            "local": 1
        }
        
        updates = []
        for history_id, event_name in missing_importance:
            # Determine importance from name
            event_name_lower = event_name.lower()
            importance = 1  # Default to local
            
            if "olympic" in event_name_lower or "olympics" in event_name_lower or "world championship" in event_name_lower:
                importance = 5
            elif any(term in event_name_lower for term in ["world cup", "world series", "wtcs", "wts", "grand final"]):
                importance = 4
            elif any(term in event_name_lower for term in ["continental", "european championship", "ironman", "70.3"]):
                importance = 3
            elif any(term in event_name_lower for term in ["national", "cup", "series"]):
                importance = 2
                
            updates.append((importance, history_id))
        
        if updates:
            cursor.executemany("UPDATE elo_history SET event_importance = ? WHERE history_id = ?", updates)
            importance_updates = len(updates)
            print(f"Updated {importance_updates} records with missing event importance values.")
    
    # Check for missing status values
    cursor.execute("SELECT COUNT(*) FROM elo_history WHERE status IS NULL OR status = ''")
    missing_status_count = cursor.fetchone()[0]
    
    status_updates = 0
    if missing_status_count > 0:
        # Set a default status for monthly updates
        cursor.execute("""
        UPDATE elo_history 
        SET status = 'MONTHLY_UPDATE'
        WHERE (status IS NULL OR status = '') AND event_name LIKE 'Monthly rating period%'
        """)
        
        # For regular events, set to COMPLETED
        cursor.execute("""
        UPDATE elo_history 
        SET status = 'COMPLETED'
        WHERE (status IS NULL OR status = '') AND event_name NOT LIKE 'Monthly rating period%'
        """)
        
        status_updates = missing_status_count
        print(f"Updated {status_updates} records with missing status values.")
    
    # Check for missing Glicko-2 specific values (RD and volatility)
    cursor.execute("""
    SELECT COUNT(*) FROM elo_history 
    WHERE old_rd IS NULL OR new_rd IS NULL OR old_volatility IS NULL OR new_volatility IS NULL
    """)
    missing_glicko_params = cursor.fetchone()[0]
    
    glicko_updates = 0
    if missing_glicko_params > 0:
        # Set reasonable defaults for Glicko-2 parameters
        cursor.execute("""
        UPDATE elo_history 
        SET 
            old_rd = CASE WHEN old_rd IS NULL THEN 350.0 ELSE old_rd END,
            new_rd = CASE WHEN new_rd IS NULL THEN 350.0 ELSE new_rd END,
            old_volatility = CASE WHEN old_volatility IS NULL THEN 0.06 ELSE old_volatility END,
            new_volatility = CASE WHEN new_volatility IS NULL THEN 0.06 ELSE new_volatility END
        WHERE old_rd IS NULL OR new_rd IS NULL OR old_volatility IS NULL OR new_volatility IS NULL
        """)
        
        glicko_updates = missing_glicko_params
        print(f"Updated {glicko_updates} records with missing Glicko-2 parameters.")
    
    conn.commit()
    
    total_updates = name_updates + importance_updates + status_updates + glicko_updates
    print(f"Total elo_history records updated: {total_updates}")
    
    return total_updates

if __name__ == "__main__":
    main() 