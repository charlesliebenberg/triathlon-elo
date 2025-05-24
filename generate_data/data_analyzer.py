import sys
import time
import math
import os
from datetime import datetime
from collections import defaultdict

# Directory paths
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")

# Import local modules
try:
    from .utils import load_data_from_json, save_data_to_json
except ImportError:
    # Fallback for direct script execution
    from utils import load_data_from_json, save_data_to_json

# Import rating calculation module
try:
    from .glicko2 import calculate_elo_ratings
except ImportError:
    try:
        from glicko2 import calculate_elo_ratings
    except ImportError:
        print("Error: glicko2.py module not found. Make sure it's in the same directory.")
        sys.exit(1)

# Try to import database uploaders
try:
    from .database_uploader import upload_data_to_database
except ImportError:
    try:
        from database_uploader import upload_data_to_database
    except ImportError:
        print("Warning: database_uploader module not found. Database upload functionality will be disabled.")
        upload_data_to_database = None

# Try to import Supabase uploader
try:
    from .supabase_uploader import upload_data_to_supabase
except ImportError:
    try:
        from supabase_uploader import upload_data_to_supabase
    except ImportError:
        print("Warning: supabase_uploader module not found. Supabase upload functionality will be disabled.")
        upload_data_to_supabase = None

def generate_head_to_head_stats(results_data, athletes_data):
    """
    Generate head-to-head statistics for athletes based on results.
    
    Args:
        results_data (dict): Results data with events and results
        athletes_data (dict): Athletes data with details
        
    Returns:
        dict: Head-to-head data for pairs of athletes
    """
    print("Generating head-to-head statistics...")
    
    # Create a lookup for event dates
    event_date_lookup = {}
    for event_id, event_data in results_data.get("events", {}).items():
        event_date = event_data.get("date")
        if event_date:
            # Store with both string and int keys for flexible lookup
            event_date_lookup[event_id] = event_date
            try:
                event_id_int = int(event_id)
                event_date_lookup[event_id_int] = event_date
            except (ValueError, TypeError):
                pass
    
    # Group results by event and program
    event_results = defaultdict(list)
    for result in results_data.get("results", []):
        event_id = result.get("event_id")
        prog_id = result.get("prog_id", 0)  # Default to 0 if prog_id doesn't exist
        if not event_id:
            continue
        
        key = f"{event_id}_{prog_id}"
        event_results[key].append(result)
    
    print(f"Grouped results into {len(event_results)} unique event-programs")
    
    # Initialize head-to-head data
    head_to_head = {}
    
    # Process each event-program
    for event_key, results in event_results.items():
        # Sort results by position
        sorted_results = sorted(
            results, 
            key=lambda x: int(x["position"]) if x.get("position") and isinstance(x["position"], int) else float('inf')
        )
        
        # Skip if we have fewer than 2 athletes
        if len(sorted_results) < 2:
            continue
        
        # Compare each athlete with all athletes that finished behind them
        for i, winner in enumerate(sorted_results):
            winner_id = winner.get("athlete_id")
            winner_position = winner.get("position")
            
            if not winner_id or not winner_position or not isinstance(winner_position, int):
                continue
                
            for loser in sorted_results[i+1:]:
                loser_id = loser.get("athlete_id")
                loser_position = loser.get("position")
                
                if not loser_id or not loser_position or not isinstance(loser_position, int):
                    continue
                
                # Create unique pair identifier (always put smaller ID first for consistency)
                smaller_id = min(winner_id, loser_id)
                larger_id = max(winner_id, loser_id)
                pair_id = f"{smaller_id}-{larger_id}"
                
                # Initialize head-to-head record if it doesn't exist
                if pair_id not in head_to_head:
                    # Get athlete names
                    athlete1 = athletes_data.get("athletes", {}).get(str(smaller_id), {})
                    athlete2 = athletes_data.get("athletes", {}).get(str(larger_id), {})
                    
                    athlete1_name = athlete1.get("details", {}).get("full_name", f"Athlete {smaller_id}")
                    athlete2_name = athlete2.get("details", {}).get("full_name", f"Athlete {larger_id}")
                    
                    head_to_head[pair_id] = {
                        "athlete1_id": smaller_id,
                        "athlete1_name": athlete1_name,
                        "athlete2_id": larger_id,
                        "athlete2_name": athlete2_name,
                        "encounters": 0,
                        "athlete1_wins": 0,
                        "athlete2_wins": 0,
                        "meetings": []
                    }
                
                # Update encounter count
                head_to_head[pair_id]["encounters"] += 1
                
                # Update win count based on which athlete is which
                if winner_id == smaller_id:
                    head_to_head[pair_id]["athlete1_wins"] += 1
                else:
                    head_to_head[pair_id]["athlete2_wins"] += 1
                
                # Get event details
                event_id, prog_id = event_key.split("_")
                event = results_data.get("events", {}).get(event_id, {})
                
                # Get event date from lookup
                event_date = event_date_lookup.get(event_id, "")
                if not event_date and event_id.isdigit():
                    event_date = event_date_lookup.get(int(event_id), "")
                
                event_title = event.get("title", "Unknown Event")
                
                meeting = {
                    "event_id": event_id,
                    "prog_id": prog_id,
                    "event_title": event_title,
                    "event_date": event_date,
                    "winner_id": winner_id,
                    "winner_position": winner_position,
                    "loser_id": loser_id,
                    "loser_position": loser_position
                }
                
                head_to_head[pair_id]["meetings"].append(meeting)
    
    print(f"Generated head-to-head stats for {len(head_to_head)} athlete pairs")
    return head_to_head

def determine_event_importance(event_name):
    """
    Determine event importance based on its name.
    
    Args:
        event_name (str): Event name/title
        
    Returns:
        int: Importance level (5=Olympic, 4=World, 3=Major, 2=Regional, 1=Local)
    """
    event_name = event_name.lower()
    
    # Olympic-level events
    if any(term in event_name for term in ["olympic", "olympics", "world championship"]):
        return 5
    
    # World-level events
    elif any(term in event_name for term in [
        "world cup", "world series", "world triathlon championship series", 
        "wtcs", "wts", "grand final", "championship final"
    ]):
        return 4
    
    # Major events
    elif any(term in event_name for term in [
        "continental championship", "european championship", "asian championship",
        "american championship", "oceania championship", "african championship",
        "ironman", "70.3", "half ironman", "challenge", "super league"
    ]):
        return 3
    
    # Regional events
    elif any(term in event_name for term in [
        "national championship", "cup", "series", "continental cup", 
        "european cup", "asian cup", "american cup", "oceania cup", "african cup"
    ]):
        return 2
    
    # Default to local
    else:
        return 1

def generate_elo_timeline(elo_ratings, athletes_data):
    """
    Generate a timeline showing Glicko-2 rating progression for each athlete.
    
    Args:
        elo_ratings (dict): Glicko-2 ratings data with history
        athletes_data (dict): Athletes data with details
        
    Returns:
        dict: Timeline data showing rating progress by date
    """
    print("Generating Glicko-2 rating timeline data...")
    
    timeline_data = {}
    
    for athlete_id, elo_data in elo_ratings.items():
        # Get athlete details
        athlete = athletes_data.get("athletes", {}).get(athlete_id, {})
        name = athlete.get("details", {}).get("full_name", f"Athlete {athlete_id}")
        
        # Skip if no history
        if not elo_data.get("history"):
            continue
            
        # Sort history by date
        history = sorted(elo_data["history"], key=lambda x: x["date"])
        
        # Create timeline with initial point
        timeline = [{"date": history[0]["date"], "elo": elo_data["initial"]}]
        date_elo_map = {}
        
        # Process each history entry
        for entry in history:
            date = entry["date"]
            new_elo = entry["new_elo"]
            
            # Store the latest rating for each date
            date_elo_map[date] = new_elo
        
        # Convert to timeline format
        for date, elo in sorted(date_elo_map.items()):
            timeline.append({
                "date": date,
                "elo": elo
            })
        
        # Add to timeline data
        timeline_data[athlete_id] = {
            "name": name,
            "initial_elo": elo_data["initial"],
            "final_elo": elo_data["current"],
            "timeline": timeline
        }
    
    return timeline_data

def check_for_supabase():
    """Check if Supabase environment variables are set"""
    if os.path.exists(os.path.join(ROOT_DIR, '.env')):
        try:
            from dotenv import load_dotenv
            load_dotenv()
            supabase_url = os.environ.get("SUPABASE_URL")
            supabase_key = os.environ.get("SUPABASE_KEY")
            
            if supabase_url and supabase_key and upload_data_to_supabase:
                print("Supabase credentials found - will use Supabase for database storage")
                return True
        except Exception as e:
            print(f"Error checking for Supabase credentials: {e}")
    
    return False

def analyze_data(results_file="results_data.json", athletes_file="athletes_data.json", output_file="analyzed_data.json", 
             limit_athletes=None, db_upload=True, db_file="triathlon.db", clear_existing=True):
    """
    Analyze data from results and athletes files to create final dataset
    
    Params:
        results_file (str): File with results data
        athletes_file (str): File with athletes data
        output_file (str or None): File to save analyzed data to (None if database only)
        limit_athletes (int): Optional limit on number of athletes to process
        db_upload (bool): Whether to upload data directly to database
        db_file (str): Database file path to use if db_upload is True
        clear_existing (bool): Whether to clear existing data in database before upload
        
    Returns:
        dict: The analyzed data
    """
    # Make paths absolute with data directory if needed
    if not os.path.isabs(results_file) and not os.path.dirname(results_file):
        results_file = os.path.join(DATA_DIR, results_file)
    
    if not os.path.isabs(athletes_file) and not os.path.dirname(athletes_file):
        athletes_file = os.path.join(DATA_DIR, athletes_file)
    
    # Handle output_file being None (database-only mode)
    if output_file is not None:
        if not os.path.isabs(output_file) and not os.path.dirname(output_file):
            output_file = os.path.join(DATA_DIR, output_file)
    
    if not os.path.isabs(db_file):
        db_file = os.path.join(DATA_DIR, db_file)
    
    # Check if we should use Supabase instead of SQLite
    use_supabase = check_for_supabase()
        
    print(f"\n🔬 TRIATHLON DATA ANALYZER 🔬")
    print(f"Loading results from {results_file}")
    print(f"Loading athletes from {athletes_file}")
    if output_file:
        print(f"Will save analyzed data to {output_file}")
    else:
        print(f"Database-only mode (no JSON output file)")
    
    if db_upload:
        if use_supabase:
            print("Data will be uploaded to Supabase")
        else:
            print(f"Data will be uploaded to SQLite database: {db_file}")
    
    # Load input data
    results_data = load_data_from_json(results_file)
    if not results_data:
        print(f"Error: Could not load results data from {results_file}")
        return None
    
    athletes_data = load_data_from_json(athletes_file)
    if not athletes_data:
        print(f"Error: Could not load athletes data from {athletes_file}")
        return None
    
    # Limit athletes if specified
    if limit_athletes and isinstance(limit_athletes, int):
        print(f"Limiting analysis to {limit_athletes} athletes for testing")
        athlete_ids = list(athletes_data["athletes"].keys())[:limit_athletes]
        limited_athletes = {athlete_id: athletes_data["athletes"][athlete_id] for athlete_id in athlete_ids}
        athletes_data["athletes"] = limited_athletes
    
    print(f"Loaded data: {len(results_data.get('results', []))} results, {len(athletes_data.get('athletes', {}))} athletes")
    
    # Record start time
    start_time = time.time()
    
    # Initialize complete data structure like in backup version
    data = {
        "athletes": athletes_data.get("athletes", {}),
        "events": results_data.get("events", {}),
        "results": results_data.get("results", []),
        "head_to_head": {},
        "athlete_elo": {},
        "elo_timeline": {},
        "metadata": {
            "date_analyzed": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "results_year_range": results_data.get("metadata", {}).get("year_range", ""),
            "athlete_count": len(athletes_data.get("athletes", {})),
            "event_count": len(results_data.get("events", {})),
            "result_count": len(results_data.get("results", [])),
            "head_to_head_count": 0,
        }
    }
    
    # Generate head-to-head statistics
    head_to_head = generate_head_to_head_stats(results_data, athletes_data)
    data["head_to_head"] = head_to_head
    data["metadata"]["head_to_head_count"] = len(head_to_head)
    
    # Set event importance values based on title
    for event_id, event in data["events"].items():
        event_importance = determine_event_importance(event.get("title", ""))
        event["importance"] = event_importance
    
    # Simplify program IDs in results - combine with event ID
    for result in data["results"]:
        # Ensure all results have an event ID
        if "event_id" not in result or result["event_id"] is None:
            continue
            
        # Remove split times if present
        if "swim_time" in result:
            result.pop("swim_time", None)
        if "bike_time" in result:
            result.pop("bike_time", None)
        if "run_time" in result:
            result.pop("run_time", None)
    
    # Calculate athlete ratings
    print("Calculating athlete ratings...")
    athlete_ratings = calculate_elo_ratings(results_data, athletes_data)
    data["athlete_elo"] = athlete_ratings
    
    # Generate ELO timeline data
    elo_timeline = generate_elo_timeline(athlete_ratings, athletes_data)
    data["elo_timeline"] = elo_timeline
    
    # Calculate processing time
    total_time = time.time() - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    data["metadata"]["processing_time"] = f"{minutes}m {seconds}s"
    
    # Print summary
    print("\n📊 Data Analysis Summary:")
    print(f"- Athletes: {data['metadata']['athlete_count']}")
    print(f"- Events: {data['metadata']['event_count']}")
    print(f"- Results: {data['metadata']['result_count']}")
    print(f"- Head-to-head pairs: {data['metadata']['head_to_head_count']}")
    
    # Print top athletes by rating
    if athlete_ratings:
        rating_list = [(athlete_id, rating_data["current"]) for athlete_id, rating_data in athlete_ratings.items()]
        sorted_ratings = sorted(rating_list, key=lambda x: x[1], reverse=True)
        
        print("\n🏆 Top Athletes by Rating:")
        for idx, (athlete_id, rating) in enumerate(sorted_ratings[:10], 1):
            athlete = athletes_data.get("athletes", {}).get(athlete_id, {})
            name = athlete.get("details", {}).get("full_name", f"Athlete {athlete_id}")
            print(f"{idx}. {name} - Rating: {rating:.1f}")
        
        # Display Glicko-2 history for top athletes
        print("\n📈 Glicko-2 Rating History for Top Athletes:")
        top_athlete_ids = [athlete_id for athlete_id, _ in sorted_ratings[:3]]  # Get top 3 athletes
        
        for athlete_id in top_athlete_ids:
            if athlete_id in elo_timeline:
                timeline = elo_timeline[athlete_id]
                print(f"\n{timeline['name']} Glicko-2 Rating Progression:")
                print(f"  Starting Rating: {timeline['initial_elo']:.1f}")
                
                # Display a sample of the timeline (first entry, some middle entries, and last entry)
                timeline_entries = timeline['timeline']
                
                if len(timeline_entries) > 0:
                    # First entry
                    print(f"  {timeline_entries[0]['date']}: {timeline_entries[0]['elo']:.1f}")
                    
                    # Sample middle entries (up to 5)
                    if len(timeline_entries) > 6:
                        step = max(1, len(timeline_entries) // 6)
                        for i in range(step, len(timeline_entries) - 1, step):
                            if i < len(timeline_entries):
                                print(f"  {timeline_entries[i]['date']}: {timeline_entries[i]['elo']:.1f}")
                    elif len(timeline_entries) > 2:
                        # If few entries, show middle one
                        mid_idx = len(timeline_entries) // 2
                        print(f"  {timeline_entries[mid_idx]['date']}: {timeline_entries[mid_idx]['elo']:.1f}")
                    
                    # Last entry
                    if len(timeline_entries) > 1:
                        print(f"  {timeline_entries[-1]['date']}: {timeline_entries[-1]['elo']:.1f}")
                
                print(f"  Final Rating: {timeline['final_elo']:.1f}")
                print(f"  Total Change: {timeline['final_elo'] - timeline['initial_elo']:.1f}")
    
    # Save analyzed data
    if output_file:
        save_data_to_json(data, output_file)
        print(f"✅ Analysis complete. Data saved to {output_file}")
    else:
        print(f"✅ Analysis complete. (No JSON output file)")
    
    # Upload to database if db_upload is enabled
    if db_upload:
        if use_supabase:
            if upload_data_to_supabase is None:
                print("Error: Supabase upload functionality is not available.")
            else:
                print("\nUploading data to Supabase...")
                try:
                    result = upload_data_to_supabase(
                        clear_existing=clear_existing, 
                        data=data
                    )
                    if result:
                        records_added, records_updated = result
                        print(f"Supabase upload completed successfully.")
                        print(f"Records processed: {records_added}")
                    else:
                        print("Supabase upload failed.")
                except Exception as e:
                    print(f"Error during Supabase upload: {e}")
        else:
            if upload_data_to_database is None:
                print("Error: Database upload functionality is not available.")
            else:
                print(f"\nUploading data to SQLite database {db_file}...")
                try:
                    # Upload data directly to database
                    result = upload_data_to_database(
                        db_file=db_file, 
                        clear_existing=clear_existing, 
                        data=data
                    )
                    if result:
                        records_added, records_updated = result
                        print(f"Database upload completed successfully.")
                        print(f"Records added: {records_added}, Records updated: {records_updated}")
                    else:
                        print("Database upload failed.")
                except Exception as e:
                    print(f"Error during database upload: {e}")
    
    print(f"\nAnalysis completed in {minutes}m {seconds}s")
    
    return data

def main():
    """Main function for the data analyzer script"""
    # Default values
    results_file = "results_data.json"
    athletes_file = "athletes_data.json"
    output_file = "analyzed_data.json"
    limit_athletes = None
    db_upload = True
    db_file = "triathlon.db"
    clear_existing = True
    
    # Process command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print("Usage: python data_analyzer.py [results_file] [athletes_file] [output_file] [--db] [db_file] [--clear] [--supabase]")
            print("  results_file: Path to JSON file with results data (default: results_data.json)")
            print("  athletes_file: Path to JSON file with athletes data (default: athletes_data.json)")
            print("  output_file: Path to save analyzed data to (default: analyzed_data.json)")
            print("  --db: Upload data directly to database")
            print("  db_file: Database file path (default: triathlon.db)")
            print("  --clear: Clear existing data in database before upload")
            print("  --supabase: Use Supabase instead of SQLite (requires .env file with credentials)")
            return
        results_file = sys.argv[1]
    
    if len(sys.argv) > 2:
        athletes_file = sys.argv[2]
    
    if len(sys.argv) > 3:
        output_file = sys.argv[3]
    
    # Check for database upload flag
    if "--db" in sys.argv:
        db_upload = True
        # If db_file is specified after --db flag
        db_index = sys.argv.index("--db")
        if db_index + 1 < len(sys.argv) and not sys.argv[db_index + 1].startswith("--"):
            db_file = sys.argv[db_index + 1]
    
    # Check for clear existing data flag
    if "--clear" in sys.argv:
        clear_existing = True
    
    # Check for empty output file (upload to database only)
    if output_file == "--db" or output_file == "--clear" or output_file == "--supabase":
        output_file = None
    
    # Analyze data
    analyze_data(
        results_file=results_file,
        athletes_file=athletes_file,
        output_file=output_file,
        limit_athletes=limit_athletes,
        db_upload=db_upload,
        db_file=db_file,
        clear_existing=clear_existing
    )

if __name__ == "__main__":
    main()