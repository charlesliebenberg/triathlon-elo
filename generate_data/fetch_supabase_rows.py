import os
from generate_data.supabase_uploader import get_supabase_client


def fetch_first_rows(limit=100):
    """Fetch and print the first `limit` rows of each Supabase table."""
    tables = [
        "athletes",
        "events",
        "results",
        "athlete_ratings",
        "elo_timeline",
        "head_to_head",
        "head_to_head_meetings",
        "metadata",
    ]

    supabase = get_supabase_client()
    if not supabase:
        print("Failed to create Supabase client. Check your .env file.")
        return

    for table in tables:
        try:
            response = supabase.table(table).select('*').limit(limit).execute()
            print(f"\n=== {table} (showing up to {limit} rows) ===")
            for row in response.data:
                print(row)
        except Exception as e:
            print(f"Error fetching rows from {table}: {e}")


if __name__ == "__main__":
    limit = int(os.getenv("LIMIT", "100"))
    fetch_first_rows(limit)
