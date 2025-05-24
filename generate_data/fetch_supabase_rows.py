import os
from collections import defaultdict

from supabase_uploader import get_supabase_client


def fetch_monthly_top_athletes(limit=10):
    """Fetch the top athletes by ELO at the end of each month."""
    supabase = get_supabase_client()
    if not supabase:
        print("Failed to create Supabase client. Check your .env file.")
        return

    try:
        timeline_resp = (
            supabase.table("elo_timeline")
            .select("athlete_id,date,elo_value")
            .order("date")
            .execute()
        )
        athlete_resp = (
            supabase.table("athletes")
            .select("athlete_id,full_name")
            .execute()
        )
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    if not timeline_resp.data:
        print("No timeline data available")
        return

    # Map athlete_id to full name
    names = {row["athlete_id"]: row.get("full_name") for row in athlete_resp.data}

    # Build latest entry for each athlete in each month
    monthly_data = defaultdict(lambda: {})
    for row in timeline_resp.data:
        athlete_id = row["athlete_id"]
        date_str = row["date"]
        month = date_str[:7]  # YYYY-MM

        cur = monthly_data[month].get(athlete_id)
        if cur is None or date_str > cur["date"]:
            monthly_data[month][athlete_id] = row

    # Sort months chronologically
    for month in sorted(monthly_data.keys()):
        entries = list(monthly_data[month].values())
        top = sorted(entries, key=lambda r: r["elo_value"], reverse=True)[:limit]
        print(f"\n=== Top {limit} as of {month} ===")
        for rank, row in enumerate(top, 1):
            name = names.get(row["athlete_id"], f"Athlete {row['athlete_id']}")
            elo = row["elo_value"]
            date = row["date"]
            print(f"{rank:2}. {name} (ID {row['athlete_id']}) - {elo} on {date}")


if __name__ == "__main__":
    limit = int(os.getenv("LIMIT", "10"))
    fetch_monthly_top_athletes(limit)
