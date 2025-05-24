import calendar
from collections import defaultdict
from datetime import datetime

from generate_data.supabase_uploader import get_supabase_client


def fetch_monthly_top_athletes(start_year: int = 2000, end_year: int = 2025, limit: int = 10):
    """Fetch top athletes for each month between the given years."""
    supabase = get_supabase_client()
    if not supabase:
        print("Failed to connect to Supabase")
        return {}

    try:
        timeline_resp = (
            supabase.table("elo_timeline")
            .select("athlete_id,date,elo_value")
            .gte("date", f"{start_year}-01-01")
            .lte("date", f"{end_year}-12-31")
            .execute()
        )
        athletes_resp = supabase.table("athletes").select("athlete_id,full_name").execute()
    except Exception as exc:
        print(f"Error fetching data: {exc}")
        return {}

    athlete_names = {row["athlete_id"]: row["full_name"] for row in athletes_resp.data}

    athlete_events = defaultdict(list)
    for row in timeline_resp.data:
        athlete_events[row["athlete_id"]].append((row["date"], row["elo_value"]))

    for events in athlete_events.values():
        events.sort(key=lambda x: x[0])

    monthly_results = {}
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            last_day = calendar.monthrange(year, month)[1]
            last_date = f"{year}-{month:02d}-{last_day}"
            month_key = f"{year}-{month:02d}"

            monthly_ratings = []
            for athlete_id, events in athlete_events.items():
                rating = None
                last_seen_date = None
                for date_str, elo in events:
                    if date_str <= last_date:
                        rating = elo
                        last_seen_date = date_str
                    else:
                        break
                if rating is not None:
                    monthly_ratings.append(
                        (athlete_id, athlete_names.get(athlete_id, "Unknown"), rating, last_seen_date)
                    )

            if monthly_ratings:
                monthly_ratings.sort(key=lambda x: x[2], reverse=True)
                monthly_results[month_key] = monthly_ratings[:limit]

    return monthly_results


def print_monthly_results(results):
    """Pretty-print the monthly top athletes."""
    for month in sorted(results.keys()):
        print(f"\n=== {month} ===")
        for rank, (athlete_id, name, elo, date) in enumerate(results[month], start=1):
            print(f"{rank:2d}. {name} ({athlete_id}) - {elo:.2f} (as of {date})")


if __name__ == "__main__":
    results = fetch_monthly_top_athletes()
    print_monthly_results(results)
