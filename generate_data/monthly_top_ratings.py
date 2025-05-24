#!/usr/bin/env python3
"""Fetch the top athletes by ELO for each month from Supabase.

This script queries the `elo_timeline` table to determine each athlete's rating
at the end of every month and prints the top performers. It joins with the
`athletes` table to display the full name for readability.
"""

import calendar
from datetime import datetime
from collections import defaultdict

from supabase_uploader import get_supabase_client


def fetch_monthly_top_athletes(limit: int = 10):
    """Return a dictionary mapping 'YYYY-MM' to a list of top athletes."""
    supabase = get_supabase_client()
    if not supabase:
        print("Failed to create Supabase client. Check your .env file.")
        return {}

    # Fetch timeline data (athlete_id, date, elo_value)
    timeline_resp = (
        supabase.table("elo_timeline")
        .select("athlete_id,date,elo_value")
        .order("date")
        .execute()
    )
    timeline_rows = timeline_resp.data or []

    if not timeline_rows:
        print("No timeline data found.")
        return {}

    # Fetch athlete full names
    athletes_resp = supabase.table("athletes").select("athlete_id,full_name").execute()
    name_map = {row["athlete_id"]: row.get("full_name") for row in athletes_resp.data or []}

    # Organize timeline data by athlete and collect month keys
    timeline_by_athlete = defaultdict(list)
    month_keys = set()

    for row in timeline_rows:
        athlete_id = row.get("athlete_id")
        date_str = row.get("date")
        elo = row.get("elo_value")
        if not athlete_id or not date_str or elo is None:
            continue
        dt = datetime.fromisoformat(date_str)
        timeline_by_athlete[athlete_id].append((dt, elo))
        month_keys.add(dt.strftime("%Y-%m"))

    # Sort entries for each athlete chronologically
    for entries in timeline_by_athlete.values():
        entries.sort(key=lambda x: x[0])

    results_by_month = {}

    for month_key in sorted(month_keys):
        year, month = map(int, month_key.split("-"))
        last_day = calendar.monthrange(year, month)[1]
        last_date = datetime(year, month, last_day)

        monthly_results = []
        for athlete_id, entries in timeline_by_athlete.items():
            latest = None
            for dt, elo in entries:
                if dt <= last_date:
                    latest = (dt, elo)
                else:
                    break
            if latest:
                dt, elo = latest
                monthly_results.append(
                    {
                        "athlete_id": athlete_id,
                        "full_name": name_map.get(athlete_id, f"Athlete {athlete_id}"),
                        "elo": elo,
                        "date": dt.date().isoformat(),
                    }
                )

        monthly_results.sort(key=lambda x: x["elo"], reverse=True)
        results_by_month[month_key] = monthly_results[:limit]

    return results_by_month


def print_monthly_results(results):
    for month_key in sorted(results.keys()):
        year, month = month_key.split("-")
        month_name = calendar.month_name[int(month)]
        print(f"\n=== Top Athletes for {month_name} {year} ===")
        print(f"{'Rank':<5}{'Athlete ID':<12}{'Full Name':<30}{'ELO':<10}{'Date':<12}")
        print("-" * 70)
        for rank, entry in enumerate(results[month_key], 1):
            print(
                f"{rank:<5}{entry['athlete_id']:<12}{entry['full_name']:<30}{entry['elo']:<10.2f}{entry['date']:<12}"
            )


if __name__ == "__main__":
    data = fetch_monthly_top_athletes()
    if data:
        print_monthly_results(data)

