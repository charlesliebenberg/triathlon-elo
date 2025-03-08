#!/usr/bin/env python3
"""
Script to find the top 10 athletes by rating for each month in the triathlon database.
This script implements a SQL-based approach to find the highest rating for each athlete
as of the last day of each month in the database.
"""

import sqlite3
import calendar
from datetime import datetime

def get_monthly_top_ratings(db_path='data/triathlon.db'):
    """
    Query the database to find the top 10 athletes by rating for each month.
    Uses a SQL-based approach to find the most recent rating as of the end of each month.
    Returns a dictionary with keys in the format 'YYYY-MM' and values as lists of tuples
    containing athlete information and ratings.
    
    Args:
        db_path (str): Path to the SQLite database file
        
    Returns:
        dict: A dictionary where each key is 'YYYY-MM' and each value is a list of tuples:
              (athlete_id, full_name, event_date, new_elo, event_name)
    """
    # Connect to the database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Phase 1: Generate a list of all months
    cursor.execute("""
        SELECT DISTINCT strftime('%Y-%m', event_date) AS month
        FROM elo_history
        ORDER BY month
    """)
    months = [row[0] for row in cursor.fetchall()]
    
    results_by_month = {}
    
    # Phase 2 & 3: For each month, find athletes' "as-of" ratings and rank them
    for month in months:
        year, month_num = map(int, month.split('-'))
        
        # Get the last day of the month
        last_day = calendar.monthrange(year, month_num)[1]
        last_day_of_month = f"{year}-{month_num:02d}-{last_day}"
        
        # Get the most recent rating for each athlete as of the last day of the month
        cursor.execute("""
            WITH last_competition AS (
                SELECT e1.athlete_id,
                       MAX(e1.event_date) AS max_date
                FROM elo_history e1
                WHERE e1.event_date <= ?
                GROUP BY e1.athlete_id
            )
            SELECT e2.athlete_id,
                   a.full_name,
                   e2.event_date,
                   e2.new_elo,
                   e2.event_name,
                   a.profile_image_url
            FROM elo_history e2
            JOIN last_competition lc
                ON lc.athlete_id = e2.athlete_id
               AND lc.max_date = e2.event_date
            JOIN athletes a
                ON a.athlete_id = e2.athlete_id
            ORDER BY e2.new_elo DESC
            LIMIT 10
        """, (last_day_of_month,))
        
        top_10 = cursor.fetchall()  # (athlete_id, full_name, event_date, new_elo, event_name)
        results_by_month[month] = top_10
    
    conn.close()
    return results_by_month

def print_monthly_ratings(monthly_ratings):
    """
    Print the top 10 ratings for each month in the dataset.
    """
    # Sort by year and month
    sorted_months = sorted(monthly_ratings.keys())
    
    for month_key in sorted_months:
        year, month_num = month_key.split("-")
        month_name = calendar.month_name[int(month_num)]
        print(f"\n=== Top 10 Athletes for {month_name} {year} ===")
        print(f"{'Rank':<5}{'Athlete ID':<12}{'Name':<30}{'Rating':<10}{'Last Race':<50}{'Date':<12}")
        print("-" * 110)
        
        for rank, athlete_data in enumerate(monthly_ratings[month_key], 1):
            athlete_id, name, date, rating, event, profile_image = athlete_data
            print(f"{rank:<5}{athlete_id:<12}{name:<30}{rating:<10.2f}{event:<50}{date:<12}")

if __name__ == "__main__":
    import os
    
    # Get database path from environment variable or use default
    db_path = os.environ.get('TRIATHLON_DB_PATH', 'data/triathlon.db')
    
    print(f"Fetching top ratings for each month from the triathlon database at {db_path}...")
    monthly_ratings = get_monthly_top_ratings(db_path)
    print_monthly_ratings(monthly_ratings)
    print("\nAnalysis complete.")