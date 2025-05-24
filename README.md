# Triathlon Elo Data Pipeline

This repository contains a set of scripts for collecting triathlon race data, generating athlete statistics and ratings, and uploading the results to a database.

## Repository Structure

- `generate_data/` – package with all data generation and upload scripts.
  - `__init__.py` – ensures the `data/` directory exists at import time.
  - `results_collector.py` – contacts the [World Triathlon API](https://api.triathlon.org/) to download event and results data. It supports asynchronous fetching with retry logic and can operate on a year range or a custom date span.
  - `athletes_collector.py` – uses previously collected results to fetch athlete details and compile yearly performance metrics. Both synchronous and asynchronous entry points are provided.
  - `data_analyzer.py` – computes various statistics from the collected data. It generates head‑to‑head summaries and ELO ratings using the `glicko2` module. Optional upload helpers are imported if available.
  - `glicko2.py` – implementation of the Glicko‑2 rating algorithm which this project uses as the basis for ELO style rankings.
  - `database_uploader.py` – utility for creating and populating a local SQLite database.
  - `supabase_uploader.py` – utilities for uploading data to Supabase. It relies on credentials in a `.env` file and the schema described in `supabase_tables.sql`.
  - `utils.py` – shared helpers for API requests, file I/O, and various small utilities.
  - `supabase_tables.sql` – SQL script with table definitions matching the fields used by the uploaders.
- `requirements.txt` – Python dependencies required to run the scripts.

All JSON output is written inside a `data/` directory at the project root. The directory is created automatically when any module is imported.

## Typical Workflow

1. **Collect event results**

   ```bash
   python generate_data/results_collector.py 2000 2023 results_data.json
   ```

   This downloads Elite Men results for the given years and stores them in `data/results_data.json`.

2. **Gather athlete details**

   ```bash
   python generate_data/athletes_collector.py results_data.json athletes_data.json
   ```

   Reads the previously generated results and fetches information for each athlete found.

3. **Analyze and upload**

    ```bash
    python generate_data/data_analyzer.py results_data.json athletes_data.json
    ```

    Calculates ratings and head‑to‑head stats. The analyzer can optionally upload data to SQLite or Supabase if configured.

## Data Structure and Output

Each stage of the pipeline writes its JSON files inside the `data/` folder. The
files are indented for readability and share a similar nested structure. Below
is a simplified overview of the main outputs.

### `results_data.json`

```json
{
  "events": { "<event_id>": { "title": "...", "date": "..." } },
  "results": [ { "event_id": 1, "athlete_id": 2, "position": 5 } ],
  "athlete_ids": [2, 3],
  "metadata": {
    "date_collected": "YYYY-MM-DD HH:MM:SS",
    "year_range": "2000-2023",
    "event_count": 123,
    "result_count": 456,
    "athlete_count": 78,
    "failed_events": 0
  }
}
```

### `athletes_data.json`

```json
{
  "athletes": {
    "2": {
      "name": "Jane Doe",
      "details": { "country": "USA", "year_of_birth": 1990 },
      "yearly_results": { "2023": [ { "event_id": 1, "position": 5 } ] },
      "performance_metrics": { "total_events": 1, "best_position": 5 }
    }
  },
  "metadata": {
    "date_collected": "YYYY-MM-DD HH:MM:SS",
    "athlete_count": 1
  }
}
```

### `analyzed_data.json`

```json
{
  "athletes": { "2": { ... } },
  "events": { "<event_id>": { ... } },
  "results": [ ... ],
  "head_to_head": {
    "2-3": {
      "encounters": 3,
      "athlete1_wins": 2,
      "athlete2_wins": 1,
      "meetings": [ { "event_id": 1, "winner_id": 2 } ]
    }
  },
  "athlete_elo": { "2": { "rating": 1500.0 } },
  "elo_timeline": { "2": { "timeline": [ { "date": "2023-01-01", "elo": 1500.0 } ] } },
  "metadata": {
    "date_analyzed": "YYYY-MM-DD HH:MM:SS",
    "head_to_head_count": 1
  }
}
```

These structures can be uploaded directly to SQLite or Supabase using the
provided utilities.

## Environment Setup

Install dependencies using pip:

```bash
pip install -r requirements.txt
```

For Supabase uploads, create a `.env` file with `SUPABASE_URL` and `SUPABASE_KEY` variables. The SQL schema in `generate_data/supabase_tables.sql` can be executed on a Supabase instance to create the required tables.

## License

The code is provided under the MIT license. See individual files for details where applicable.
