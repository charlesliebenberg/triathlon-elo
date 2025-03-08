# Triathlon Analysis Project

This project is designed to collect triathlon event data and athlete performance metrics from a remote API, analyze them (including generating Glicko2 ratings), and optionally store all of the processed data in a local SQLite database (`triathlon.db`). Below is a consolidated guide to each script's function, how they interconnect, and recommended best practices for querying and understanding the database schema.

## Table of Contents

1. [Project Overview](#project-overview)  
2. [Core Files](#core-files)  
3. [Running the Pipeline](#running-the-pipeline)  
4. [Commands & Usage](#commands--usage)  
5. [Code Style Guidelines](#code-style-guidelines)  
6. [Database Schema Overview](#database-schema-overview)  
7. [Querying triathlon.db](#querying-triathlondb)  
8. [Final Notes](#final-notes)  

## Project Overview

The Triathlon Analysis Project is composed of multiple Python modules that perform distinct tasks:

1. **Data Collection**: Gathering athlete metadata, race results, and event information from a remote API.  
2. **Data Processing**: Filtering, cleaning, and structuring the results for analysis.  
3. **Rating & Analytics**: Applying the Glicko2 rating system to measure athlete performance over time.  
4. **Database Storage**: Storing final data, including athlete details, events, results, and Glicko2 history, in `triathlon.db`.

Broadly, the project workflows look like this:

```
  ┌───────────────────┐
  │athletes_collector │    Collect or fetch
  │.py & results_     │ →  data from API
  │collector.py       │
  └───────────────────┘
            ↓
  ┌───────────────────┐
  │data_analyzer.py   │    Processes and
  │                   │ →  analyzes data
  └───────────────────┘
            ↓
  ┌───────────────────┐
  │database_uploader  │    Creates tables,
  │.py                │ →  stores data in DB
  └───────────────────┘
```

## Core Files

### 1. athletes_collector.py

Collects detailed athlete information, including personal details, from an external API.

**Key Functions**:
- `get_athlete_details_async` / `get_athlete_details`: Fetches an individual athlete's info.
- `process_athlete_data_async` / `process_athlete_data`: Combines athlete details with local results to produce a comprehensive per-athlete dataset.

**Interconnections**: Often used alongside preexisting `results_data.json` to cross-match athlete IDs with their performance data.

### 2. results_collector.py

Retrieves event and race results data.

**Key Functions**:
- `get_all_events_async`: Fetches lists of event metadata (name, date, etc.).
- `get_event_details_async`: Fetches detailed info (location, programs) for a single event.
- `get_event_results_async`: Fetches results (finishing positions, times) for a specific event program.

**Interconnections**: Outputs JSON data that can be loaded by `athletes_collector.py` and/or processed by `data_analyzer.py`.

### 3. data_analyzer.py

Coordinates the final data analysis steps, including:

- **Generating Head-to-Head Stats**: Compares results among all athletes who compete in the same events.
- **Applying Glicko2**: Recalculates athlete ratings across events in chronological order.
- **Output**: A consolidated JSON with final stats. It can also call `database_uploader.py` to upload data.

### 4. database_uploader.py

Handles the creation and structure of `triathlon.db` and uploads data into relevant tables.

**Key Functions**:
- `create_database_tables`: Creates or upgrades DB schemas for `athletes`, `events`, `results`, `head_to_head`, `elo_history`, and so on.
- `upload_data_to_database`: Orchestrates the insertion of athletes, events, results, head-to-head data, and rating info into their respective tables.

**Interconnections**: Typically invoked after `data_analyzer.py` has created an `analyzed_data.json`.

### 5. glicko2.py

Implements the Glicko-2 rating system (used to rate chess, similarly adapted for triathlon performance).

**Key Classes/Functions**:
- `Player`: Maintains rating, rating deviation, and volatility for an individual athlete.
- `update_player`: Updates an athlete's rating based on matchups.

**Interconnections**: Called from `data_analyzer.py` to calculate or update athlete ratings in a batch.

### 6. utils.py

Provides shared functionality such as:

- **API Key & Headers**: Shared constants and credentials.
- **File I/O**: `load_data_from_json`, `save_data_to_json`.
- **Network**: `make_api_request` for synchronous calls and placeholders for asynchronous requests.
- **Filtering**: Helper like `is_elite_men_program` to ensure we only track certain categories.

## Running the Pipeline

1. **Collect Results**
   ```bash
   python generate_data/results_collector.py
   ```
   Gathers event details and results data, storing them in JSON (e.g., `results_data.json`).

2. **Collect Athlete Data**
   ```bash
   python generate_data/athletes_collector.py [input_file] [output_file]
   ```
   Reads athlete IDs from `[input_file]` (or from existing data) and fetches detailed info, storing the results in `[output_file]`.

3. **Run Data Analysis**
   ```bash
   python generate_data/data_analyzer.py
   ```
   Loads athletes/results from JSON, runs head-to-head analyses, Glicko2 rating updates, etc.

4. **Upload to Database** (Optional)
   ```bash
   python generate_data/database_uploader.py
   ```
   Creates `triathlon.db` if it doesn't exist and populates it with all the processed data (athletes, events, results, etc.).

## Commands & Usage

Below are the primary commands used in this project (some require arguments):

- **Run data collection**:
  ```bash
  python generate_data/athletes_collector.py [input_file] [output_file]
  ```

- **Run data analysis**:
  ```bash
  python generate_data/data_analyzer.py
  ```

- **Upload to database**:
  ```bash
  python generate_data/database_uploader.py
  ```

These scripts assume the presence of a `data/` folder where JSON data is stored. If you wish to tweak the file paths, you can modify them in `utils.py`.

## Code Style Guidelines

When working on or extending any of these files, please adhere to the following conventions:

### Naming
- **Functions/variables**: `snake_case` (e.g., `fetch_event_data`)
- **Classes**: `PascalCase` (e.g., `DataAnalyzer`)
- **Constants**: `UPPER_CASE` (e.g., `API_KEY`)

### Docstrings
- Use **Google-style** docstrings for functions and classes.
- Always include parameter and return value descriptions.

### Imports
- Group imports in three sections:
  1. Standard library
  2. Third-party libraries
  3. Local modules

### Error Handling
- Catch **specific exception types** where possible.
- Include **retry logic** (especially for network requests).

### Type Hints
- Include them when parameter types or return values aren't obvious, or for complex data structures.

### Asynchronous Patterns
- Use `asyncio` and `aiohttp` where helpful (e.g., `athletes_collector.py` and `results_collector.py` have async variants for concurrency).

### Glicko2 Implementation
- Found in `glicko2.py`, follow the above naming & docstring practices if adding or modifying code.

## Database Schema Overview

When you run `python generate_data/database_uploader.py` or enable database uploading in `data_analyzer.py`, an SQLite database named `triathlon.db` is created (or updated) with the following tables:

### 1. `athletes`
- **athlete_id** (`INTEGER PRIMARY KEY`)
- **first_name** (`TEXT`)
- **last_name** (`TEXT`)
- **full_name** (`TEXT`)
- **nationality** (`TEXT`)
- **gender** (`TEXT`)
- **age** (`INTEGER`)
- **birth_date** (`TEXT`)

### 2. `events`
- **event_id** (`INTEGER PRIMARY KEY`)
- **title** (`TEXT`)
- **date** (`TEXT`)
- **location** (`TEXT`)
- **country** (`TEXT`)
- **event_type** (`TEXT`)
- **distance** (`TEXT`)
- **year** (`INTEGER`)
- **importance** (`INTEGER`)

### 3. `results`
- **result_id** (`INTEGER PRIMARY KEY AUTOINCREMENT`)
- **event_id** (`INTEGER`)
- **prog_id** (`INTEGER`)
- **athlete_id** (`INTEGER`)
- **position** (`INTEGER`)
- **swim_time** (`TEXT`)
- **bike_time** (`TEXT`)
- **run_time** (`TEXT`)
- **total_time** (`TEXT`)
- **points** (`INTEGER`)

### 4. `head_to_head`
- **pair_id** (`TEXT PRIMARY KEY`)
- **athlete1_id** (`INTEGER`)
- **athlete2_id** (`INTEGER`)
- **athlete1_name** (`TEXT`)
- **athlete2_name** (`TEXT`)
- **encounters** (`INTEGER`)
- **athlete1_wins** (`INTEGER`)
- **athlete2_wins** (`INTEGER`)

### 5. `head_to_head_meetings`
- **meeting_id** (`INTEGER PRIMARY KEY AUTOINCREMENT`)
- **pair_id** (`TEXT`)
- **event_id** (`INTEGER`)
- **event_title** (`TEXT`)
- **event_date** (`TEXT`)
- **prog_id** (`INTEGER`)
- **winner_id** (`INTEGER`)
- **winner_position** (`INTEGER`)
- **loser_id** (`INTEGER`)
- **loser_position** (`INTEGER`)

### 6. `athlete_elo`
- **athlete_elo_id** (`INTEGER PRIMARY KEY AUTOINCREMENT`)
- **athlete_id** (`INTEGER UNIQUE`)
- **initial_elo** (`REAL DEFAULT 1500`)
- **current_elo** (`REAL DEFAULT 1500`)
- **races_completed** (`INTEGER DEFAULT 0`)

### 7. `elo_history`
- **history_id** (`INTEGER PRIMARY KEY AUTOINCREMENT`)
- **athlete_id** (`INTEGER`)
- **event_id** (`INTEGER`)
- **event_date** (`TEXT`)
- **event_name** (`TEXT`)
- **event_importance** (`INTEGER`)
- **prog_id** (`INTEGER`)
- **position** (`INTEGER`)
- **status** (`TEXT`)
- **old_elo** (`REAL`)
- **new_elo** (`REAL`)
- **old_rd** (`REAL`)
- **new_rd** (`REAL`)
- **old_volatility** (`REAL`)
- **new_volatility** (`REAL`)
- **elo_change** (`REAL`)
- **opponents_faced** (`INTEGER`)

### 8. `metadata`
- **key** (`TEXT PRIMARY KEY`)
- **value** (`TEXT`)
- **updated_at** (`TEXT`)

Each table has **foreign key constraints** linking together the relevant columns (e.g., `results.athlete_id` references `athletes.athlete_id`), allowing for easy queries that join data across the schema.

## Querying triathlon.db

Below is a brief outline of how to query the database once it's populated:

### 1. Connect to triathlon.db

You can use Python's built-in sqlite3 library (or any SQLite client):

```python
import sqlite3

# Connect or create the DB if not present
conn = sqlite3.connect('triathlon.db')
cursor = conn.cursor()
```

### 2. Run Queries

```python
# Example: retrieve a list of all athletes
cursor.execute("SELECT athlete_id, full_name, gender FROM athletes")
rows = cursor.fetchall()

for row in rows:
    print(row)  # Each row is a tuple (athlete_id, full_name, gender)
```

You can similarly execute any valid SQL statement, for example:

```python
# Get top 10 best Glicko2-rated athletes
sql_query = """
SELECT a.athlete_id, a.full_name, e.current_elo 
FROM athletes a
JOIN athlete_elo e ON a.athlete_id = e.athlete_id
ORDER BY e.current_elo DESC
LIMIT 10
"""
cursor.execute(sql_query)
results = cursor.fetchall()
```

### 3. Viewing Head-to-Head Records

```python
# Example: see how many times athlete 100 beat athlete 200
cursor.execute("""
    SELECT athlete1_wins, athlete2_wins 
    FROM head_to_head
    WHERE (athlete1_id=100 AND athlete2_id=200) 
       OR (athlete1_id=200 AND athlete2_id=100)
""")
record = cursor.fetchone()
```

### 4. Cleanup

Always close the connection when finished:

```python
conn.close()
```

Note: You can also interact with `triathlon.db` directly via the SQLite CLI:

```bash
sqlite3 triathlon.db
sqlite> .tables
sqlite> SELECT * FROM athletes LIMIT 5;
sqlite> .exit
```

## Final Notes

- Keep your local environment's `API_KEY` safe—by default, it's stored in `utils.py`.
- The project assumes a typical triathlon data structure from the remote API. If you modify the event data schema, ensure you update the relevant scripts (especially `database_uploader.py` for the DB tables).
- For performance or concurrency concerns, you can integrate asynchronous code in your collectors (`athletes_collector.py`, `results_collector.py`) to handle many requests in parallel.

With these steps and guidelines, you should have a clear understanding of how each script ties into the overall pipeline—and how to run, customize, and query the resulting triathlon database.

Happy coding and triathlon analyzing!







You said:
Include a section about the database schema and the tables and their columns. Send me the full markdown as a raw text output

Reasoned about database schema and tables for 12 seconds
markdown
Copy
Edit
# Triathlon Analysis Project

This project is designed to collect triathlon event data and athlete performance metrics from a remote API, analyze them (including generating Glicko2 ratings), and optionally store all of the processed data in a local SQLite database (`triathlon.db`). Below is a consolidated guide to each script's function, how they interconnect, and recommended best practices for querying and understanding the database schema.

---

## Table of Contents

1. [Project Overview](#project-overview)  
2. [Core Files](#core-files)  
   1. [1. `athletes_collector.py`](#1-athletes_collectorpy)  
   2. [2. `results_collector.py`](#2-results_collectorpy)  
   3. [3. `data_analyzer.py`](#3-data_analyzerpy)  
   4. [4. `database_uploader.py`](#4-database_uploaderpy)  
   5. [5. `glicko2.py`](#5-glicko2py)  
   6. [6. `utils.py`](#6-utilspy)  
3. [Running the Pipeline](#running-the-pipeline)  
4. [Commands & Usage](#commands--usage)  
5. [Code Style Guidelines](#code-style-guidelines)  
6. [Database Schema Overview](#database-schema-overview)  
7. [Querying `triathlon.db`](#querying-triathlondb)  

---

## Project Overview

The Triathlon Analysis Project is composed of multiple Python modules that perform distinct tasks:

1. **Data Collection**: Gathering athlete metadata, race results, and event information from a remote API.  
2. **Data Processing**: Filtering, cleaning, and structuring the results for analysis.  
3. **Rating & Analytics**: Applying the [Glicko2 rating system](glicko2.py) to measure athlete performance over time.  
4. **Database Storage**: Storing final data, including athlete details, events, results, and Glicko2 history, in `triathlon.db`.

Broadly, the project workflows look like this:

┌───────────────────┐ │athletes_collector │ Collect or fetch │.py & results_ │ → data from API │collector.py │ └───────────────────┘ ↓ ┌───────────────────┐ │data_analyzer.py │ Processes and │ │ → analyzes data └───────────────────┘ ↓ ┌───────────────────┐ │database_uploader │ Creates tables, │.py │ → stores data in DB └───────────────────┘

yaml
Copy
Edit

---

## Core Files

### 1. `athletes_collector.py`

Collects detailed athlete information, including personal details, from an external API.

- **Key Functions**:  
  - `get_athlete_details_async` / `get_athlete_details`: Fetches an individual athlete's info.
  - `process_athlete_data_async` / `process_athlete_data`: Combines athlete details with local results to produce a comprehensive per-athlete dataset.

- **Interconnections**:  
  Often used alongside preexisting `results_data.json` to cross-match athlete IDs with their performance data.

---

### 2. `results_collector.py`

Retrieves event and race results data.

- **Key Functions**:  
  - `get_all_events_async`: Fetches lists of event metadata (name, date, etc.).
  - `get_event_details_async`: Fetches detailed info (location, programs) for a single event.
  - `get_event_results_async`: Fetches results (finishing positions, times) for a specific event program.

- **Interconnections**:  
  Outputs JSON data that can be loaded by `athletes_collector.py` and/or processed by `data_analyzer.py`.

---

### 3. `data_analyzer.py`

Coordinates final data analysis steps, including:

- **Generating Head-to-Head Stats**: Compares results among all athletes who compete in the same events.  
- **Applying Glicko2**: Recalculates athlete ratings across events in chronological order.
- **Output**: A consolidated JSON with final stats. It can also call `database_uploader.py` to upload data.

---

### 4. `database_uploader.py`

Handles the creation and structure of `triathlon.db` and uploads data into relevant tables.

- **Key Functions**:  
  - `create_database_tables`: Creates or upgrades DB schemas for `athletes`, `events`, `results`, `head_to_head`, `elo_history`, etc.  
  - `upload_data_to_database`: Orchestrates the insertion of athletes, events, results, head-to-head data, and rating info into their respective tables.

- **Interconnections**:  
  Typically invoked after `data_analyzer.py` has created an `analyzed_data.json`.

---

### 5. `glicko2.py`

Implements the Glicko-2 rating system (used to rate chess, similarly adapted for triathlon performance).

- **Key Classes/Functions**:  
  - `Player`: Maintains rating, rating deviation, and volatility for an individual athlete.
  - `update_player`: Updates an athlete's rating based on matchups.

- **Interconnections**:  
  Called from `data_analyzer.py` to calculate or update athlete ratings in a batch.

---

### 6. `utils.py`

Provides shared functionality such as:

- **API Key & Headers**: Shared constants and credentials.
- **File I/O**: `load_data_from_json`, `save_data_to_json`.
- **Network**: `make_api_request` for synchronous calls and placeholders for asynchronous requests.
- **Filtering**: Helper like `is_elite_men_program` to ensure we only track certain categories.

---

## Running the Pipeline

1. **Collect Results**  
python generate_data/results_collector.py

markdown
Copy
Edit
- Gathers event details and results data, storing them in JSON (e.g., `results_data.json`).

2. **Collect Athlete Data**  
python generate_data/athletes_collector.py [input_file] [output_file]

markdown
Copy
Edit
- Reads athlete IDs from `[input_file]` (or from existing data) and fetches detailed info, storing the results in `[output_file]`.

3. **Run Data Analysis**  
python generate_data/data_analyzer.py

pgsql
Copy
Edit
- Loads athletes/results from JSON, runs head-to-head analyses, Glicko2 rating updates, etc.

4. **Upload to Database** (Optional)  
python generate_data/database_uploader.py

yaml
Copy
Edit
- Creates `triathlon.db` if it doesn't exist and populates it with all the processed data (athletes, events, results, etc.).

---

## Commands & Usage

- **Run data collection**:  
python generate_data/athletes_collector.py [input_file] [output_file]

markdown
Copy
Edit

- **Run data analysis**:  
python generate_data/data_analyzer.py

markdown
Copy
Edit

- **Upload to database**:  
python generate_data/database_uploader.py

markdown
Copy
Edit

---

## Code Style Guidelines

1. **Naming**  
 - **Functions/variables**: `snake_case` (e.g., `fetch_event_data`)
 - **Classes**: `PascalCase` (e.g., `DataAnalyzer`)
 - **Constants**: `UPPER_CASE` (e.g., `API_KEY`)

2. **Docstrings**  
 - Use **Google-style** docstrings for functions and classes.
 - Always include parameter and return value descriptions.

3. **Imports**  
 - Group imports in three sections:
   1. Standard library
   2. Third-party libraries
   3. Local modules

4. **Error Handling**  
 - Catch **specific exception types** where possible.
 - Include **retry logic** (especially for network requests).

5. **Type Hints**  
 - Include them when parameter types or return values aren't obvious, or for complex data structures.

6. **Asynchronous Patterns**  
 - Use `asyncio` and `aiohttp` where helpful (e.g., `athletes_collector.py` and `results_collector.py` have async variants).

7. **Glicko2 Implementation**  
 - Found in `glicko2.py`, follow these naming & docstring practices if adding or modifying code.

---

## Database Schema Overview

When you run `python generate_data/database_uploader.py` or enable database uploading in `data_analyzer.py`, an SQLite database named `triathlon.db` is created (or updated) with the following tables:

1. **`athletes`**  
 - **athlete_id** (`INTEGER PRIMARY KEY`)  
 - **first_name** (`TEXT`)  
 - **last_name** (`TEXT`)  
 - **full_name** (`TEXT`)  
 - **nationality** (`TEXT`)  
 - **gender** (`TEXT`)  
 - **age** (`INTEGER`)  
 - **birth_date** (`TEXT`)  

2. **`events`**  
 - **event_id** (`INTEGER PRIMARY KEY`)  
 - **title** (`TEXT`)  
 - **date** (`TEXT`)  
 - **location** (`TEXT`)  
 - **country** (`TEXT`)  
 - **event_type** (`TEXT`)  
 - **distance** (`TEXT`)  
 - **year** (`INTEGER`)  
 - **importance** (`INTEGER`)  

3. **`results`**  
 - **result_id** (`INTEGER PRIMARY KEY AUTOINCREMENT`)  
 - **event_id** (`INTEGER`)  
 - **prog_id** (`INTEGER`)  
 - **athlete_id** (`INTEGER`)  
 - **position** (`INTEGER`)  
 - **swim_time** (`TEXT`)  
 - **bike_time** (`TEXT`)  
 - **run_time** (`TEXT`)  
 - **total_time** (`TEXT`)  
 - **points** (`INTEGER`)  

4. **`head_to_head`**  
 - **pair_id** (`TEXT PRIMARY KEY`)  
 - **athlete1_id** (`INTEGER`)  
 - **athlete2_id** (`INTEGER`)  
 - **athlete1_name** (`TEXT`)  
 - **athlete2_name** (`TEXT`)  
 - **encounters** (`INTEGER`)  
 - **athlete1_wins** (`INTEGER`)  
 - **athlete2_wins** (`INTEGER`)  

5. **`head_to_head_meetings`**  
 - **meeting_id** (`INTEGER PRIMARY KEY AUTOINCREMENT`)  
 - **pair_id** (`TEXT`)  
 - **event_id** (`INTEGER`)  
 - **event_title** (`TEXT`)  
 - **event_date** (`TEXT`)  
 - **prog_id** (`INTEGER`)  
 - **winner_id** (`INTEGER`)  
 - **winner_position** (`INTEGER`)  
 - **loser_id** (`INTEGER`)  
 - **loser_position** (`INTEGER`)  

6. **`athlete_elo`**  
 - **athlete_elo_id** (`INTEGER PRIMARY KEY AUTOINCREMENT`)  
 - **athlete_id** (`INTEGER UNIQUE`)  
 - **initial_elo** (`REAL DEFAULT 1500`)  
 - **current_elo** (`REAL DEFAULT 1500`)  
 - **races_completed** (`INTEGER DEFAULT 0`)  

7. **`elo_history`**  
 - **history_id** (`INTEGER PRIMARY KEY AUTOINCREMENT`)  
 - **athlete_id** (`INTEGER`)  
 - **event_id** (`INTEGER`)  
 - **event_date** (`TEXT`)  
 - **event_name** (`TEXT`)  
 - **event_importance** (`INTEGER`)  
 - **prog_id** (`INTEGER`)  
 - **position** (`INTEGER`)  
 - **status** (`TEXT`)  
 - **old_elo** (`REAL`)  
 - **new_elo** (`REAL`)  
 - **old_rd** (`REAL`)  
 - **new_rd** (`REAL`)  
 - **old_volatility** (`REAL`)  
 - **new_volatility** (`REAL`)  
 - **elo_change** (`REAL`)  
 - **opponents_faced** (`INTEGER`)  

8. **`metadata`**  
 - **key** (`TEXT PRIMARY KEY`)  
 - **value** (`TEXT`)  
 - **updated_at** (`TEXT`)  

Each table has **foreign key constraints** linking together the relevant columns (e.g., `results.athlete_id` references `athletes.athlete_id`), allowing for easy queries that join data across the schema.

---

## Querying `triathlon.db`

Below are examples demonstrating how to query the newly created database.

### 1. Connect to `triathlon.db`

```python
import sqlite3

# Connect or create the DB if not present
conn = sqlite3.connect('triathlon.db')
cursor = conn.cursor()
```

### 2. Run Queries

```python
# Example: retrieve a list of all athletes
cursor.execute("SELECT athlete_id, full_name, gender FROM athletes")
rows = cursor.fetchall()

for row in rows:
    print(row)  # Each row is a tuple (athlete_id, full_name, gender)
```

You can similarly execute any valid SQL statement, for example:

```python
# Get top 10 best Glicko2-rated athletes
sql_query = """
SELECT a.athlete_id, a.full_name, e.current_elo 
FROM athletes a
JOIN athlete_elo e ON a.athlete_id = e.athlete_id
ORDER BY e.current_elo DESC
LIMIT 10
"""
cursor.execute(sql_query)
results = cursor.fetchall()
```

### 3. Viewing Head-to-Head Records

```python
# Example: see how many times athlete 100 beat athlete 200
cursor.execute("""
    SELECT athlete1_wins, athlete2_wins 
    FROM head_to_head
    WHERE (athlete1_id=100 AND athlete2_id=200) 
       OR (athlete1_id=200 AND athlete2_id=100)
""")
record = cursor.fetchone()
```

### 4. Cleanup

Always close the connection when finished:

```python
conn.close()
```

Note: You can also interact with `triathlon.db` directly via the SQLite CLI:

```bash
sqlite3 triathlon.db
sqlite> .tables
sqlite> SELECT * FROM athletes LIMIT 5;
sqlite> .exit
```

## Final Notes

- Keep your local environment's `API_KEY` safe—by default, it's stored in `utils.py`.
- The project assumes a typical triathlon data structure from the remote API. If you modify the event data schema, ensure you update the relevant scripts (especially `database_uploader.py` for the DB tables).
- For performance or concurrency concerns, you can integrate asynchronous code in your collectors (`athletes_collector.py`, `results_collector.py`) to handle many requests in parallel.

With these steps and guidelines, you should have a clear understanding of how each script ties into the overall pipeline—and how to run, customize, and query the resulting triathlon database.

Happy coding and triathlon analyzing!