-- Create athletes table
CREATE TABLE IF NOT EXISTS public.athletes (
    athlete_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    full_name TEXT,
    nationality TEXT,
    gender TEXT,
    birth_year INTEGER
);

-- Create events table
CREATE TABLE IF NOT EXISTS public.events (
    event_id INTEGER PRIMARY KEY,
    title TEXT,
    date TEXT,
    importance INTEGER
);

-- Create results table
CREATE TABLE IF NOT EXISTS public.results (
    result_id SERIAL PRIMARY KEY,
    event_id INTEGER REFERENCES public.events(event_id),
    athlete_id INTEGER REFERENCES public.athletes(athlete_id),
    position INTEGER,
    total_time TEXT,
    points INTEGER,
    UNIQUE(athlete_id, event_id)
);

-- Create athlete ratings table with history
CREATE TABLE IF NOT EXISTS public.athlete_ratings (
    rating_id SERIAL PRIMARY KEY,
    athlete_id INTEGER REFERENCES public.athletes(athlete_id),
    rating_value REAL DEFAULT 1500,
    rating_date TEXT,
    races_completed INTEGER DEFAULT 0,
    event_id INTEGER REFERENCES public.events(event_id),
    UNIQUE(athlete_id, rating_date)
);

-- Create ELO timeline table for easier timeline queries
CREATE TABLE IF NOT EXISTS public.elo_timeline (
    timeline_id SERIAL PRIMARY KEY,
    athlete_id INTEGER REFERENCES public.athletes(athlete_id),
    date TEXT,
    elo_value REAL,
    UNIQUE(athlete_id, date)
);

-- Create head-to-head table for pairs of athletes
CREATE TABLE IF NOT EXISTS public.head_to_head (
    pair_id TEXT PRIMARY KEY,
    athlete1_id INTEGER REFERENCES public.athletes(athlete_id),
    athlete2_id INTEGER REFERENCES public.athletes(athlete_id),
    athlete1_name TEXT,
    athlete2_name TEXT,
    encounters INTEGER DEFAULT 0,
    athlete1_wins INTEGER DEFAULT 0,
    athlete2_wins INTEGER DEFAULT 0
);

-- Create head-to-head meetings table for individual meetings
CREATE TABLE IF NOT EXISTS public.head_to_head_meetings (
    meeting_id SERIAL PRIMARY KEY,
    pair_id TEXT REFERENCES public.head_to_head(pair_id),
    event_id INTEGER REFERENCES public.events(event_id),
    event_title TEXT,
    event_date TEXT,
    prog_id TEXT,
    winner_id INTEGER REFERENCES public.athletes(athlete_id),
    winner_position INTEGER,
    loser_id INTEGER REFERENCES public.athletes(athlete_id),
    loser_position INTEGER
);

-- Create metadata table
CREATE TABLE IF NOT EXISTS public.metadata (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_athletes_full_name ON public.athletes(full_name);
CREATE INDEX IF NOT EXISTS idx_events_date ON public.events(date);
CREATE INDEX IF NOT EXISTS idx_results_athlete_event ON public.results(athlete_id, event_id);
CREATE INDEX IF NOT EXISTS idx_athlete_ratings_athlete_date ON public.athlete_ratings(athlete_id, rating_date);
CREATE INDEX IF NOT EXISTS idx_elo_timeline_athlete_date ON public.elo_timeline(athlete_id, date);
CREATE INDEX IF NOT EXISTS idx_head_to_head_athletes ON public.head_to_head(athlete1_id, athlete2_id);
CREATE INDEX IF NOT EXISTS idx_meetings_pair ON public.head_to_head_meetings(pair_id);

-- Insert initial metadata
INSERT INTO public.metadata (key, value, updated_at)
VALUES 
    ('schema_version', '1.0', CURRENT_TIMESTAMP),
    ('created_at', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (key) 
DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at; 