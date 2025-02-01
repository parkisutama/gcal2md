import os
import sqlite3

import pandas as pd

import datetime
import tzlocal

# Database setup
DB_NAME = "events.sqlite"


def create_table():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        calendar_name TEXT,
        calendar_id TEXT,
        event_id TEXT UNIQUE,
        summary TEXT,
        description TEXT,
        start TEXT,
        end TEXT,
        duration_minutes INTEGER,
        duration_hours REAL,
        location TEXT,
        activity_block TEXT,
        activity_category TEXT,
        persona TEXT
    );
    """
    )
    conn.commit()
    conn.close()


def import_csv_to_sqlite(csv_path):
    if not os.path.exists(DB_NAME):
        create_table()

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='events';"
    )
    if not cursor.fetchone():
        create_table()

    df = pd.read_csv(csv_path)

    for _, row in df.iterrows():
        conn.execute(
            """
        INSERT INTO events (
            calendar_name, calendar_id, event_id, summary, description,
            start, end, duration_minutes, duration_hours, location,
            activity_block, activity_category
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            calendar_name=excluded.calendar_name,
            calendar_id=excluded.calendar_id,
            summary=excluded.summary,
            description=excluded.description,
            start=excluded.start,
            end=excluded.end,
            duration_minutes=excluded.duration_minutes,
            duration_hours=excluded.duration_hours,
            location=excluded.location,
            activity_block=excluded.activity_block,
            activity_category=excluded.activity_category
        """,
            tuple(row),
        )

    conn.commit()
    conn.close()


def export_sqlite_to_csv(output_csv):
    conn = sqlite3.connect(DB_NAME)
    query = "SELECT * FROM events"
    df = pd.read_sql_query(query, conn)
    df.to_csv(output_csv, index=False)
    conn.close()


def view_data():
    conn = sqlite3.connect(DB_NAME)
    query = "SELECT * FROM events"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def update_data(df, table_name="events"):
    """
    Update the SQLite database with the modified DataFrame.
    """
    conn = sqlite3.connect(DB_NAME)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()


def read_calendar_ids(file_path):
    """Read calendar IDs from a CSV file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Calendar ID file '{file_path}' not found.")
    df = pd.read_csv(file_path)
    if "calendar_id" not in df.columns:
        raise ValueError("CSV file must contain a 'calendar_id' column.")
    return df["calendar_id"].tolist()



def get_system_timezone_offset():
    """
    Detects the system's local timezone and returns the UTC offset in +HH:MM format.
    """
    # Get the system's local timezone
    local_tz = tzlocal.get_localzone()
    timezone_name = local_tz
    
    # Get the current UTC offset in seconds
    now = datetime.datetime.now(local_tz)
    utc_offset_seconds = now.utcoffset().total_seconds()

    # Convert to +HH:MM format
    hours, remainder = divmod(abs(int(utc_offset_seconds)), 3600)
    minutes = remainder // 60
    sign = "+" if utc_offset_seconds >= 0 else "-"

    utc_offset = f"{sign}{hours:02}:{minutes:02}"

    return timezone_name, utc_offset
