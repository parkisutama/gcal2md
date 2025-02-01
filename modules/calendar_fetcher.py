import logging
import sqlite3

import pandas as pd
import pytz.exceptions
from pytz import UTC, timezone

from modules.config import CONFIG
from modules.google_calendar import (
    authenticate_google_api,
    get_calendar_name,
    get_events_from_calendar,
)
from modules.utills import read_calendar_ids, get_system_timezone_offset

def fetch_and_process_events(start_date, end_date):
    """
    Fetch and process Google Calendar events within the specified date range.
    """
    timezone_name, utc_offset = get_system_timezone_offset()
    
    time_min = f"{start_date}T00:00:00{utc_offset}"
    time_max = f"{end_date}T23:59:59{utc_offset}"

    logging.info(f"Fetching events from {time_min} to {time_max}...")

    try:
        logging.info("Authenticating with Google Calendar API...")
        service = authenticate_google_api(CONFIG["service_account_path"])
    except Exception as e:
        logging.error(f"Failed to authenticate with Google Calendar API: {e}")
        return []

    try:
        calendar_ids = read_calendar_ids(CONFIG["calendars_csv_path"])
        if not calendar_ids:
            logging.warning("No calendar IDs found. Ensure the CSV file is correctly formatted.")
            return []
    except Exception as e:
        logging.error(f"Failed to read calendar IDs: {e}")
        return []

    all_processed_events = []

    for calendar_id in calendar_ids:
        try:
            calendar_name = get_calendar_name(service, calendar_id)
            logging.info(f"Fetching events from calendar: {calendar_name} ({calendar_id})")
            events = get_events_from_calendar(service, calendar_id, time_min, time_max)

            if not events:
                logging.warning(f"No events found for calendar: {calendar_name} ({calendar_id})")
                continue

            logging.info(f"Fetched {len(events)} events from {calendar_name} ({calendar_id})")

            processed_events = process_events(events, calendar_name, calendar_id)
        except Exception as e:
            logging.error(f"Error processing events for {calendar_name} ({calendar_id}): {e}")
            continue

        try:
            save_events_to_db(processed_events, CONFIG["DB_FILE"])
            logging.info(f"Successfully saved {len(processed_events)} events to database.")
        except Exception as e:
            logging.error(f"Database insertion failed for {calendar_name}: {e}")
            continue

        all_processed_events.extend(processed_events)

    logging.info(f"Total events processed: {len(all_processed_events)}")
    
    if not all_processed_events:
        logging.warning("No events were processed. Check API response or database connection.")
    
    return all_processed_events


def get_utc_offset(dt):
    """
    Calculate the UTC offset from a timezone-aware datetime object.
    """
    if dt.tzinfo:
        offset = dt.utcoffset()
        if offset:
            # Format offset as +HH:MM or -HH:MM
            total_seconds = offset.total_seconds()
            hours, remainder = divmod(abs(int(total_seconds)), 3600)
            minutes = remainder // 60
            sign = "+" if total_seconds >= 0 else "-"
            return f"{sign}{hours:02}:{minutes:02}"
    return "+00:00"  # Default to UTC offset if no timezone is provided


def process_events(events, calendar_name, calendar_id):
    """
    Process events by analyzing their details, including timezone (IANA format) and UTC offset.
    """
    processed_events = []
    for event in events:
        # Parse raw start and end times
        start_raw = event["start"].get("dateTime", event["start"].get("date", ""))
        end_raw = event["end"].get("dateTime", event["end"].get("date", ""))
        start_dt = pd.to_datetime(start_raw, errors="coerce")
        end_dt = pd.to_datetime(end_raw, errors="coerce")

        if start_dt is None or end_dt is None:
            logging.error("Invalid date format in event data.")
            continue

        # Determine IANA Timezone
        iana_timezone = (
            event.get("originalStartTime", {}).get(
                "timeZone"
            )  # Check originalStartTime.timeZone
            or event["start"].get("timeZone")  # Fallback to start.timeZone
            or "UTC"  # Default to UTC
        )

        try:
            tz = timezone(iana_timezone)
            start_dt = start_dt.tz_localize(tz) if start_dt.tzinfo is None else start_dt
            end_dt = end_dt.tz_localize(tz) if end_dt.tzinfo is None else end_dt
        except pytz.exceptions.UnknownTimeZoneError:
            logging.error(f"Unknown timezone: {iana_timezone}. Defaulting to UTC.")
            start_dt = start_dt.tz_localize(UTC)
            end_dt = end_dt.tz_localize(UTC)

        # Calculate UTC Offset (Offsite)
        start_offset = get_utc_offset(start_dt)

        # Build the event data
        event_data = {
            "Calendar Name": calendar_name,
            "Calendar ID": calendar_id,
            "Event ID": event.get("id", ""),
            "Summary": event.get("summary", ""),
            "Description": event.get("description", ""),
            "Start": start_dt.isoformat(),
            "End": end_dt.isoformat(),
            "Start Date": start_dt.strftime("%Y-%m-%dT%H:%M"),
            "End Date": end_dt.strftime("%Y-%m-%dT%H:%M"),
            "Duration_Minutes": calculate_duration(event, unit="minutes"),
            "Duration_Hours": calculate_duration(event, unit="hours"),
            "Timezone": iana_timezone,  # Use IANA timezone name directly
            "Offsite": start_offset,  # Store the start's timezone offset
            "Location": event.get("location", ""),
        }
        processed_events.append(event_data)
    return processed_events


def calculate_duration(event, unit="minutes"):
    """
    Calculate the duration of an event in minutes or hours.
    """
    start = pd.to_datetime(
        event["start"].get("dateTime", event["start"].get("date", ""))
    )
    end = pd.to_datetime(event["end"].get("dateTime", event["end"].get("date", "")))
    duration = (end - start).total_seconds()
    return duration / 60 if unit == "minutes" else duration / 3600


def save_events_to_db(events, database_path):
    """
    Save events to an SQLite database. Update events if they already exist.
    """
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()
    
    logging.info(f"Saving {len(events)} events to the database...")
    
    # Create table with Event_ID as the primary key
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT PRIMARY KEY,
            calendar_name TEXT,
            calendar_id TEXT,
            summary TEXT,
            description TEXT,
            start TEXT,
            end TEXT,
            start_date TEXT,
            end_date TEXT,
            duration_minutes REAL,
            duration_hours REAL,
            timezone TEXT,
            offsite TEXT,
            location TEXT
        )
        """
    )

    # Insert or replace events into the table
    for event in events:
        logging.info(f"Inserting event {event['Event ID']} - {event['Summary']}")
        cursor.execute(
            """
            INSERT OR REPLACE INTO events (
                event_id, calendar_name, calendar_id, summary, description, start, end,
                start_date, end_date, duration_minutes, duration_hours, timezone, offsite, location
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["Event ID"],
                event["Calendar Name"],
                event["Calendar ID"],
                event["Summary"],
                event["Description"],
                event["Start"],
                event["End"],
                event["Start Date"],
                event["End Date"],
                event["Duration_Minutes"],
                event["Duration_Hours"],
                event["Timezone"],
                event["Offsite"],  # Store offset as a string
                event["Location"],
            ),
        )

    conn.commit()
    conn.close()
    logging.info(f"Events synchronized to database at {database_path}")
