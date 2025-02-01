from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import logging
SCOPES = ["https://www.googleapis.com/auth/calendar"]


def authenticate_google_api(credentials_path):
    """Authenticate using a service account and return the Google Calendar service."""
    credentials = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    return build("calendar", "v3", credentials=credentials)


def get_calendar_name(service, calendar_id):
    """Fetch the name of the calendar using its ID."""
    calendar = service.calendars().get(calendarId=calendar_id).execute()
    return calendar.get("summary", "Unknown Calendar")


def get_events_from_calendar(service, calendar_id, time_min=None, time_max=None):
    """Fetch events from a specific Google Calendar."""
    logging.info(f"Fetching events from {calendar_id} between {time_min} and {time_max}...")
    events_result = (
        service.events()
        .list(
            calendarId=calendar_id,
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            orderBy="startTime",
        )
        .execute()
    )
    events = events_result.get("items", [])

    if not events:
        logging.warning(f"No events found for {calendar_id}. Check if API permissions are correct.")

    return events
