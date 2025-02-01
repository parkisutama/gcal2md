import datetime
import typer
from dateutil.relativedelta import relativedelta
from modules.config import CONFIG
from modules.activity_generator import MarkdownSyncGenerator
from modules.activity_updater import sync_journals
from modules.calendar_fetcher import fetch_and_process_events
from modules.logging import setup_logging

# Setup logging
setup_logging(CONFIG["LOG_DIR"], CONFIG["LOG_FILE"])

app = typer.Typer()

generator = MarkdownSyncGenerator(
    db_path=CONFIG["DB_FILE"],
    template_path=CONFIG["TEMPLATE_FILE"],
    output_dir=CONFIG["OUTPUT_ACTIVITIES_DIR"],
    frontmatter_columns={
        "title": "summary",
        "start-date": "start_date",
        "end-date": "end_date",
        "duration-minutes": "duration_minutes",
        "activity-block": "activity_block",
        "activity-category": "activity_category",
        "persona": "persona",
    },
)

def get_date_range(option: str):
    today = datetime.date.today()
    if option == "today":
        return today, today
    elif option == "week":
        start_of_week = today - datetime.timedelta(days=today.weekday())
        end_of_week = start_of_week + datetime.timedelta(days=6)
        return start_of_week, end_of_week
    elif option == "month":
        start_of_month = today.replace(day=1)
        end_of_month = start_of_month + relativedelta(months=1, days=-1)
        return start_of_month, end_of_month
    elif option == "year":
        start_of_year = today.replace(month=1, day=1)
        end_of_year = today.replace(month=12, day=31)
        return start_of_year, end_of_year
    else:
        raise typer.BadParameter("Invalid date range option.")

@app.command()
def sync(option: str = typer.Argument("today", help="Sync events and generate markdown for today, week, month, or year"), start: str = None, end: str = None):
    """Fetch events from Google Calendar, store them in the database, and generate/update Markdown files for a specified date range."""
    import logging
    
    if start and end:
        try:
            start_date = datetime.datetime.strptime(start, "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(end, "%Y-%m-%d").date()
            if start_date > end_date:
                logging.error("Error: Start date must be before end date.")
                raise typer.Exit(code=1)
        except ValueError:
            logging.error("Invalid date format. Use YYYY-MM-DD.")
            raise typer.Exit(code=1)
    else:
        start_date, end_date = get_date_range(option)
    
    logging.info(f"Fetching events from {start_date} to {end_date}...")
    fetch_and_process_events(start_date, end_date)
    logging.info("Events fetched and stored in the database.")
    logging.info("Generating Markdown files...")
    generator.generate(
        table_name="events",
        file_name_column="event_id",
        start_date=start_date,
        end_date=end_date,
    )
    logging.info("Updating existing Markdown files...")
    sync_journals(CONFIG["TEMPLATE_ACTIVITIES_BLOCK"], CONFIG["DB_FILE"], start_date, end_date)
    logging.info(
        "Sync completed: Events fetched, Markdown generated and updated for the specified date range."
    )

if __name__ == "__main__":
    app()
