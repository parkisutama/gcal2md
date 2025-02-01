import os
import re
import sqlite3
from datetime import datetime

from modules.config import CONFIG

import yaml
from jinja2 import Environment




#  Load template
def load_template(template_activities_block):
    with open(template_activities_block, "r") as file:
        template_content = file.read()

    # Create an environment with block trimming enabled
    env = Environment(
        trim_blocks=True,  # Remove newlines after blocks
        lstrip_blocks=True,  # Strip leading whitespace
    )

    # Create a template from the file content
    return env.from_string(template_content)


# Fetch events from the database
def fetch_events(db_file, date):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    query = """
    SELECT event_id, start_date, end_date, summary
    FROM events
    WHERE DATE(start_date) = ?
    ORDER BY start_date;
    """

    cursor.execute(query, (date,))
    rows = cursor.fetchall()
    conn.close()
    return rows


# Safely extract and parse frontmatter
def extract_frontmatter(content):
    match = re.match(r"---(.*?)---", content, re.DOTALL)
    if match:
        frontmatter = match.group(1).strip()
        return frontmatter, content[match.end() :].strip()
    return None, content


# Update or generate the journal file
def update_journal(template, date, events):
    output_path = os.path.join(CONFIG["OUTPUT_JOURNALS_DIR"], date[:4], date[:7], f"{date}.md")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Read the existing file if it exists
    if os.path.exists(output_path):
        with open(output_path, "r") as file:
            content = file.read()

        # Extract and parse frontmatter
        frontmatter, body = extract_frontmatter(content)
        if frontmatter:
            try:
                frontmatter_data = yaml.safe_load(frontmatter)
            except yaml.YAMLError as e:
                print(f"Error parsing YAML frontmatter: {e}")
                return
        else:
            frontmatter_data = {}

        # Check if `## Activities` exists
        activities_start = body.find("## Activities")
        if activities_start == -1:
            print(f"Error: File '{output_path}' must contain '## Activities' section.")
            return

        # Isolate content before `## Activities`
        before_activities = body[: activities_start + len("## Activities\n")]

        # Chek for Goal and preserve content after it
        goal_start = body.find("## Goal")
        after_goal = body[goal_start:] if goal_start != -1 else ""

        # Check for `Kanban Setting` and preserve content after it
        config_start = body.find("%% kanban:settings")
        after_config = body[config_start:] if config_start != -1 else ""

        # Render new activities
        new_activities = template.render(
            date=date,
            events=[
                {
                    "start_time": datetime.fromisoformat(event[1]).strftime("%H:%M"),
                    "end_time": datetime.fromisoformat(event[2]).strftime("%H:%M"),
                    "event_summary": event[3],
                    "event_id": event[0],
                }
                for event in events
            ],
        )

        # Combine the updated frontmatter with the clean body
        updated_content = (
            "---\n"
            + yaml.dump(frontmatter_data, default_flow_style=False)
            + "---\n"
            + before_activities
            + new_activities
            + "\n"
            + after_goal
        )
    else:
        # Create a new file
        formatted_date = datetime.strptime(date, "%Y-%m-%d").strftime("%A, %d %B %Y")
        updated_content = template.render(
            date=date,
            formatted_date=formatted_date,
            events=[
                {
                    "start_time": datetime.fromisoformat(event[1]).strftime("%H:%M"),
                    "end_time": datetime.fromisoformat(event[2]).strftime("%H:%M"),
                    "event_summary": event[3],
                    "event_id": event[0],
                }
                for event in events
            ],
        )

    # Write the updated content back
    with open(output_path, "w") as file:
        file.write(updated_content)

    print(f"Updated journal for {date} at {output_path}")


# Sync the journal files with the database
def sync_journals(template_activities_block, db_file, start_date: str, end_date: str):
    """
    Sync the journal files with the database, but only for the specified date range.

    Args:
        template_activities_block (str): Path to the Markdown template file.
        db_file (str): Path to the SQLite database file.
        start_date (str): Start date (YYYY-MM-DD).
        end_date (str): End date (YYYY-MM-DD).
    """
    template = load_template(template_activities_block)

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    query = """
    SELECT DISTINCT DATE(start_date) FROM events
    WHERE DATE(start_date) BETWEEN ? AND ?
    ORDER BY DATE(start_date);
    """
    cursor.execute(query, (start_date, end_date))
    dates = [row[0] for row in cursor.fetchall()]
    conn.close()

    for date in dates:
        events = fetch_events(db_file, date)
        update_journal(template, date, events)


if __name__ == "__main__":
    sync_journals(CONFIG["TEMPLATE_ACTIVITIES_BLOCK"], CONFIG["DB_FILE"])
