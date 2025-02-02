import os
import sqlite3
import yaml
import logging
from modules.logging import setup_logging
from modules.config import CONFIG

# Configure logging
setup_logging(CONFIG["LOG_DIR"], CONFIG["LOG_FILE"])

# Get table name from configuration
TABLE_NAME = CONFIG.get("DB_TABLE_NAME", "events")

# Mapping front matter keys to database column names
FRONTMATTER_TO_DB_MAPPING = CONFIG.get("FRONTMATTER_TO_DB_MAPPING", {
    "title": "event_title",
    "date": "event_date",
    "location": "event_location"
})

def extract_frontmatter(content: str) -> dict:
    """
    Extracts the front matter (YAML metadata) from a Markdown file.
    Assumes front matter is wrapped between `---` at the start and end.
    """
    parts = content.split("---")
    if len(parts) > 2:
        return yaml.safe_load(parts[1])  # Extract YAML front matter
    return {}

def update_sqlite_from_markdown(db_path: str, markdown_dir: str, table_name: str):
    """
    Reads all Markdown files, extracts front matter, and updates SQLite database.
    Updates only existing records and maps front matter fields to correct database columns.
    Uses a dynamic table name specified in the configuration.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for filename in os.listdir(markdown_dir):
        if filename.endswith(".md"):
            file_path = os.path.join(markdown_dir, filename)
            primary_key = os.path.splitext(filename)[0]  # Extract filename without extension

            with open(file_path, "r", encoding="utf-8") as file:
                content = file.read()
            
            frontmatter = extract_frontmatter(content)
            if not frontmatter:
                logging.info(f"Skipping {filename}: No front matter found")
                continue

            # Map front matter keys to database columns
            mapped_data = {
                FRONTMATTER_TO_DB_MAPPING[key]: frontmatter[key]
                for key in FRONTMATTER_TO_DB_MAPPING if key in frontmatter
            }
            if not mapped_data:
                logging.info(f"Skipping {filename}: No mapped fields found for update.")
                continue

            # Check if the record exists before updating
            check_sql = f"SELECT 1 FROM {table_name} WHERE event_id = ?"
            cursor.execute(check_sql, (primary_key,))
            if cursor.fetchone() is None:
                logging.warning(f"Skipping {filename}: No existing record with ID '{primary_key}' found in table '{table_name}'.")
                continue

            # Build update query
            set_clause = ", ".join([f'"{db_column}" = ?' for db_column in mapped_data.keys()])
            values = list(mapped_data.values()) + [primary_key]

            update_sql = f'UPDATE {table_name} SET {set_clause} WHERE event_id = ?'
            try:
                cursor.execute(update_sql, values)
                if cursor.rowcount > 0:
                    logging.info(f"Updated {filename} in table '{table_name}' with fields: {list(mapped_data.keys())}")
                else:
                    logging.warning(f"No changes made for {filename} (ID: {primary_key}).")
            except sqlite3.Error as e:
                logging.error(f"Error updating {filename} in table '{table_name}': {e}")

    conn.commit()
    conn.close()

# Usage
db_path = CONFIG["DB_FILE"]  # SQLite database path
markdown_dir = CONFIG["OUTPUT_ACTIVITIES_DIR"]  # Directory containing markdown files
update_sqlite_from_markdown(db_path, markdown_dir, CONFIG["DB_TABLE_NAME"])
