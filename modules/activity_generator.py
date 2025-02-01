import os
import sqlite3
from typing import Any, Dict, List

import yaml
from jinja2 import Template


class MarkdownSyncGenerator:
    def __init__(
        self,
        db_path: str,
        template_path: str,
        output_dir: str,
        frontmatter_columns: Dict[str, str],
    ):
        """
        Initialize the MarkdownSyncGenerator with paths for the database, template, and output directory.

        Args:
            db_path (str): Path to the SQLite database.
            template_path (str): Path to the Markdown template file.
            output_dir (str): Directory to save the generated Markdown files.
            frontmatter_columns (Dict[str, str]): Mapping of frontmatter fields to database columns.
        """
        self.db_path = db_path
        self.template_path = template_path
        self.output_dir = output_dir
        self.frontmatter_columns = frontmatter_columns
        os.makedirs(output_dir, exist_ok=True)

    def fetch_data(
        self, table_name: str, start_date: str, end_date: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch events from the specified SQLite table within the given date range.
        Replace `None` values with an empty string.

        Args:
            table_name (str): Name of the table to fetch data from.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).

        Returns:
            List[Dict[str, Any]]: List of rows represented as dictionaries.
        """

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            query = f"""
            SELECT * FROM {table_name} 
            WHERE DATE(start_date) BETWEEN ? AND ?;
            """
            cursor.execute(query, (start_date, end_date))
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]

        return [self.replace_none(dict(zip(column_names, row))) for row in rows]

    @staticmethod
    def replace_none(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replace `None` values in a dictionary with empty strings.

        Args:
            data (Dict[str, Any]): Dictionary to process.

        Returns:
            Dict[str, Any]: Updated dictionary with `None` replaced by empty strings.
        """
        return {
            key: (value if value is not None else "") for key, value in data.items()
        }

    def load_template(self) -> Template:
        """
        Load the Markdown template using Jinja2.

        Returns:
            Template: Compiled Jinja2 template.
        """
        with open(self.template_path, "r") as file:
            template_content = file.read()
        return Template(template_content)

    def read_markdown_body(self, file_path: str) -> str:
        """
        Read the body content from an existing Markdown file, excluding frontmatter.

        Args:
            file_path (str): Path to the Markdown file.

        Returns:
            str: The body content of the Markdown file.
        """
        with open(file_path, "r") as file:
            lines = file.readlines()

        # If frontmatter exists, skip it (everything before the second '---')
        if lines[0].strip() == "---":
            end_idx = lines[1:].index("---\n") + 1
            return "".join(lines[end_idx + 1 :])
        return "".join(lines)

    def write_markdown(self, content: str, file_name: str):
        """
        Save the generated Markdown content to a file.

        Args:
            content (str): Markdown content to save.
            file_name (str): Name of the Markdown file.
        """
        file_path = os.path.join(self.output_dir, file_name)
        with open(file_path, "w") as file:
            file.write(content)

    def update_frontmatter(self, file_path: str, new_data: Dict[str, Any]):
        """
         Delete the existing frontmatter and write new frontmatter from the database.

        Args:
        file_path (str): Path to the existing Markdown file.
        new_data (Dict[str, Any]): New frontmatter data from the database.
        """

        # Customize YAML dumping to avoid quoting dates
        def represent_str_unquoted(dumper, value):
            if any(k in ["start-date", "end-date"] for k in new_data):
                return dumper.represent_scalar(
                    "tag:yaml.org,2002:str", value, style=None
                )
            return dumper.represent_scalar("tag:yaml.org,2002:str", value)

        yaml.add_representer(str, represent_str_unquoted, Dumper=yaml.Dumper)

        frontmatter = yaml.dump(new_data, default_flow_style=False, sort_keys=False)
        body_content = self.read_markdown_body(file_path)

        # Recreate the file with new frontmatter
        updated_content = "---\n" + frontmatter + "---\n" + body_content

        with open(file_path, "w") as updated_file:
            updated_file.write(updated_content)

    def generate(
        self, table_name: str, file_name_column: str, start_date: str, end_date: str
    ):
        """
        Generate or update Markdown files from the SQLite table based on the specified date range.

        Args:
            table_name (str): Name of the SQLite table to fetch data from.
            file_name_column (str): Column used to generate file names.
            start_date (str): Start date (YYYY-MM-DD).
            end_date (str): End date (YYYY-MM-DD).
        """
        template = self.load_template()
        rows = self.fetch_data(table_name, start_date, end_date)

        for row in rows:
            file_name = f"{row[file_name_column]}.md"
            file_path = os.path.join(self.output_dir, file_name)
            markdown_content = template.render(row)

            if os.path.exists(file_path):
                self.update_frontmatter(file_path, row)
            else:
                self.write_markdown(markdown_content, file_name)
