import logging
import os
from modules.config import CONFIG

def setup_logging(log_dir, log_file_name):
    """
    Set up logging to append logs instead of overwriting.
    Logs will be saved to a file and displayed in the console.
    """
    # Ensure log directory exists
    os.makedirs(CONFIG["LOG_DIR"], exist_ok=True)
    log_path = os.path.join(CONFIG["LOG_DIR"], CONFIG["LOG_FILE"])

    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove any existing handlers to avoid duplication
    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler - Append mode to keep previous logs
    file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)

    # Console handler - Print to terminal
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logging.info(f"Logging initialized. Logs will be appended to: {log_path}")