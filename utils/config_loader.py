import os
from dotenv import load_dotenv

def load_config():
    """Load environment variables from .env file."""
    load_dotenv()
    config = {
        "DUCKDB_TOKEN": os.getenv("DUCKDB_TOKEN"),
        "SCRAPOXY_USER": os.getenv("SCRAPOXY_USER"),
        "SCRAPOXY_TOKEN": os.getenv("SCRAPOXY_TOKEN"),
        "SCRAPOXY_URL": os.getenv("SCRAPOXY_URL"),
    }
    missing_keys = [key for key, value in config.items() if value is None]
    if missing_keys:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_keys)}")
    return config
