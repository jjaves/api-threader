# Python
import logging
import requests

logger = logging.getLogger("base_client_logger")

class BaseClient:
    def __init__(self, token=None, max_records=100):
        self.token = token
        self.max_records = max_records
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})

    @staticmethod
    def safe_get(data, *keys):
        """
        Safely retrieve nested data from a dictionary.
        Returns None if any key is missing.
        """
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None
        return data

    @staticmethod
    def extract_array(data, fields=None):
        """
        Extracts an array of dictionaries with specific fields.
        """
        if not isinstance(data, list):
            return []
        if not fields:
            return data
        return [{field: BaseClient.safe_get(item, *field) for field in fields} for item in data]

    def get_imdb_endpoint_ids(self, table_name, column_counter, counter_filter=1, limit=100, order_type="desc"):
        """
        Mock method to simulate fetching unprocessed IMDb IDs from a database.
        """
        logger.info(f"Fetching IMDb IDs from table: {table_name}")
        # Example: Simulate fetching IDs
        return [(f"tt{id:07d}", counter_filter) for id in range(1, limit + 1)]

    def close(self):
        """Close the session."""
        self.session.close()
