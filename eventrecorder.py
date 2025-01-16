import datetime
import os
import sys
import logging
import traceback
import io

class Recorder:
    """Class wrapper for logging and traceback management."""

    def __init__(self, script_name="script", directory="logs", level="INFO", days_of_logs=30):
        self.script_name = script_name
        self.today = datetime.date.today()
        self.log_path = os.path.join(os.getcwd(), directory)
        self.days_of_logs = days_of_logs
        self.log_file = os.path.join(self.log_path, f"{self.today}_log.txt")

        if not os.path.exists(self.log_path):
            os.makedirs(self.log_path)

        self.logger = logging.getLogger(script_name)
        self.logger.setLevel(getattr(logging, level.upper(), logging.INFO))

        file_handler = logging.FileHandler(self.log_file)
        console_handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def log(self, level, message):
        """Generic logging function."""
        log_function = getattr(self.logger, level.lower(), self.logger.info)
        log_function(message)

    def trim_logs(self):
        """Remove log files older than n days."""
        days_ago = datetime.timedelta(self.days_of_logs)
        for filename in os.listdir(self.log_path):
            file_path = os.path.join(self.log_path, filename)
            if filename.endswith('.txt'):
                file_date = datetime.datetime.strptime(filename.split('_')[0], "%Y-%m-%d").date()
                if file_date <= datetime.datetime.now().date() - days_ago:
                    os.remove(file_path)
