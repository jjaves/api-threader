import threading
from tqdm import tqdm

class ProgressUpdater:
    def __init__(self, total):
        self.meta = {
            "🫸": 0,  # remaining in record queue
            "🙋": 0,  # requests
            "🤔": 0,  # queue
            "❌": 0,  # failed
            "✍️": 0,  # written to DB
        }
        self.progress_bar = tqdm(
            total=total,
            position=1,
            ncols=130,
            colour="green",
            leave=False,
            postfix=self.meta
        )
        self.lock = threading.Lock()

    def update(self, n=1):
        with self.lock:
            self.progress_bar.update(n)

    def set_meta(self, key, value):
        with self.lock:
            self.meta[key] = value
            self.progress_bar.set_postfix(**self.meta)

    def increment_meta(self, key, increment=1):
        with self.lock:
            self.meta[key] += increment
            self.progress_bar.set_postfix(**self.meta)

    def close(self):
        with self.lock:
            self.progress_bar.close()
