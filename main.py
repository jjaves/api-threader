import os
import signal
import threading
import time
import argparse
from queue import Queue
from dotenv import load_dotenv
from utils.eventrecorder import Recorder
from utils.progress_bar import ProgressUpdater
from utils.workers import worker, writer_thread, failure_worker
from example_client.base_client import BaseClient
from example_client.endpoints_config import endpoints
from utils.config_loader import load_config

load_dotenv()

# Initialize logging
logger = Recorder(script_name="ThreadedAPI", directory="logs", level="INFO").logger

# Load configuration
config = load_config()

SENTINEL = object()
NUM_THREADS = 4
BATCH_SIZE = 10
MAX_RECORDS = 100

def make_proxy(prefix=""):
    user = os.getenv(f"SCRAPOXY{prefix}_USER")
    token = os.getenv(f"SCRAPOXY{prefix}_TOKEN")
    port = os.getenv(f"SCRAPOXY{prefix}_PORT")
    url = os.getenv("SCRAPOXY_URL")
    crt = os.getenv(f"SCRAPOXY{prefix}_CRT")

    if not all([user, token, port, url]):
        logger.error(f"Missing proxy configuration for prefix '{prefix}'.")
        raise ValueError(f"Incomplete proxy configuration for prefix '{prefix}'.")

    proxy_url = f"http://{user}:{token}@{url}:{port}"
    return proxy_url, crt

proxies = [make_proxy(), make_proxy("_2")]

def main():
    parser = argparse.ArgumentParser(description="Threaded API Miner")
    parser.add_argument(
        "--endpoint",
        type=str,
        default=os.getenv("DEFAULT_ENDPOINT", "locations"),
        help="The endpoint key to use (default: 'locations')."
    )
    args = parser.parse_args()

    endpoint_key = args.endpoint
    if endpoint_key not in endpoints:
        logger.error(f"Invalid endpoint key: {endpoint_key}. Available keys: {list(endpoints.keys())}")
        return

    endpoint_config = endpoints[endpoint_key]
    app_state = {
        'terminate_flag': threading.Event(),
        'writer_terminate_flag': threading.Event(),
        'failure_terminate_flag': threading.Event()
    }

    signal.signal(signal.SIGINT, handle_interrupt)

    def handle_interrupt(signal_num, frame):
        logger.info("Interrupt received. Cleaning up...")
        app_state['terminate_flag'].set()
        app_state['writer_terminate_flag'].set()
        app_state['failure_terminate_flag'].set()
        exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)

    endpoint_config = endpoints[endpoint_key]

    progress_updater = ProgressUpdater(total=MAX_RECORDS)
    for key in ["✍️", "❌", "🤔", "🙋", "🫸"]:
        progress_updater.set_meta(key, 0)

    base_data = BaseClient(config["DUCKDB_TOKEN"], max_records=MAX_RECORDS)
    unprocessed_ids = base_data.get_example_endpoint_ids(
        endpoint_config["table_name"],
        endpoint_config.get("column_counter", 1),
        counter_filter=1,
        limit=MAX_RECORDS,
        order_type='desc'
    )

    if not unprocessed_ids:
        logger.info("No unprocessed IDs found.")
        progress_updater.close()
        return

    record_queue = Queue()
    result_queue = Queue()
    failure_queue = Queue()

    for id_tuple in unprocessed_ids:
        record_queue.put(id_tuple)

    for _ in range(NUM_THREADS):
        record_queue.put(SENTINEL)

    threads = []
    for num in range(NUM_THREADS):
        t = threading.Thread(
            target=worker,
            args=(
                record_queue,
                result_queue,
                failure_queue,
                BATCH_SIZE,
                endpoint_config,
                config["DUCKDB_TOKEN"],
                app_state['terminate_flag'],
                progress_updater,
                proxies,
                SENTINEL
            ),
            name=f"Worker-{num}"
        )
        t.daemon = True  # Ensure thread terminates with the main program
        t.start()
        threads.append(t)

    writer = threading.Thread(
        target=writer_thread,
        args=(
            result_queue,
            record_queue,
            NUM_THREADS,
            BATCH_SIZE,
            MAX_RECORDS,
            endpoint_config,
            config["DUCKDB_TOKEN"],
            app_state['writer_terminate_flag'],
            progress_updater,
            SENTINEL
        ),
        name="WriterThread"
    )
    writer.daemon = True
    writer.start()

    failure_writer = threading.Thread(
        target=failure_worker,
        args=(
            failure_queue,
            record_queue,
            BATCH_SIZE,
            MAX_RECORDS,
            endpoint_config,
            config["DUCKDB_TOKEN"],
            app_state['failure_terminate_flag'],
            progress_updater,
            SENTINEL
        ),
        name="FailWriterThread"
    )
    failure_writer.daemon = True
    failure_writer.start()

    start_time = time.time()

    try:
        record_queue.join(timeout=30)
    except Exception as e:
        logger.error(f"Error while waiting for record_queue to join: {e}")
    
    result_queue.put(SENTINEL)
    app_state['terminate_flag'].set()

    try:
        result_queue.join(timeout=30)
    except Exception as e:
        logger.error(f"Error while waiting for result_queue to join: {e}")

    failure_queue.put(SENTINEL)

    try:
        failure_queue.join(timeout=30)
    except Exception as e:
        logger.error(f"Error while waiting for failure_queue to join: {e}")

    app_state['writer_terminate_flag'].set()
    app_state['failure_terminate_flag'].set()

    for t in threads:
        t.join(timeout=10)
    writer.join(timeout=10)
    failure_writer.join(timeout=10)

    progress_updater.close()
    end_time = time.time()
    logger.info(f"Processed {MAX_RECORDS} records in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
