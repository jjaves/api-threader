import os
import threading
import signal
from queue import Queue
from dotenv import load_dotenv
from api_client.base_client import BaseClient
from api_client.endpoints_config import endpoints
from utils.workers import worker, writer_thread, failure_worker
from utils.progress_bar import ProgressUpdater

load_dotenv()

def main():
    app_state = {
        'terminate_flag': threading.Event(),
        'writer_terminate_flag': threading.Event(),
        'failure_terminate_flag': threading.Event()
    }

    def handle_interrupt(signal_num, frame):
        print("\nInterrupt received. Cleaning up...")
        app_state['terminate_flag'].set()
        exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)

    endpoint_key = "awards"  # Example endpoint
    config = endpoints[endpoint_key]

    progress_updater = ProgressUpdater(total=100)  # Example total
    base_data = BaseClient("connection_string", max_records=100)
    unprocessed_ids = base_data.get_ids(config["table_name"], limit=100)

    if not unprocessed_ids:
        print("No unprocessed IDs found.")
        return

    record_queue = Queue()
    result_queue = Queue()
    failure_queue = Queue()

    for id_tuple in unprocessed_ids:
        record_queue.put(id_tuple)

    threads = []
    for _ in range(4):  # Example thread count
        t = threading.Thread(
            target=worker,
            args=(record_queue, result_queue, failure_queue, 10, config, "connection_string", app_state['terminate_flag'], progress_updater, None, None)
        )
        t.start()
        threads.append(t)

    writer = threading.Thread(
        target=writer_thread,
        args=(result_queue, record_queue, 4, 10, 100, config, "connection_string", app_state['writer_terminate_flag'], progress_updater, None)
    )
    writer.start()

    failure_writer = threading.Thread(
        target=failure_worker,
        args=(failure_queue, record_queue, 10, 100, config, "connection_string", app_state['failure_terminate_flag'], progress_updater, None)
    )
    failure_writer.start()

    for t in threads:
        t.join()
    writer.join()
    failure_writer.join()

if __name__ == "__main__":
    main()