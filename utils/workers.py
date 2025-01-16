import logging
from queue import Empty
import duckdb
from utils.data_store import write_to_duckdb, write_to_disk
from utils.http_retry import requests_retry_session

logger = logging.getLogger(__name__)

def writer_thread(result_queue, record_queue, thread, batch_size, max_records, config, connection_string, writer_terminate_flag, progress_updater, sentinel):
    aggregated_batch = []
    aggregation_threshold = min(thread * batch_size // 2, max_records)
    con_writer = duckdb.connect(connection_string)

    while True:
        try:
            batch = result_queue.get(timeout=3)
            if batch is sentinel:
                result_queue.task_done()
                break
            progress_updater.increment_meta("🤔", len(batch))
            aggregated_batch.extend(batch)
            result_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            logger.error(f"Error in writer thread: {e}")

        if len(aggregated_batch) >= aggregation_threshold:
            write_to_duckdb(aggregated_batch, config["table_name"], con_writer, progress_updater)
            progress_updater.increment_meta("🤔", -len(aggregated_batch))
            aggregated_batch.clear()

    if aggregated_batch:
        write_to_duckdb(aggregated_batch, config["table_name"], con_writer, progress_updater)
        progress_updater.increment_meta("🤔", -len(aggregated_batch))
        aggregated_batch.clear()

    con_writer.close()

def worker(record_queue, result_queue, failure_queue, batch_size, config, connection_string, terminate_flag, progress_updater, proxies, sentinel):
    session = requests_retry_session(proxies=proxies)
    local_batch = []

    while not terminate_flag.is_set():
        try:
            item = record_queue.get(timeout=3)
        except Empty:
            continue

        if item is sentinel:
            record_queue.task_done()
            progress_updater.set_meta("🫸", record_queue.qsize())
            break

        example_id, record_count = item

        try:
            progress_updater.increment_meta("🙋", 1)
            progress_updater.update(1)
            resp_data = config["get_function"](example_id, record_count, config, session=session)

            if not resp_data:
                logger.error(f"No data returned for ID {example_id} with {record_count} records")
                continue

            filtered = config["filter_func"](resp_data, example_id, config)
            if not filtered:
                logger.error(f"Filtering failed for {example_id} with {record_count} records")
                continue

            local_batch.extend(filtered)
            if len(local_batch) >= batch_size:
                result_queue.put(local_batch.copy())
                local_batch.clear()

        except Exception as e:
            logger.error(f"Error processing ID {example_id}: {e}")
        finally:
            record_queue.task_done()
            progress_updater.set_meta("🫸", record_queue.qsize())

    if local_batch:
        result_queue.put(local_batch.copy())
        local_batch.clear()
