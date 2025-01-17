import logging
from queue import Empty
import duckdb
import time
from datetime import datetime
from utils.data_store import write_to_duckdb, write_to_disk
from utils.http_retry import requests_retry_session

logger = logging.getLogger(__name__)

def writer_thread(
        result_queue,
        record_queue,
        thread,
        batch_size,
        max_records,
        config,
        connection_string,
        writer_terminate_flag,
        progress_updater,
        sentinel,
):
    """
    Single thread responsible for aggregating data from result_queue and writing it to DuckDB.
    """
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

def worker(
        record_queue,
        result_queue,
        failure_queue,
        batch_size,
        config,
        connection_string,
        terminate_flag,
        progress_updater,
        proxies,
        sentinel
):
    """
    Each worker processes items from the record_queue for a single endpoint.
    """
    time.sleep(1)
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
                format_failed(
                    example_id=example_id,
                    reason_code="No Data",
                    error_message="No data returned",
                    table_name=config["table_name"],
                    failure_queue=failure_queue,
                    progress_updater=progress_updater
                )
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

def failure_worker(
        failure_queue,
        record_queue,
        batch_size,
        max_records,
        config,
        connection_string,
        failure_terminate_flag,
        progress_updater,
        sentinel
):
    """
    Stores failure records in a global dictionary and writes to DuckDB.
    """
    failed_aggregated_batch = []
    failed_threshold = min(batch_size, max_records)
    con_failure_worker = duckdb.connect(connection_string)

    while True:
        try:
            failed_batch = failure_queue.get(timeout=3)
            if failed_batch is sentinel:
                failure_queue.task_done()
                break

            failed_aggregated_batch.append(failed_batch)
            failure_queue.task_done()
        except Empty:
            if failure_terminate_flag.is_set():
                break
            continue
        except Exception as e:
            logger.error(f"Error in failure_worker: {e}")

        if len(failed_aggregated_batch) >= failed_threshold:
            write_to_duckdb(
                failed_aggregated_batch,
                config["failed_table"],
                con_failure_worker,
                progress_updater
            )
            failed_aggregated_batch.clear()

    if failed_aggregated_batch:
        time.sleep(5)
        write_to_duckdb(
            failed_aggregated_batch,
            config["failed_table"],
            con_failure_worker,
            progress_updater
        )
        failed_aggregated_batch.clear()
    con_failure_worker.close()

def format_failed(example_id, reason_code, error_message, table_name, failure_queue, progress_updater):
    """
    Format failed record before sending to failure worker.
    """
    progress_updater.increment_meta("❌", 1)

    failure_data = {
        "example_id": example_id,
        "reason_code": reason_code,
        "error_message": error_message,
        "recorded_at": datetime.utcnow().isoformat(),
        "table_name": table_name
    }

    failure_queue.put(failure_data.copy())
    failure_data.clear()