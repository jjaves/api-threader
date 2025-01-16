import logging
from datetime import datetime
import pandas as pd
import duckdb

logger = logging.getLogger(__name__)
records_written = 0

def write_to_disk(batch, table_name):
    """Write batch to disk."""
    if not batch:
        logger.debug(f"No records to write to disk for {table_name}.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fail_path = './failed_batches/backups'
    file_name = f'batches_{table_name.replace(".", "_")}_{timestamp}'
    batch_df = pd.DataFrame(batch)

    try:
        out_type = 'parquet'
        parquet_failed_path = f'{fail_path}_{out_type}/{file_name}.{out_type}'
        batch_df.to_parquet(parquet_failed_path, engine='pyarrow', index=False)
    except Exception as e:
        logger.error(f"Error in write_to_disk: {e}")

    logger.debug(f"JSON & Parquet file written to disk at {fail_path}")

def write_to_duckdb(batch, table_name, con, progress_updater):
    """Write batch to DuckDB database."""
    global records_written

    try:
        df = pd.DataFrame(batch)
        con.execute(f'INSERT INTO {table_name} SELECT * FROM df')

        progress_updater.increment_meta("✍️", len(batch))
        records_written += len(batch)
        logger.info(f"Wrote {len(batch)} records to DuckDB (Total: {records_written})")
    except duckdb.Error as e:
        logger.error(f"Error writing to DuckDB: {e}")
        write_to_disk(batch, table_name)
