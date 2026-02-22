"""
PostgreSQL pgvector Insert Benchmark Script

Supports multiple datasets:
- Aerospace faults dataset (legacy): barcode, description, registration_code, etc.
- WoT dataset: text, book_name, chapter_title, etc.

The script auto-detects the dataset type based on available columns.
Uses a generic schema with content + JSONB metadata for flexibility.
"""
import time
import sys
import statistics
import numpy as np
import psycopg2
from psycopg2.extras import execute_values, Json
from pathlib import Path

# Add current directory to sys.path to import common
sys.path.append(str(Path(__file__).parent))
import common

# Import shared modules
sys.path.append(str(Path(__file__).parent.parent))
from shared.dataset import load_dataset, load_dataset_chunked
from shared.logger_structlog import setup_structlog

import os
import json

# Initialize structlog
logger = setup_structlog()

# Configuration
# Default paths are for local execution, overridden by Kubernetes Env Vars
DATA_FILE = Path(os.getenv("DATA_FILE", str(Path(__file__).parent.parent.parent / 'data/historic_faults_with_embeddings.parquet')))

# Load batch sizes from env or use default
batch_sizes_env = os.getenv("BATCH_SIZES")
if batch_sizes_env:
    try:
        BATCH_SIZES = json.loads(batch_sizes_env)
    except json.JSONDecodeError:
        logger.warn("invalid_batch_sizes_env", env_value=batch_sizes_env, msg="Using default batch sizes")
        BATCH_SIZES = [100, 500, 1000, 2000, 5000]
else:
    BATCH_SIZES = [100, 500, 1000, 2000, 5000]

# Chunked insert: when set to a positive integer, the dataset is streamed
# from disk in chunks of this size instead of loaded entirely into memory.
# When unset or 0, the full dataset is loaded (backward-compatible behavior).
INSERT_CHUNK_SIZE = int(os.getenv("INSERT_CHUNK_SIZE", "0"))


def detect_dataset_type(df):
    """
    Detect the dataset type based on available columns.

    Returns:
        str: 'wot' for Wheel of Time dataset, 'aerospace' for legacy aerospace faults dataset
    """
    columns = set(df.columns)

    # WoT dataset has 'text' and 'book_name' columns
    if 'text' in columns and 'book_name' in columns:
        return 'wot'
    # Aerospace dataset has 'barcode' and 'description' columns
    elif 'barcode' in columns and 'description' in columns:
        return 'aerospace'
    else:
        raise ValueError(f"Unknown dataset type. Columns: {columns}")


def get_db_connection():
    """
    Get database connection using psycopg2.

    Returns:
        psycopg2 connection object
    """
    return psycopg2.connect(
        host=common.PG_HOST,
        port=common.PG_PORT,
        dbname=common.PG_DATABASE,
        user=common.PG_USER,
        password=common.PG_PASSWORD
    )


def setup_database(conn):
    """
    Set up the database table with vector extension.
    """
    with conn.cursor() as cur:
        logger.info("setting_up_database_tables")
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        cur.execute(f"DROP TABLE IF EXISTS {common.TABLE_NAME};")
        cur.execute(f"""
            CREATE TABLE {common.TABLE_NAME} (
                id bigserial PRIMARY KEY,
                content text,
                metadata jsonb,
                embedding vector({common.EMBEDDING_DIM})
            );
        """)
    conn.commit()
    logger.info("database_tables_setup_complete")


def prepare_record(row, dataset_type):
    """
    Prepare a single record for PostgreSQL insertion based on dataset type.

    Args:
        row: DataFrame row (Series)
        dataset_type: 'wot' or 'aerospace'

    Returns:
        tuple: (content, metadata, embedding) ready for insertion
    """
    # Handle embedding conversion
    embedding_list = row['embedding']
    if isinstance(embedding_list, np.ndarray):
        embedding_list = embedding_list.tolist()

    if dataset_type == 'wot':
        # WoT dataset: use 'text' as content
        content = str(row.get('text', ''))
        metadata = {
            'book_name': str(row.get('book_name', '')),
            'chapter_number': int(row.get('chapter_number', 0)) if row.get('chapter_number') not in ['N/A', None, ''] else 0,
            'chapter_title': str(row.get('chapter_title', ''))
        }
    else:
        # Aerospace dataset: use 'description' as content
        content = str(row.get('description', row.get('content', '')))
        metadata = {
            'barcode': str(row.get('barcode', '')),
            'registration_code': str(row.get('registration_code', '')),
            'aircraft_type': str(row.get('aircraft_type', '')),
            'failed_system': str(row.get('failed_system', '')),
            'action_date': str(row.get('action_date', ''))
        }

    return (content, Json(metadata), embedding_list)


def run_insert_benchmark(df, batch_size, dataset_type, skip_setup: bool = False, conn=None):
    """
    Run insert benchmark for a specific batch size.

    Args:
        df: DataFrame containing the dataset (or a single chunk)
        batch_size: Number of records per batch
        dataset_type: 'wot' or 'aerospace'
        skip_setup: If True, skip table drop/recreate. Used for chunked inserts.
        conn: Optional existing database connection. If None, a new one is created.
    """
    log = logger.bind(batch_size=batch_size, database="PostgreSQL", dataset_type=dataset_type)
    log.info("starting_benchmark_run")

    # Use provided connection or create a new one
    own_conn = conn is None
    if own_conn:
        conn = get_db_connection()

    if not skip_setup:
        setup_database(conn)

    # Ensure result tables exist in the results database
    pg_results_conn = common.results_db.get_results_db_connection()
    common.results_db.setup_results_tables(pg_results_conn)
    pg_results_conn.close()

    total_records = len(df)

    log.info("preparing_data", total_records=total_records)
    records = []
    for _, row in df.iterrows():
        record = prepare_record(row, dataset_type)
        records.append(record)

    log.info("starting_insertion")
    batch_times = []
    granular_metrics = []

    start_time_total = time.time()
    cumulative_time = 0
    cumulative_vectors = 0

    with conn.cursor() as cur:
        num_batches = (total_records + batch_size - 1) // batch_size

        for i in range(0, total_records, batch_size):
            batch_num = (i // batch_size) + 1
            batch = records[i:i + batch_size]
            current_batch_size = len(batch)

            start_time_batch = time.time()
            execute_values(
                cur,
                f"INSERT INTO {common.TABLE_NAME} (content, metadata, embedding) VALUES %s",
                batch,
                template=None,
                page_size=batch_size
            )
            conn.commit()
            end_time_batch = time.time()
            batch_duration = end_time_batch - start_time_batch

            batch_times.append(batch_duration)
            cumulative_time += batch_duration
            cumulative_vectors = i + current_batch_size
            instant_throughput = current_batch_size / batch_duration if batch_duration > 0 else 0

            granular_metrics.append({
                "batch_number": batch_num,
                "batch_time_seconds": batch_duration,
                "cumulative_time_seconds": cumulative_time,
                "cumulative_vectors_inserted": cumulative_vectors,
                "batch_size_actual": current_batch_size,
                "instantaneous_throughput": instant_throughput
            })

            # Print progress every batch
            log.info("batch_complete",
                     batch=batch_num,
                     total_batches=num_batches,
                     vectors_inserted=cumulative_vectors,
                     throughput=round(instant_throughput, 2))

    end_time_total = time.time()
    total_time = end_time_total - start_time_total

    log.info("benchmark_complete", duration_seconds=round(total_time, 2))

    # Save Results to results database
    pg_results_conn = common.results_db.get_results_db_connection()
    common.results_db.save_insert_metrics(
        pg_results_conn,
        common.get_database_name(),
        common.TEST_LOCATION,
        common.DATASET_SIZE,
        common.DATABASE_CONFIG,
        total_records,
        batch_size,
        batch_times,
        granular_metrics,
        total_time
    )
    pg_results_conn.close()

    # Only close the connection if we created it ourselves
    if own_conn:
        conn.close()


def run_insert_benchmark_chunked(batch_size: int, dataset_type: str) -> None:
    """
    Run insert benchmark in chunked mode for a single batch_size.

    Streams the dataset from disk in chunks of INSERT_CHUNK_SIZE rows.
    Creates the table once, then inserts all chunks sequentially.
    Saves a single aggregated metric at the end.

    Args:
        batch_size: Number of records per insert batch.
        dataset_type: 'wot' or 'aerospace'.
    """
    log = logger.bind(
        task="chunked_insert",
        batch_size=batch_size,
        chunk_size=INSERT_CHUNK_SIZE,
        database="PostgreSQL",
    )
    log.info("chunked_insert_started")

    conn = get_db_connection()
    setup_database(conn)

    # Ensure result tables exist in the results database
    pg_results_conn = common.results_db.get_results_db_connection()
    common.results_db.setup_results_tables(pg_results_conn)
    pg_results_conn.close()

    # Accumulators for cross-chunk metrics
    all_batch_times: list[float] = []
    all_granular_metrics: list[dict] = []
    total_records = 0
    cumulative_time = 0.0
    cumulative_vectors = 0
    global_batch_counter = 0

    start_time_total = time.time()

    for chunk_index, chunk_df in enumerate(load_dataset_chunked(DATA_FILE, chunk_size=INSERT_CHUNK_SIZE)):
        chunk_records = len(chunk_df)
        total_records += chunk_records

        log.info("processing_chunk", chunk_index=chunk_index, chunk_records=chunk_records)

        # Insert records in batches — prepare records per batch (not per chunk)
        # to avoid materializing all 100K embeddings as Python lists at once.
        with conn.cursor() as cur:
            num_batches = (chunk_records + batch_size - 1) // batch_size

            for batch_num in range(num_batches):
                i = batch_num * batch_size
                batch_df = chunk_df.iloc[i:i + batch_size]
                current_batch_size = len(batch_df)

                # Prepare records only for this batch
                batch = []
                for _, row in batch_df.iterrows():
                    batch.append(prepare_record(row, dataset_type))

                start_time_batch = time.time()
                execute_values(
                    cur,
                    f"INSERT INTO {common.TABLE_NAME} (content, metadata, embedding) VALUES %s",
                    batch,
                    template=None,
                    page_size=batch_size,
                )
                conn.commit()
                batch_duration = time.time() - start_time_batch

                all_batch_times.append(batch_duration)
                cumulative_time += batch_duration
                cumulative_vectors += current_batch_size
                global_batch_counter += 1
                instant_throughput = current_batch_size / batch_duration if batch_duration > 0 else 0

                all_granular_metrics.append({
                    "batch_number": global_batch_counter,
                    "batch_time_seconds": batch_duration,
                    "cumulative_time_seconds": cumulative_time,
                    "cumulative_vectors_inserted": cumulative_vectors,
                    "batch_size_actual": current_batch_size,
                    "instantaneous_throughput": instant_throughput,
                })

                log.info(
                    "batch_complete",
                    global_batch=global_batch_counter,
                    chunk=chunk_index,
                    vectors_inserted=cumulative_vectors,
                    throughput=round(instant_throughput, 2),
                )

    end_time_total = time.time()
    total_time = end_time_total - start_time_total

    log.info(
        "chunked_insert_complete",
        total_records=total_records,
        total_time_seconds=round(total_time, 2),
        avg_throughput=round(total_records / total_time, 2) if total_time > 0 else 0,
    )

    # Save aggregated results to results database
    pg_results_conn = common.results_db.get_results_db_connection()
    common.results_db.save_insert_metrics(
        pg_results_conn,
        common.get_database_name(),
        common.TEST_LOCATION,
        common.DATASET_SIZE,
        common.DATABASE_CONFIG,
        total_records,
        batch_size,
        all_batch_times,
        all_granular_metrics,
        total_time,
    )
    pg_results_conn.close()
    conn.close()


def main():
    """
    Main entry point for PostgreSQL pgvector insert benchmark.

    Supports two modes controlled by the INSERT_CHUNK_SIZE env var:
    - INSERT_CHUNK_SIZE == 0 (default): Load entire dataset into memory.
    - INSERT_CHUNK_SIZE > 0: Stream dataset from disk in chunks.
    """
    logger.info("test_init", database="Azure PostgreSQL (pgvector)")

    if INSERT_CHUNK_SIZE > 0:
        # --- Chunked mode: stream from disk ---
        logger.info("mode_selected", mode="chunked", chunk_size=INSERT_CHUNK_SIZE)

        # Detect dataset type from the first chunk
        first_chunk = next(load_dataset_chunked(DATA_FILE, chunk_size=INSERT_CHUNK_SIZE))
        try:
            dataset_type = detect_dataset_type(first_chunk)
            logger.info("dataset_type_detected", dataset_type=dataset_type)
        except ValueError as e:
            logger.error("dataset_detection_failed", error=str(e))
            return
        del first_chunk

        for batch_size in BATCH_SIZES:
            try:
                run_insert_benchmark_chunked(batch_size, dataset_type)
            except Exception as e:
                logger.error("benchmark_failed", batch_size=batch_size, error=str(e))
                continue
    else:
        # --- Legacy mode: load full dataset into memory ---
        logger.info("mode_selected", mode="full_load")
        try:
            df = load_dataset(DATA_FILE)
            logger.info("dataset_loaded", path=str(DATA_FILE), records=len(df))
        except Exception as e:
            logger.error("dataset_load_failed", error=str(e))
            return

        try:
            dataset_type = detect_dataset_type(df)
            logger.info("dataset_type_detected", dataset_type=dataset_type)
        except ValueError as e:
            logger.error("dataset_detection_failed", error=str(e))
            return

        for batch_size in BATCH_SIZES:
            try:
                run_insert_benchmark(df, batch_size, dataset_type)
            except Exception as e:
                logger.error("benchmark_failed", batch_size=batch_size, error=str(e))
                continue


if __name__ == "__main__":
    main()
