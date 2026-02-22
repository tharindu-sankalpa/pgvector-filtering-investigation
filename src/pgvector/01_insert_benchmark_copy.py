"""
PostgreSQL pgvector Insert Benchmark - COPY Binary Format

This benchmark uses PostgreSQL's COPY command with binary format for maximum
ingestion performance. This is the official pgvector-python recommended approach
for bulk loading vectors.

Key differences from standard INSERT benchmark:
- Uses COPY protocol instead of INSERT statements
- Binary format eliminates text parsing overhead
- Direct data streaming to PostgreSQL
- 10-50x faster than standard INSERT for bulk loading

IMPORTANT: This benchmark is NOT directly comparable to MongoDB/Milvus/Azure
standard insert benchmarks. Use 01_insert_benchmark.py for fair comparisons.
This benchmark shows PostgreSQL's maximum bulk loading capability.

Supports multiple datasets:
- Aerospace faults dataset (legacy): barcode, description, registration_code, etc.
- WoT dataset: text, book_name, chapter_title, etc.

The script auto-detects the dataset type based on available columns.
Uses a generic schema with content + JSONB metadata for flexibility.
"""

import time
import sys
import json
import os
from pathlib import Path

import numpy as np
import psycopg
from psycopg.types.json import Jsonb
from pgvector.psycopg import register_vector

# Add current directory to sys.path to import common
sys.path.append(str(Path(__file__).parent))
import common

# Import shared modules
sys.path.append(str(Path(__file__).parent.parent))
from shared.dataset import load_dataset, load_dataset_chunked
from shared.logger_structlog import setup_structlog

# Initialize structlog
logger = setup_structlog()

# Configuration
DATA_FILE = Path(os.getenv(
    "DATA_FILE",
    str(Path(__file__).parent.parent.parent / 'data/historic_faults_with_embeddings.parquet')
))

# Load batch sizes from env or use default
# For COPY, larger batches are more efficient
batch_sizes_env = os.getenv("BATCH_SIZES")
if batch_sizes_env:
    try:
        BATCH_SIZES = json.loads(batch_sizes_env)
    except json.JSONDecodeError:
        logger.warn("invalid_batch_sizes_env", env_value=batch_sizes_env)
        BATCH_SIZES = [1000, 5000, 10000, 25000, 50000]
else:
    BATCH_SIZES = [1000, 5000, 10000, 25000, 50000]

# Chunked insert: when set to a positive integer, the dataset is streamed
# from disk in chunks of this size instead of loaded entirely into memory.
INSERT_CHUNK_SIZE = int(os.getenv("INSERT_CHUNK_SIZE", "0"))


def detect_dataset_type(df):
    """
    Detect the dataset type based on available columns.

    Returns:
        str: 'wot' for Wheel of Time dataset, 'aerospace' for legacy aerospace faults dataset
    """
    columns = set(df.columns)

    if 'text' in columns and 'book_name' in columns:
        return 'wot'
    elif 'barcode' in columns and 'description' in columns:
        return 'aerospace'
    else:
        raise ValueError(f"Unknown dataset type. Columns: {columns}")


def get_db_connection():
    """
    Get database connection using psycopg3 with vector type registered.

    Returns:
        psycopg connection object with vector type support
    """
    conn = psycopg.connect(
        host=common.PG_HOST,
        port=common.PG_PORT,
        dbname=common.PG_DATABASE,
        user=common.PG_USER,
        password=common.PG_PASSWORD,
        autocommit=True
    )

    # Register vector type for COPY binary format support
    register_vector(conn)

    return conn


def setup_database(conn):
    """
    Set up the database table with vector extension.
    """
    logger.info("setting_up_database_tables")

    conn.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    conn.execute(f"DROP TABLE IF EXISTS {common.TABLE_NAME};")
    conn.execute(f"""
        CREATE TABLE {common.TABLE_NAME} (
            id bigserial PRIMARY KEY,
            content text,
            metadata jsonb,
            embedding vector({common.EMBEDDING_DIM})
        );
    """)

    logger.info("database_tables_setup_complete")


def prepare_records_vectorized(df, dataset_type):
    """
    Prepare records using vectorized operations where possible.

    This is more efficient than iterrows() but still requires
    per-row tuple creation for the COPY protocol.

    Args:
        df: DataFrame containing the dataset
        dataset_type: 'wot' or 'aerospace'

    Returns:
        list: List of tuples (content, metadata, embedding)
    """
    logger.info("preparing_records_vectorized", total_records=len(df))

    records = []

    if dataset_type == 'wot':
        # Prepare metadata as list comprehension (faster than iterrows)
        for i in range(len(df)):
            content = str(df.iloc[i]['text'])
            metadata = Jsonb({
                'book_name': str(df.iloc[i]['book_name']),
                'chapter_number': int(df.iloc[i]['chapter_number'])
                    if df.iloc[i]['chapter_number'] not in ['N/A', None, ''] else 0,
                'chapter_title': str(df.iloc[i]['chapter_title'])
            })
            embedding = df.iloc[i]['embedding']
            if isinstance(embedding, np.ndarray):
                embedding = embedding.astype(np.float32)

            records.append((content, metadata, embedding))
    else:
        # Aerospace dataset
        for i in range(len(df)):
            content = str(df.iloc[i].get('description', df.iloc[i].get('content', '')))
            metadata = Jsonb({
                'barcode': str(df.iloc[i].get('barcode', '')),
                'registration_code': str(df.iloc[i].get('registration_code', '')),
                'aircraft_type': str(df.iloc[i].get('aircraft_type', '')),
                'failed_system': str(df.iloc[i].get('failed_system', '')),
                'action_date': str(df.iloc[i].get('action_date', ''))
            })
            embedding = df.iloc[i]['embedding']
            if isinstance(embedding, np.ndarray):
                embedding = embedding.astype(np.float32)

            records.append((content, metadata, embedding))

    logger.info("records_prepared", count=len(records))
    return records


def run_copy_benchmark(conn, records, batch_size, dataset_type):
    """
    Run COPY benchmark for a specific batch size.

    Uses PostgreSQL's COPY command with binary format for maximum performance.

    Args:
        conn: psycopg3 connection with vector type registered
        records: List of prepared records
        batch_size: Number of records per COPY batch
        dataset_type: 'wot' or 'aerospace'
    """
    log = logger.bind(
        batch_size=batch_size,
        database="PostgreSQL (COPY)",
        dataset_type=dataset_type
    )
    log.info("starting_benchmark_run", method="COPY_BINARY")

    # Reset table
    setup_database(conn)

    # Ensure result tables exist in the results database
    pg_results_conn = common.results_db.get_results_db_connection()
    common.results_db.setup_results_tables(pg_results_conn)
    pg_results_conn.close()

    total_records = len(records)
    num_batches = (total_records + batch_size - 1) // batch_size

    log.info("starting_copy_insertion",
             total_records=total_records,
             num_batches=num_batches)

    batch_times = []
    granular_metrics = []

    start_time_total = time.time()
    cumulative_time = 0
    cumulative_vectors = 0

    for batch_num in range(num_batches):
        i = batch_num * batch_size
        batch = records[i:i + batch_size]
        current_batch_size = len(batch)

        batch_start = time.time()

        # Use COPY with binary format for maximum performance
        with conn.cursor() as cur:
            with cur.copy(
                f"COPY {common.TABLE_NAME} (content, metadata, embedding) "
                "FROM STDIN WITH (FORMAT BINARY)"
            ) as copy:
                # Set types for binary COPY - critical for vector type
                copy.set_types(['text', 'jsonb', 'vector'])

                for record in batch:
                    copy.write_row(record)

        batch_duration = time.time() - batch_start
        batch_times.append(batch_duration)

        cumulative_time += batch_duration
        cumulative_vectors += current_batch_size
        instant_throughput = current_batch_size / batch_duration if batch_duration > 0 else 0

        granular_metrics.append({
            "batch_number": batch_num + 1,
            "batch_time_seconds": batch_duration,
            "cumulative_time_seconds": cumulative_time,
            "cumulative_vectors_inserted": cumulative_vectors,
            "batch_size_actual": current_batch_size,
            "instantaneous_throughput": instant_throughput
        })

        log.info("batch_complete",
                 batch=batch_num + 1,
                 total_batches=num_batches,
                 vectors_inserted=cumulative_vectors,
                 throughput=round(instant_throughput, 2))

    end_time_total = time.time()
    total_time = end_time_total - start_time_total

    avg_throughput = total_records / total_time if total_time > 0 else 0
    log.info("benchmark_complete",
             duration_seconds=round(total_time, 2),
             avg_throughput=round(avg_throughput, 2))

    # Save Results to results database
    # Use a modified database name to distinguish from standard INSERT benchmark
    pg_results_conn = common.results_db.get_results_db_connection()
    common.results_db.save_insert_metrics(
        pg_results_conn,
        f"{common.get_database_name()} (COPY)",
        common.TEST_LOCATION,
        common.DATASET_SIZE,
        f"{common.DATABASE_CONFIG} | COPY BINARY",
        total_records,
        batch_size,
        batch_times,
        granular_metrics,
        total_time
    )
    pg_results_conn.close()


def run_copy_benchmark_chunked(batch_size: int, dataset_type: str) -> None:
    """
    Run COPY benchmark in chunked mode for a single batch_size.

    Streams the dataset from disk in chunks of INSERT_CHUNK_SIZE rows,
    prepares records per chunk, and inserts via COPY binary format.
    Saves a single aggregated metric at the end.

    Args:
        batch_size: Number of records per COPY batch.
        dataset_type: 'wot' or 'aerospace'.
    """
    log = logger.bind(
        task="chunked_copy_insert",
        batch_size=batch_size,
        chunk_size=INSERT_CHUNK_SIZE,
        database="PostgreSQL (COPY)",
    )
    log.info("chunked_copy_insert_started")

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

        # Prepare all records from this chunk — safe here because
        # prepare_records_vectorized keeps embeddings as numpy float32
        # arrays (~3KB each) rather than converting to Python lists (~21KB).
        # 100K records × 3KB ≈ 300MB, well within 8GB pod limit.
        records = prepare_records_vectorized(chunk_df, dataset_type)
        num_batches = (chunk_records + batch_size - 1) // batch_size

        for batch_num in range(num_batches):
            i = batch_num * batch_size
            batch = records[i:i + batch_size]
            current_batch_size = len(batch)

            batch_start = time.time()

            # Use COPY with binary format for maximum performance
            with conn.cursor() as cur:
                with cur.copy(
                    f"COPY {common.TABLE_NAME} (content, metadata, embedding) "
                    "FROM STDIN WITH (FORMAT BINARY)"
                ) as copy:
                    copy.set_types(['text', 'jsonb', 'vector'])
                    for record in batch:
                        copy.write_row(record)

            batch_duration = time.time() - batch_start
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

    avg_throughput = total_records / total_time if total_time > 0 else 0
    log.info(
        "chunked_copy_insert_complete",
        total_records=total_records,
        total_time_seconds=round(total_time, 2),
        avg_throughput=round(avg_throughput, 2),
    )

    # Save aggregated results to results database
    pg_results_conn = common.results_db.get_results_db_connection()
    common.results_db.save_insert_metrics(
        pg_results_conn,
        f"{common.get_database_name()} (COPY)",
        common.TEST_LOCATION,
        common.DATASET_SIZE,
        f"{common.DATABASE_CONFIG} | COPY BINARY",
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
    Main entry point for COPY-based insert benchmark.

    Supports two modes controlled by the INSERT_CHUNK_SIZE env var:
    - INSERT_CHUNK_SIZE == 0 (default): Load entire dataset into memory.
    - INSERT_CHUNK_SIZE > 0: Stream dataset from disk in chunks.
    """
    logger.info("=" * 80)
    logger.info("PostgreSQL pgvector Insert Benchmark - COPY Binary Format")
    logger.info("=" * 80)
    logger.info("NOTE: This benchmark uses COPY for maximum ingestion speed.")
    logger.info("For fair comparison with other databases, use 01_insert_benchmark.py")
    logger.info("=" * 80)

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
                run_copy_benchmark_chunked(batch_size, dataset_type)
            except Exception as e:
                logger.error("benchmark_failed", batch_size=batch_size, error=str(e))
                continue

        logger.info("benchmark_suite_complete")
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

        # Prepare records once (reused across all batch sizes)
        records = prepare_records_vectorized(df, dataset_type)

        conn = get_db_connection()
        logger.info("database_connected",
                    host=common.PG_HOST,
                    database=common.PG_DATABASE)

        logger.info("test_init",
                    database="Azure PostgreSQL (pgvector) - COPY BINARY",
                    batch_sizes=BATCH_SIZES)

        for batch_size in BATCH_SIZES:
            try:
                run_copy_benchmark(conn, records, batch_size, dataset_type)
            except Exception as e:
                logger.error("benchmark_failed", batch_size=batch_size, error=str(e))
                continue

        conn.close()
        logger.info("benchmark_suite_complete")


if __name__ == "__main__":
    main()
