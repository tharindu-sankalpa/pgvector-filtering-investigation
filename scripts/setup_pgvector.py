# Dependencies:
# pip install pandas structlog pyarrow asyncpg pgvector python-dotenv numpy

import structlog
import logging
import sys
import os
import asyncio
import numpy as np
import pandas as pd
import asyncpg
from pathlib import Path
from dotenv import load_dotenv
from pgvector.asyncpg import register_vector
from typing import List, Dict, Any

import time
from tqdm import tqdm

# --- Structlog Configuration (Scenario A: Standalone Script) ---
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(colors=True),
    ],
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger()
# ---------------------------------------------------------------

# Load environment variables from .env.azure
load_dotenv('.env.azure')

async def main() -> None:
    """
    Main execution function for the pgvector test script.
    Loads data, inserts into Postgres, and performs nearest neighbor searches.
    """
    log = logger.bind(script="pgvector_test")
    
    # Database connection parameters from environment variables
    pg_host = os.getenv('PGHOST')
    pg_user = os.getenv('PGUSER')
    pg_pass = os.getenv('PGPASSWORD')
    pg_db = os.getenv('PGDATABASE')
    
    if not all([pg_host, pg_user, pg_pass, pg_db]):
        log.error("missing_env_vars", 
                  help="Ensure PGHOST, PGUSER, PGPASSWORD, PGDATABASE are set in .env.azure")
        sys.exit(1)

    conn = None
    try:
        # --- 1. Load Data ---
        script_dir = Path(__file__).parent
        data_path = script_dir.parent / 'data' / 'wot_chunks_with_embeddings_100pct.parquet'
        
        log.info("loading_data", file_path=str(data_path))
        df = pd.read_parquet(data_path)
        
        # Use full dataset
        df_subset = df
        
        # Get embedding dimension from first row
        first_embedding = np.array(df_subset['embedding'].iloc[0])
        embedding_dim = len(first_embedding)
        
        log.info("data_loaded", 
                 total_rows=len(df), 
                 subset_rows=len(df_subset), 
                 embedding_dim=embedding_dim)

        # --- 2. Connect to PostgreSQL ---
        log.info("connecting_to_postgres", host=pg_host, db=pg_db)
        conn = await asyncpg.connect(
            host=pg_host,
            user=pg_user,
            password=pg_pass,
            database=pg_db
        )
        
        # Enable pgvector and register type
        await conn.execute('CREATE EXTENSION IF NOT EXISTS vector')
        await register_vector(conn)
        
        # --- 3. Check Table Existence ---
        table_name = 'wot_chunks_full'
        log.info("checking_table_existence", table=table_name)
        
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE  table_schema = 'public'
                AND    table_name   = $1
            );
        """, table_name)

        if table_exists:
            log.info("table_exists_skipping_ingestion", table=table_name)
        else:
            # --- 3. Setup Schema (Only if table doesn't exist) ---
            log.info("setting_up_schema", table=table_name)
            # await conn.execute('DROP TABLE IF EXISTS wot_chunks_1k') # Removed since we check existence now
            await conn.execute(f'''
                CREATE TABLE {table_name} (
                    id SERIAL PRIMARY KEY,
                    text TEXT,
                    book_name TEXT,
                    chapter_number TEXT,
                    chapter_title TEXT,
                    char_count INTEGER,
                    embedding vector({embedding_dim})
                )
            ''')
            
            # --- 4. Insert Data ---
            log.info("starting_ingestion", count=len(df_subset))
            
            # Prepare batch for insertion
            records = []
            for _, row in df_subset.iterrows():
                records.append((
                    row['text'],
                    row['book_name'],
                    str(row['chapter_number']), # Ensure string matches schema
                    row['chapter_title'],
                    row['char_count'],
                    np.array(row['embedding'])
                ))
                
            # Bulk insert is more efficient
            await conn.copy_records_to_table(
                table_name,
                records=records,
                columns=['text', 'book_name', 'chapter_number', 'chapter_title', 'char_count', 'embedding']
            )
            
            log.info("ingestion_completed", count=len(records))

            # --- 5. Create Index ---
            log.info("creating_index", type="hnsw")
            await conn.execute(f'CREATE INDEX ON {table_name} USING hnsw (embedding vector_cosine_ops)')

        
        # --- 6. Random Search Test ---
        sample_size = 1000
        # If subset is smaller than sample size, take all
        n_samples = min(sample_size, len(df_subset))
        
        log.info("starting_random_search_test", sample_size=n_samples)
        
        random_subset = df_subset.sample(n=n_samples)
        
        results_summary = []
        latencies_ms = []

        start_time_total = time.perf_counter()
        
        # Use tqdm for progress visualization
        for idx, row in tqdm(random_subset.iterrows(), total=n_samples, desc="Running NNS Queries"):
            query_embedding = np.array(row['embedding'])
            
            start_time_query = time.perf_counter()
            # Find 5 nearest neighbors (excluding self if exact match, but here we just want neighbors)
            # Using <=> for cosine distance
            rows = await conn.fetch(f'''
                SELECT id, text, book_name, chapter_title, 
                       embedding <=> $1 AS distance
                FROM {table_name}
                ORDER BY embedding <=> $1
                LIMIT 5
            ''', query_embedding)
            end_time_query = time.perf_counter()

            # Calculate latency in milliseconds
            latencies_ms.append((end_time_query - start_time_query) * 1000)
            
            # Log individual query results? Might be too verbose for 100 queries.
            # Let's aggregate stats or log a few examples.
            # We'll log the top match for each to keep it somewhat concise but informative.
            
            top_match = rows[0]
            results_summary.append({
                "query_id": idx,
                "top_match_book": top_match['book_name'],
                "top_match_dist": float(top_match['distance'])
            })

        end_time_total = time.perf_counter()
        total_duration = end_time_total - start_time_total

        # --- Metrics Calculation ---
        qps = n_samples / total_duration
        p50 = np.percentile(latencies_ms, 50)
        p95 = np.percentile(latencies_ms, 95)
        p99 = np.percentile(latencies_ms, 99)
        avg_lat = np.mean(latencies_ms)
        min_lat = np.min(latencies_ms)
        max_lat = np.max(latencies_ms)

        # Log comprehensive benchmark results
        log.info("benchmark_results", 
                 total_queries_executed=len(latencies_ms),
                 total_duration_sec=round(total_duration, 4),
                 qps=round(qps, 2),
                 latency_min_ms=round(min_lat, 2),
                 latency_max_ms=round(max_lat, 2),
                 latency_avg_ms=round(avg_lat, 2),
                 latency_p50_ms=round(p50, 2),
                 latency_p95_ms=round(p95, 2),
                 latency_p99_ms=round(p99, 2))

        # Explicit verification that 100 queries were sent
        if len(latencies_ms) == n_samples:
            log.info("verification_success", 
                     message=f"Confirmed {len(latencies_ms)} sequential query requests were sent to the DB.")
        else:
             log.error("verification_failed", 
                     expected=n_samples, 
                     actual=len(latencies_ms))

        # Log summary stats for accuracy/quality
        avg_dist = np.mean([r['top_match_dist'] for r in results_summary])
        log.info("search_quality_stats", 
                 avg_top_match_distance=round(avg_dist, 5),
                 queries_run=len(results_summary))
        
        # Log a few examples fully
        for i in range(min(3, len(results_summary))):
            log.info("sample_search_result", 
                     query_index=i, 
                     result=results_summary[i])

    except Exception as e:
        log.exception("script_failed", error=str(e))
        raise
        
    finally:
        if conn:
            await conn.close()
            log.info("connection_closed")

if __name__ == '__main__':
    asyncio.run(main())
