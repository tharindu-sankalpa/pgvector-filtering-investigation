"""
PGVector Common Configuration Module

Provides connection management, SQL query templates, and configuration for
PostgreSQL pgvector benchmarks. Supports both static (hardcoded) schemas
and dynamic (DatasetConfig-based) query generation.

Shared configuration for all PostgreSQL client drivers:
- asyncpg
- psycopg2 (synchronous)
- psycopg3 (synchronous)
- psycopg3 (asynchronous)

Environment Variables:
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD: Database connection
    VECTOR_PG_DATABASE: Vector data database name
    PG_TABLE_NAME: Table name for embeddings
    DATASET_CONFIG: Path to dataset YAML config (for dynamic schema)
"""

import os
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Add src directory to path to import shared modules
sys.path.append(str(Path(__file__).parent.parent))
from shared import common_utils, results_db
from shared.logger_structlog import setup_structlog

# Initialize structlog
logger = setup_structlog()

# Load environment variables
ENV_PATH = Path(__file__).parent.parent.parent / ".env.azure"
load_dotenv(ENV_PATH)


# =============================================================================
# Database Connection Configuration
# =============================================================================

PG_HOST = os.getenv("PG_HOST")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("VECTOR_PG_DATABASE", os.getenv("PG_DATABASE", "vector_benchmark"))
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Table and schema configuration
# Support both TABLE_NAME (generic) and PG_TABLE_NAME (pgvector-specific)
TABLE_NAME = os.getenv("TABLE_NAME", os.getenv("PG_TABLE_NAME", "embeddings"))
EMBEDDING_DIM = 1536


# =============================================================================
# Benchmark Configuration (Consistent across all vector databases)
# =============================================================================

# Concurrency levels - same as other vector database benchmarks
CONCURRENCY_LEVELS = [
    int(c) for c in os.getenv("CONCURRENCY_LEVELS", "1,2,4,8,16,32,50,100").split(",")
]

# Top-K values - same as other vector database benchmarks
TOP_K_VALUES = [int(k) for k in os.getenv("TOP_K_VALUES", "1,5,10,20,50,100").split(",")]

# Number of queries per scenario
NUM_QUERIES = int(os.getenv("BENCHMARK_NUM_QUERIES", common_utils.NUM_QUERIES))

# Query file path
QUERY_FILE = os.getenv("QUERY_FILE", "data/hybrid_test_queries.parquet")


# =============================================================================
# HNSW ef_search Configuration (Fair Benchmarking)
# =============================================================================
# pgvector defaults to hnsw.ef_search=40 which silently clips results when
# LIMIT > ~67.  Milvus uses ef=max(64, top_k) per query, ensuring ef >= top_k.
# To make benchmarks comparable, we mirror Milvus's approach.

HNSW_EF_SEARCH_MIN = int(os.getenv("HNSW_EF_SEARCH_MIN", "64"))


def compute_hnsw_ef_search(top_k: int) -> int:
    """
    Compute the HNSW ef_search value that should be SET before a pgvector query.

    Mirrors the Milvus convention: ef = max(HNSW_EF_SEARCH_MIN, top_k).
    This guarantees the HNSW candidate list is always large enough to return
    the requested number of results without silent clipping.

    Args:
        top_k: The LIMIT / number of nearest neighbours requested.

    Returns:
        int: The ef_search value to use.
    """
    return max(HNSW_EF_SEARCH_MIN, top_k)


# HNSW iterative_scan mode for filtered search (pgvector 0.8.x+).
#
# The core problem: without iterative_scan, the planner explores exactly
# ef_search candidates from the HNSW graph and discards those that fail the
# WHERE predicate.  If fewer than top_k candidates survive, the result set is
# silently short — at top_k=37, ef_search=64 a dense book can yield only 6 rows.
#
# strict_order: expand the graph in batches until top_k filter-passing results
#               are found (or the graph is exhausted).  Results returned in exact
#               distance order — same ranking semantics as a sequential scan.
# relaxed_order: same iterative expansion but results may be returned slightly
#                out of order (faster, acceptable for ANN workloads).
# off: disabled — default pgvector behaviour, may return fewer than top_k results.
#
# Scope: SET only inside execute_filtered_search; RESET immediately after.
# Vector/hybrid search functions are unaffected — iterative expansion is a no-op
# on unfiltered queries and would add overhead without benefit.
HNSW_ITERATIVE_SCAN = os.getenv("HNSW_ITERATIVE_SCAN", "strict_order")


# =============================================================================
# Pool Configuration
# =============================================================================

# Connection pool sizing: max_concurrency + buffer for optimal performance
POOL_BUFFER = int(os.getenv("POOL_BUFFER", "10"))
MAX_CONCURRENCY = max(CONCURRENCY_LEVELS)
POOL_MIN_SIZE = int(os.getenv("POOL_MIN_SIZE", "10"))
POOL_MAX_SIZE = int(os.getenv("POOL_MAX_SIZE", str(MAX_CONCURRENCY + POOL_BUFFER)))


# =============================================================================
# Test Metadata Configuration
# =============================================================================

TEST_LOCATION = os.getenv("TEST_LOCATION", common_utils.LOC_LOCAL_PC)
DATASET_SIZE = common_utils.get_env_int("DATASET_SIZE", 35048)
DATABASE_CONFIG = os.getenv("DATABASE_CONFIG", "Default")


# =============================================================================
# Helper Functions
# =============================================================================


def get_database_name() -> str:
    """
    Get the database name for reporting, with optional override.

    Returns:
        str: Database name for benchmark results.
    """
    override_name = os.getenv("DATABASE_NAME_OVERRIDE")
    if override_name:
        return override_name
    return common_utils.DB_PGVECTOR_AZURE


def get_connection_string() -> str:
    """
    Build PostgreSQL connection string for psycopg3.

    Returns:
        str: Connection string.
    """
    return (
        f"host={PG_HOST} "
        f"port={PG_PORT} "
        f"dbname={PG_DATABASE} "
        f"user={PG_USER} "
        f"password={PG_PASSWORD}"
    )


def get_vector_db_connection_string() -> str:
    """
    Build PostgreSQL connection string for vector database.

    Uses VECTOR_PG_* environment variables if set.

    Returns:
        str: Connection string for vector database.
    """
    host = os.getenv("VECTOR_PG_HOST", PG_HOST)
    port = os.getenv("VECTOR_PG_PORT", str(PG_PORT))
    database = os.getenv("VECTOR_PG_DATABASE", "vector_data")
    user = os.getenv("VECTOR_PG_USER", PG_USER)
    password = os.getenv("VECTOR_PG_PASSWORD", PG_PASSWORD)

    return f"host={host} port={port} dbname={database} user={user} password={password}"


def validate_config() -> bool:
    """
    Validate that all required configuration is present.

    Returns:
        bool: True if all required variables are set.
    """
    required = [PG_HOST, PG_USER, PG_PASSWORD, PG_DATABASE]
    if not all(required):
        logger.error(
            "missing_env_vars",
            help="Ensure PG_HOST, PG_USER, PG_PASSWORD, PG_DATABASE are set",
        )
        return False
    return True


def get_db_connection():
    """
    Get a psycopg2 database connection for the vector database.

    This is used by index creation and other scripts that require
    a psycopg2 connection (not psycopg3).

    Returns:
        psycopg2 connection object
    """
    import psycopg2

    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )


# =============================================================================
# SQL Queries - Unified Schema (Production Use)
# =============================================================================
# The benchmark uses a unified schema for all datasets:
#   - id: bigserial PRIMARY KEY
#   - content: text (stores description for aerospace, text for WoT)
#   - metadata: jsonb (stores filter fields like aircraft_type, book_name)
#   - embedding: vector(1536)
#
# This unified schema allows the same queries to work with any dataset.
# Filter fields are accessed via: metadata->>'field_name'


def get_vector_search_query(table_name: str = TABLE_NAME) -> str:
    """
    Pure vector search query - consistent with other vector database benchmarks.

    Returns id and similarity score.
    """
    return f"""
        SELECT id, 1 - (embedding <=> %s) AS similarity
        FROM {table_name}
        ORDER BY embedding <=> %s
        LIMIT %s
    """


def get_filtered_search_query(
    table_name: str = TABLE_NAME,
    filter_field: str = "aircraft_type"
) -> str:
    """
    Filtered vector search query - filter by specified metadata field.

    Supports both aerospace (aircraft_type) and WoT (book_name) datasets.
    Consistent with Milvus and MongoDB filtered search patterns.

    Args:
        table_name: Table to query.
        filter_field: Metadata field to filter on (default: aircraft_type for aerospace).

    Returns:
        str: SQL query with %s placeholders for psycopg.
    """
    return f"""
        SELECT id, 1 - (embedding <=> %s) AS similarity
        FROM {table_name}
        WHERE metadata->>'{filter_field}' = %s
        ORDER BY embedding <=> %s
        LIMIT %s
    """


def get_hybrid_search_query(table_name: str = TABLE_NAME) -> str:
    """
    Hybrid search query using RRF (Reciprocal Rank Fusion).

    Combines vector similarity with full-text search, consistent with
    other vector database hybrid search implementations.
    Uses k=60 for RRF scoring, same as MongoDB Atlas.
    """
    return f"""
        WITH vector_search AS (
            SELECT id,
                   ROW_NUMBER() OVER (ORDER BY embedding <=> %s) as rank,
                   1 - (embedding <=> %s) as vector_similarity
            FROM {table_name}
            LIMIT %s
        ),
        text_search AS (
            SELECT id,
                   ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                                       plainto_tsquery('english', %s)) DESC) as rank,
                   ts_rank(to_tsvector('english', content), plainto_tsquery('english', %s)) as text_score
            FROM {table_name}
            WHERE to_tsvector('english', content) @@ plainto_tsquery('english', %s)
            LIMIT %s
        ),
        rrf_scores AS (
            SELECT
                COALESCE(v.id, t.id) as id,
                (COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0)) as rrf_score,
                v.vector_similarity,
                t.text_score
            FROM vector_search v
            FULL OUTER JOIN text_search t ON v.id = t.id
        )
        SELECT id, rrf_score as similarity
        FROM rrf_scores
        ORDER BY rrf_score DESC
        LIMIT %s
    """


# =============================================================================
# AsyncPG-specific Queries (Uses $1 parameterization, LIMIT embedded)
# =============================================================================
# NOTE: LIMIT is embedded directly in SQL (not parameterized) because:
# 1. PostgreSQL's query planner optimizes differently based on LIMIT value
# 2. asyncpg's statement caching works better with explicit LIMIT values
# 3. This matches the original benchmark scripts behavior


def get_vector_search_query_asyncpg(top_k: int, table_name: str = TABLE_NAME) -> str:
    """Pure vector search query for asyncpg (uses $1 syntax, LIMIT embedded)."""
    return f"""
        SELECT id, 1 - (embedding <=> $1) AS similarity
        FROM {table_name}
        ORDER BY embedding <=> $1
        LIMIT {top_k}
    """


def get_filtered_search_query_asyncpg(
    top_k: int,
    table_name: str = TABLE_NAME,
    filter_field: str = "aircraft_type"
) -> str:
    """
    Filtered vector search query for asyncpg (LIMIT embedded).

    Supports both aerospace (aircraft_type) and WoT (book_name) datasets.

    Args:
        top_k: Number of results to return (embedded in query).
        table_name: Table to query.
        filter_field: Metadata field to filter on.

    Returns:
        str: SQL query with $N placeholders for asyncpg.
    """
    return f"""
        SELECT id, 1 - (embedding <=> $1) AS similarity
        FROM {table_name}
        WHERE metadata->>'{filter_field}' = $2
        ORDER BY embedding <=> $1
        LIMIT {top_k}
    """


def get_hybrid_search_query_asyncpg(top_k: int, table_name: str = TABLE_NAME) -> str:
    """Hybrid search query for asyncpg (LIMIT embedded)."""
    vector_limit = top_k * 2  # Retrieve more candidates for RRF
    return f"""
        WITH vector_search AS (
            SELECT id,
                   ROW_NUMBER() OVER (ORDER BY embedding <=> $1) as rank,
                   1 - (embedding <=> $1) as vector_similarity
            FROM {table_name}
            LIMIT {vector_limit}
        ),
        text_search AS (
            SELECT id,
                   ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                                       plainto_tsquery('english', $2)) DESC) as rank,
                   ts_rank(to_tsvector('english', content), plainto_tsquery('english', $2)) as text_score
            FROM {table_name}
            WHERE to_tsvector('english', content) @@ plainto_tsquery('english', $2)
            LIMIT {vector_limit}
        ),
        rrf_scores AS (
            SELECT
                COALESCE(v.id, t.id) as id,
                (COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0)) as rrf_score,
                v.vector_similarity,
                t.text_score
            FROM vector_search v
            FULL OUTER JOIN text_search t ON v.id = t.id
        )
        SELECT id, rrf_score as similarity
        FROM rrf_scores
        ORDER BY rrf_score DESC
        LIMIT {top_k}
    """


# =============================================================================
# Dynamic Schema Support (DatasetConfig-based) - For Reference/Future Use
# =============================================================================
# NOTE: These functions generate SQL for a schema with direct columns for each
# field (instead of JSONB metadata). This is provided as an alternative approach
# but is NOT used by the current benchmark scripts.
#
# Current benchmarks use the unified schema (content + metadata JSONB) defined
# in setup_database() in 01_insert_benchmark.py.


def get_create_table_sql(config, embedding_dim: int = 1536) -> str:
    """
    Generate CREATE TABLE SQL from dataset configuration.

    Args:
        config: DatasetConfig from schema_registry.
        embedding_dim: Dimension of embedding vectors.

    Returns:
        str: CREATE TABLE SQL statement.
    """
    table_name = config.metadata.get("pg_table_name", config.name.replace("-", "_"))

    columns = ["id SERIAL PRIMARY KEY"]

    # Add ID column if specified (as separate column, not primary key)
    if config.id_column:
        columns.append(f"{config.id_column} VARCHAR(100)")

    # Add text column
    columns.append(f"{config.text_column} TEXT")

    # Add filter columns
    for col in config.filter_columns:
        columns.append(f"{col} VARCHAR(500)")

    # Add embedding column
    columns.append(f"{config.embedding_column} vector({embedding_dim})")

    # Add metadata JSONB column for flexibility
    columns.append("metadata JSONB")

    sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(columns)}
        );
    """

    return sql


def get_create_index_sql(
    config,
    index_type: str = "hnsw",
    m: int = 16,
    ef_construction: int = 64,
) -> str:
    """
    Generate CREATE INDEX SQL for vector column.

    Args:
        config: DatasetConfig from schema_registry.
        index_type: Index type ("hnsw" or "ivfflat").
        m: HNSW M parameter.
        ef_construction: HNSW ef_construction parameter.

    Returns:
        str: CREATE INDEX SQL statement.
    """
    table_name = config.metadata.get("pg_table_name", config.name.replace("-", "_"))
    index_name = f"{table_name}_{config.embedding_column}_idx"

    if index_type.lower() == "hnsw":
        return f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table_name}
            USING hnsw ({config.embedding_column} vector_cosine_ops)
            WITH (m = {m}, ef_construction = {ef_construction});
        """
    elif index_type.lower() == "ivfflat":
        lists = 100  # Default number of lists
        return f"""
            CREATE INDEX IF NOT EXISTS {index_name}
            ON {table_name}
            USING ivfflat ({config.embedding_column} vector_cosine_ops)
            WITH (lists = {lists});
        """
    else:
        raise ValueError(f"Unknown index type: {index_type}")


def get_vector_search_query_dynamic(
    config,
    paramstyle: str = "psycopg",
) -> str:
    """
    Generate vector search query from dataset configuration.

    Args:
        config: DatasetConfig from schema_registry.
        paramstyle: "psycopg" for %s, "asyncpg" for $1.

    Returns:
        str: SQL query string.
    """
    table_name = config.metadata.get("pg_table_name", config.name.replace("-", "_"))
    emb_col = config.embedding_column

    if paramstyle == "asyncpg":
        return f"""
            SELECT id, 1 - ({emb_col} <=> $1) AS similarity
            FROM {table_name}
            ORDER BY {emb_col} <=> $1
            LIMIT $2
        """
    else:
        return f"""
            SELECT id, 1 - ({emb_col} <=> %s) AS similarity
            FROM {table_name}
            ORDER BY {emb_col} <=> %s
            LIMIT %s
        """


def get_filtered_search_query_dynamic(
    config,
    filter_column: str,
    paramstyle: str = "psycopg",
) -> str:
    """
    Generate filtered vector search query from dataset configuration.

    Args:
        config: DatasetConfig from schema_registry.
        filter_column: Column to filter on.
        paramstyle: "psycopg" for %s, "asyncpg" for $1.

    Returns:
        str: SQL query string.
    """
    table_name = config.metadata.get("pg_table_name", config.name.replace("-", "_"))
    emb_col = config.embedding_column

    if paramstyle == "asyncpg":
        return f"""
            SELECT id, 1 - ({emb_col} <=> $1) AS similarity
            FROM {table_name}
            WHERE {filter_column} = $2
            ORDER BY {emb_col} <=> $1
            LIMIT $3
        """
    else:
        return f"""
            SELECT id, 1 - ({emb_col} <=> %s) AS similarity
            FROM {table_name}
            WHERE {filter_column} = %s
            ORDER BY {emb_col} <=> %s
            LIMIT %s
        """


def get_hybrid_search_query_dynamic(
    config,
    paramstyle: str = "psycopg",
) -> str:
    """
    Generate hybrid search query from dataset configuration.

    Args:
        config: DatasetConfig from schema_registry.
        paramstyle: "psycopg" for %s, "asyncpg" for $1.

    Returns:
        str: SQL query string with RRF scoring.
    """
    table_name = config.metadata.get("pg_table_name", config.name.replace("-", "_"))
    emb_col = config.embedding_column
    text_col = config.text_column

    if paramstyle == "asyncpg":
        return f"""
            WITH vector_search AS (
                SELECT id,
                       ROW_NUMBER() OVER (ORDER BY {emb_col} <=> $1) as rank,
                       1 - ({emb_col} <=> $1) as vector_similarity
                FROM {table_name}
                LIMIT $3
            ),
            text_search AS (
                SELECT id,
                       ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', {text_col}),
                                                           plainto_tsquery('english', $2)) DESC) as rank,
                       ts_rank(to_tsvector('english', {text_col}), plainto_tsquery('english', $2)) as text_score
                FROM {table_name}
                WHERE to_tsvector('english', {text_col}) @@ plainto_tsquery('english', $2)
                LIMIT $3
            ),
            rrf_scores AS (
                SELECT
                    COALESCE(v.id, t.id) as id,
                    (COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0)) as rrf_score
                FROM vector_search v
                FULL OUTER JOIN text_search t ON v.id = t.id
            )
            SELECT id, rrf_score as similarity
            FROM rrf_scores
            ORDER BY rrf_score DESC
            LIMIT $4
        """
    else:
        return f"""
            WITH vector_search AS (
                SELECT id,
                       ROW_NUMBER() OVER (ORDER BY {emb_col} <=> %s) as rank,
                       1 - ({emb_col} <=> %s) as vector_similarity
                FROM {table_name}
                LIMIT %s
            ),
            text_search AS (
                SELECT id,
                       ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', {text_col}),
                                                           plainto_tsquery('english', %s)) DESC) as rank,
                       ts_rank(to_tsvector('english', {text_col}), plainto_tsquery('english', %s)) as text_score
                FROM {table_name}
                WHERE to_tsvector('english', {text_col}) @@ plainto_tsquery('english', %s)
                LIMIT %s
            ),
            rrf_scores AS (
                SELECT
                    COALESCE(v.id, t.id) as id,
                    (COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0)) as rrf_score
                FROM vector_search v
                FULL OUTER JOIN text_search t ON v.id = t.id
            )
            SELECT id, rrf_score as similarity
            FROM rrf_scores
            ORDER BY rrf_score DESC
            LIMIT %s
        """


def get_insert_sql(config) -> str:
    """
    Generate INSERT SQL from dataset configuration.

    Args:
        config: DatasetConfig from schema_registry.

    Returns:
        str: INSERT SQL statement with placeholders.
    """
    table_name = config.metadata.get("pg_table_name", config.name.replace("-", "_"))

    columns = []
    placeholders = []
    idx = 1

    if config.id_column:
        columns.append(config.id_column)
        placeholders.append(f"${idx}")
        idx += 1

    columns.append(config.text_column)
    placeholders.append(f"${idx}")
    idx += 1

    for col in config.filter_columns:
        columns.append(col)
        placeholders.append(f"${idx}")
        idx += 1

    columns.append(config.embedding_column)
    placeholders.append(f"${idx}")

    return f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({', '.join(placeholders)})
    """
