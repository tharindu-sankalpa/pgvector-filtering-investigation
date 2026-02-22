"""
Dataset Loading Module

Provides functions for loading benchmark datasets and test queries.
Supports both legacy hardcoded paths and dynamic configuration via DatasetConfig.

Usage:
    # Legacy usage (hardcoded paths)
    df = load_dataset("data/historic_faults_with_embeddings.parquet")
    queries = load_test_queries("data/hybrid_test_queries.parquet")

    # Dynamic usage (with DatasetConfig)
    from shared.schema_registry import load_dataset_config
    config = load_dataset_config(Path("data/faults_dataset.yaml"))
    df = load_dataset_from_config(config)
    queries = load_test_queries_from_config(config)
"""

from pathlib import Path
from typing import Generator, Optional, Union

import pandas as pd
import pyarrow.parquet as pq

from shared.logger_structlog import setup_structlog

# Import DatasetConfig type for type hints
# Delayed import to avoid circular dependency
try:
    from shared.schema_registry import DatasetConfig
except ImportError:
    DatasetConfig = None  # type: ignore

logger = setup_structlog()


# =============================================================================
# Legacy Loading Functions (Backward Compatible)
# =============================================================================


def load_dataset(data_file_path: Union[Path, str]) -> pd.DataFrame:
    """
    Load the embeddings dataset from Parquet or CSV.

    This is the legacy loading function that works with hardcoded paths.
    For dynamic dataset loading, use load_dataset_from_config().

    Args:
        data_file_path: Path to the dataset file (Parquet or CSV).

    Returns:
        pd.DataFrame: Loaded dataset.

    Raises:
        FileNotFoundError: If file does not exist.
    """
    path = Path(data_file_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found at {path}")

    log = logger.bind(task="load_dataset", path=str(path))
    log.info("loading_dataset")

    if path.suffix == ".csv":
        df = pd.read_csv(path)
    else:
        df = pd.read_parquet(path)

    # Log dataset profile
    log.info(
        "dataset_loaded",
        records=len(df),
        columns=list(df.columns),
        memory_mb=round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
    )

    return df


def load_dataset_chunked(
    data_file_path: Union[Path, str],
    chunk_size: int = 100_000,
) -> Generator[pd.DataFrame, None, None]:
    """
    Yield DataFrames of chunk_size rows from a Parquet file.

    Uses PyArrow's ParquetFile.iter_batches() to stream data from disk
    without loading the entire file into memory. This is essential for
    datasets that exceed available RAM (e.g., 2.5M vectors at ~22GB).

    Each yielded DataFrame is an independent chunk that can be processed
    and then garbage-collected, keeping peak memory usage proportional
    to chunk_size rather than total dataset size.

    Args:
        data_file_path: Path to the Parquet dataset file.
        chunk_size: Number of rows per chunk. Defaults to 100,000.

    Yields:
        pd.DataFrame: A chunk of the dataset with chunk_size rows
            (last chunk may be smaller).

    Raises:
        FileNotFoundError: If the file does not exist at the given path.
    """
    path = Path(data_file_path)
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found at {path}")

    log = logger.bind(task="load_dataset_chunked", path=str(path), chunk_size=chunk_size)

    # Open the Parquet file via PyArrow for streaming reads
    pf = pq.ParquetFile(str(path))

    # Log file-level metadata before reading any data
    total_rows = pf.metadata.num_rows
    log.info(
        "chunked_loading_started",
        total_rows=total_rows,
        expected_chunks=(total_rows + chunk_size - 1) // chunk_size,
    )

    chunk_index = 0
    for batch in pf.iter_batches(batch_size=chunk_size):
        # Convert the Arrow RecordBatch to a pandas DataFrame
        chunk_df = batch.to_pandas()

        log.info(
            "chunk_yielded",
            chunk_index=chunk_index,
            rows=len(chunk_df),
            memory_mb=round(chunk_df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        )

        yield chunk_df
        chunk_index += 1

    log.info("chunked_loading_complete", total_chunks=chunk_index)


def load_test_queries(
    queries_file_path: Union[Path, str],
    limit: Optional[int] = None,
) -> list[dict]:
    """
    Load test queries from Parquet for consistent benchmarking.

    Supports both legacy aerospace format and new WoT format:
    - Aerospace: query_text, query_embedding (assigns rotating aircraft_type filters)
    - WoT: query_text, query_embedding, keywords, filter_field, filter_value

    Args:
        queries_file_path: Path to the queries parquet file.
        limit: Maximum number of queries to load.

    Returns:
        list[dict]: List of query dictionaries with keys:
            - embedding: Vector embedding for vector search
            - text: Original query text
            - keyword: Extracted keywords for hybrid search (joins list to string)
            - filters: Dict of {field: value} for filtered search (WoT format)
            - aircraft_type: Filter value for aerospace benchmarks (legacy)
            - failed_system: Filter value for aerospace benchmarks (legacy)
            - filter_value: Direct filter value (WoT format)
    """
    log = logger.bind(task="load_test_queries")

    path = Path(queries_file_path)
    if not path.exists():
        # Try looking up relative to script location
        repo_root = Path(__file__).parent.parent.parent
        alt_path = repo_root / queries_file_path
        if alt_path.exists():
            path = alt_path
        elif (repo_root / "data" / "hybrid_test_queries.parquet").exists():
            path = repo_root / "data" / "hybrid_test_queries.parquet"
        else:
            raise FileNotFoundError(f"Queries file not found at {path} or {alt_path}")

    log = log.bind(path=str(path))
    log.info("loading_test_queries")

    df = pd.read_parquet(path)

    # Sample if limit is provided
    if limit and limit < len(df):
        log.info("sampling_queries", limit=limit, total_available=len(df))
        df = df.sample(n=limit, random_state=42).reset_index(drop=True)

    # Detect dataset format based on columns
    has_wot_format = "keywords" in df.columns and "filter_field" in df.columns
    log.info("dataset_format_detected", wot_format=has_wot_format, columns=list(df.columns))

    # Map columns to expected keys
    queries = []

    # Default filter values for aerospace dataset
    aircraft_types = ["737-NG", "ERJ190"]
    failed_systems = [
        "21", "22", "23", "24", "25", "26", "27", "28", "29",
        "30", "31", "32", "33", "34", "35", "36", "38", "49",
        "52", "53", "54", "55", "56", "57", "71", "72", "73",
    ]

    for idx, row in df.iterrows():
        # Get embedding (required for all formats)
        embedding = row.get("query_embedding")
        if hasattr(embedding, "tolist"):
            embedding = embedding.tolist()

        # Get original query text
        text = row.get("query_text", "")

        if has_wot_format:
            # WoT format: Use extracted keywords for hybrid search
            keywords_list = row.get("keywords", [])
            if hasattr(keywords_list, "tolist"):
                keywords_list = keywords_list.tolist()

            # Join keywords into a search string for hybrid search
            # Use space-separated keywords for BM25/text search
            keyword_str = " ".join(keywords_list) if keywords_list else text

            # Build filters dict for filtered search
            filter_field = row.get("filter_field", "book_name")
            filter_value = row.get("filter_value", "unknown")
            filters = {filter_field: filter_value} if filter_value != "unknown" else {}

            queries.append({
                "embedding": embedding,
                "text": text,  # Original query (for reference/logging)
                "keyword": keyword_str,  # Extracted keywords joined for hybrid search
                "keywords_list": keywords_list,  # Raw keywords list
                "filters": filters,  # For filtered search
                "filter_value": filter_value,  # Direct access to filter value
                "filter_field": filter_field,  # Direct access to filter field
                # Legacy compatibility fields (use filter values)
                "aircraft_type": filter_value,  # Maps to book_name for filtered search
                "failed_system": "",  # Not applicable for WoT
                "query_type": row.get("query_type", "unknown"),
            })
        else:
            # Aerospace/legacy format: Assign rotating filter values
            aircraft_type = aircraft_types[idx % len(aircraft_types)]
            failed_system = failed_systems[idx % len(failed_systems)]

            queries.append({
                "embedding": embedding,
                "text": text,
                "keyword": text,  # Full text for hybrid search (aerospace)
                "filters": {"aircraft_type": aircraft_type},
                "filter_value": aircraft_type,
                "aircraft_type": aircraft_type,
                "failed_system": failed_system,
                "query_type": row.get("query_type", "unknown"),
            })

    log.info(
        "test_queries_loaded",
        count=len(queries),
        format="wot" if has_wot_format else "aerospace",
        sample_keywords=queries[0].get("keyword", "")[:50] if queries else "",
    )
    return queries


# =============================================================================
# Dynamic Loading Functions (DatasetConfig-based)
# =============================================================================


def load_dataset_from_config(config: "DatasetConfig") -> pd.DataFrame:
    """
    Load dataset using DatasetConfig.

    Args:
        config: Dataset configuration from schema_registry.

    Returns:
        pd.DataFrame: Loaded dataset.

    Raises:
        FileNotFoundError: If data file doesn't exist.
    """
    log = logger.bind(task="load_dataset_from_config", dataset=config.name)

    if not config.data_file.exists():
        log.error("data_file_not_found", path=str(config.data_file))
        raise FileNotFoundError(f"Dataset file not found: {config.data_file}")

    log.info("loading_dataset", path=str(config.data_file))
    df = pd.read_parquet(config.data_file)

    log.info(
        "dataset_loaded",
        records=len(df),
        columns=list(df.columns),
        memory_mb=round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
    )

    return df


def load_test_queries_from_config(
    config: "DatasetConfig",
    limit: Optional[int] = None,
) -> list[dict]:
    """
    Load test queries using DatasetConfig.

    Returns a standardized query format that works with any dataset:
    - embedding: Vector for similarity search
    - text: Text for hybrid/BM25 search
    - filters: Dict of {field: value} for filtered search

    Args:
        config: Dataset configuration from schema_registry.
        limit: Maximum number of queries to load.

    Returns:
        list[dict]: List of query dictionaries with standardized keys.

    Raises:
        FileNotFoundError: If query file doesn't exist.
    """
    log = logger.bind(task="load_test_queries_from_config", dataset=config.name)

    if not config.query_file.exists():
        log.error("query_file_not_found", path=str(config.query_file))
        raise FileNotFoundError(f"Query file not found: {config.query_file}")

    log.info("loading_test_queries", path=str(config.query_file))
    df = pd.read_parquet(config.query_file)

    # Sample if limit is provided
    if limit and limit < len(df):
        log.info("sampling_queries", limit=limit, total_available=len(df))
        df = df.sample(n=limit, random_state=42).reset_index(drop=True)

    queries = []
    for _, row in df.iterrows():
        # Get embedding from configured column
        embedding = row.get(config.query_embedding_column)
        if hasattr(embedding, "tolist"):
            embedding = embedding.tolist()

        # Get text from configured column
        text = row.get(config.query_text_column, "")

        # Build filters dict from available filter columns
        filters = {}
        if "filter_field" in df.columns and "filter_value" in df.columns:
            # WoT dataset format: filter_field + filter_value columns
            field = row.get("filter_field")
            value = row.get("filter_value")
            if field and value:
                filters[field] = value
        else:
            # Check for direct filter column values
            for col in config.filter_columns:
                if col in df.columns:
                    value = row.get(col)
                    if pd.notna(value):
                        filters[col] = value

        # Build query dict
        query = {
            "embedding": embedding,
            "text": text,
            "keyword": text,  # Alias for legacy compatibility
            "filters": filters,
        }

        # Add any extra metadata columns
        if "keywords" in df.columns:
            keywords = row.get("keywords")
            if hasattr(keywords, "tolist"):
                keywords = keywords.tolist()
            query["keywords"] = keywords

        if "source_qa_id" in df.columns:
            query["source_qa_id"] = row.get("source_qa_id")

        if "query_type" in df.columns:
            query["query_type"] = row.get("query_type", "unknown")

        queries.append(query)

    log.info(
        "test_queries_loaded",
        count=len(queries),
        has_filters=any(q.get("filters") for q in queries),
    )

    return queries


def get_filter_expression_for_query(
    query: dict,
    database_type: str,
    default_filter_column: Optional[str] = None,
    default_filter_value: Optional[str] = None,
) -> Optional[str]:
    """
    Generate a database-specific filter expression from a query.

    Converts the generic filters dict to database-specific syntax.

    Args:
        query: Query dictionary with optional 'filters' key.
        database_type: One of 'milvus', 'mongodb', 'pgvector', 'azure_ai_search'.
        default_filter_column: Default column to filter on if no filters in query.
        default_filter_value: Default value to filter on if no filters in query.

    Returns:
        str: Database-specific filter expression, or None if no filters.
    """
    filters = query.get("filters", {})

    # Use defaults if no filters present
    if not filters and default_filter_column and default_filter_value:
        filters = {default_filter_column: default_filter_value}

    if not filters:
        return None

    # Get the first filter (most common case)
    field, value = next(iter(filters.items()))

    if database_type == "milvus":
        # Milvus expression syntax
        return f'{field} == "{value}"'

    elif database_type == "mongodb":
        # MongoDB filter syntax (for aggregation pipeline)
        return {field: value}

    elif database_type == "pgvector":
        # PostgreSQL WHERE clause
        return f"metadata->>'{field}' = '{value}'"

    elif database_type == "azure_ai_search":
        # Azure AI Search OData filter
        return f"{field} eq '{value}'"

    else:
        # Generic fallback
        return f'{field}="{value}"'
