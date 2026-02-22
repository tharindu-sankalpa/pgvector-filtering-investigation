"""
Schema Registry Module

Provides dynamic schema configuration for benchmark datasets. This module enables
data-driven schema definitions rather than hardcoded column names, supporting
multiple datasets with different structures.

Key Components:
    - DatasetConfig: Dataclass representing dataset configuration
    - load_dataset_config: Load configuration from YAML file
    - infer_schema_from_parquet: Auto-infer column types from Parquet metadata

Usage:
    from shared.schema_registry import load_dataset_config

    # Load configuration
    config = load_dataset_config(Path("data/wot_dataset.yaml"))

    # Access configuration
    print(config.embedding_column)  # "embedding"
    print(config.filter_columns)    # ["book_name", "chapter_title"]

Environment Variables:
    DATASET_CONFIG: Path to dataset YAML configuration file
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import pyarrow.parquet as pq
import yaml

from shared.logger_structlog import setup_structlog

logger = setup_structlog()


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DatasetConfig:
    """
    Configuration for a benchmark dataset.

    Describes the structure of a dataset including paths to data files,
    column mappings, and filter configurations. This enables benchmark
    scripts to work with different datasets without code changes.

    Attributes:
        name: Dataset identifier (e.g., "wheel_of_time", "aerospace_faults").
        data_file: Path to the data Parquet file containing vectors.
        query_file: Path to the query Parquet file for benchmarks.
        embedding_column: Column name containing vector embeddings.
        text_column: Column name for full-text/BM25 search.
        filter_columns: List of columns available for filtered search.
        id_column: Primary key column (optional, auto-generated if None).
        query_embedding_column: Column in query file containing embeddings
            (defaults to "query_embedding").
        query_text_column: Column in query file containing text
            (defaults to "query_text").
        metadata: Additional metadata as key-value pairs.
    """

    name: str
    data_file: Path
    query_file: Path
    embedding_column: str
    text_column: str
    filter_columns: list[str] = field(default_factory=list)
    id_column: Optional[str] = None
    query_embedding_column: str = "query_embedding"
    query_text_column: str = "query_text"
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_data_columns(self) -> list[str]:
        """
        Get list of all data columns (excluding embedding).

        Returns:
            list[str]: Column names for non-embedding data.
        """
        columns = [self.text_column] + self.filter_columns
        if self.id_column:
            columns.insert(0, self.id_column)
        return columns

    def get_insert_columns(self) -> list[str]:
        """
        Get list of columns needed for insert operations.

        Returns:
            list[str]: Column names including embedding.
        """
        return self.get_data_columns() + [self.embedding_column]

    def __post_init__(self) -> None:
        """Convert string paths to Path objects."""
        if isinstance(self.data_file, str):
            self.data_file = Path(self.data_file)
        if isinstance(self.query_file, str):
            self.query_file = Path(self.query_file)


# =============================================================================
# Configuration Loading Functions
# =============================================================================


def load_dataset_config(config_path: Path) -> DatasetConfig:
    """
    Load dataset configuration from a YAML file.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        DatasetConfig: Loaded and validated configuration.

    Raises:
        FileNotFoundError: If configuration file doesn't exist.
        ValueError: If required fields are missing.

    Example YAML format:
        name: wheel_of_time
        data_file: data/wot_chunks_with_embeddings_100pct.parquet
        query_file: data/wot_retrieval_queries.parquet
        embedding_column: embedding
        text_column: text
        filter_columns:
          - book_name
          - chapter_title
        id_column: null
    """
    log = logger.bind(task="load_dataset_config", path=str(config_path))

    if not config_path.exists():
        log.error("config_file_not_found")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    # Validate required fields
    required_fields = ["name", "data_file", "query_file", "embedding_column", "text_column"]
    missing = [field for field in required_fields if field not in data]
    if missing:
        log.error("missing_required_fields", missing=missing)
        raise ValueError(f"Missing required configuration fields: {missing}")

    # Resolve relative paths to absolute paths
    config_dir = config_path.parent
    data_file = Path(data["data_file"])
    query_file = Path(data["query_file"])

    # Make paths absolute relative to config file location
    if not data_file.is_absolute():
        data_file = (config_dir / data_file).resolve()
    if not query_file.is_absolute():
        query_file = (config_dir / query_file).resolve()

    config = DatasetConfig(
        name=data["name"],
        data_file=data_file,
        query_file=query_file,
        embedding_column=data["embedding_column"],
        text_column=data["text_column"],
        filter_columns=data.get("filter_columns", []),
        id_column=data.get("id_column"),
        query_embedding_column=data.get("query_embedding_column", "query_embedding"),
        query_text_column=data.get("query_text_column", "query_text"),
        metadata=data.get("metadata", {}),
    )

    log.info(
        "config_loaded",
        name=config.name,
        data_file=str(config.data_file),
        query_file=str(config.query_file),
        embedding_column=config.embedding_column,
        filter_columns=config.filter_columns,
    )

    return config


def get_dataset_config_from_env() -> Optional[DatasetConfig]:
    """
    Load dataset configuration from DATASET_CONFIG environment variable.

    Returns:
        DatasetConfig: Loaded configuration, or None if env var not set.
    """
    config_path = os.getenv("DATASET_CONFIG")
    if not config_path:
        return None

    return load_dataset_config(Path(config_path))


# =============================================================================
# Schema Inference Functions
# =============================================================================


def infer_schema_from_parquet(parquet_path: Path) -> dict[str, Any]:
    """
    Infer column schema from Parquet file metadata.

    Reads Parquet metadata without loading the full dataset to determine
    column names, types, and dimensions for vector columns.

    Args:
        parquet_path: Path to the Parquet file.

    Returns:
        dict: Schema information with structure:
            {
                "columns": [
                    {"name": "id", "type": "int64", "nullable": True},
                    {"name": "embedding", "type": "list<float>", "dims": 1536},
                    ...
                ],
                "num_rows": 35048,
                "file_size_bytes": 300000000
            }

    Raises:
        FileNotFoundError: If Parquet file doesn't exist.
    """
    log = logger.bind(task="infer_schema_from_parquet", path=str(parquet_path))

    if not parquet_path.exists():
        log.error("parquet_file_not_found")
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    # Read Parquet metadata without loading data
    parquet_file = pq.ParquetFile(parquet_path)
    schema = parquet_file.schema_arrow
    metadata = parquet_file.metadata

    columns = []
    for i, field in enumerate(schema):
        col_info = {
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable,
        }

        # Detect vector dimensions for list types
        type_str = str(field.type)
        if "list<" in type_str and "float" in type_str:
            # Try to infer dimensions by reading first row
            try:
                table = parquet_file.read_row_group(0, columns=[field.name])
                first_val = table.column(0)[0].as_py()
                if isinstance(first_val, (list, tuple)):
                    col_info["dims"] = len(first_val)
            except Exception:
                pass

        columns.append(col_info)

    result = {
        "columns": columns,
        "num_rows": metadata.num_rows,
        "file_size_bytes": parquet_path.stat().st_size,
    }

    log.info(
        "schema_inferred",
        num_columns=len(columns),
        num_rows=result["num_rows"],
        file_size_mb=round(result["file_size_bytes"] / (1024 * 1024), 2),
    )

    return result


def get_embedding_dimensions(config: DatasetConfig) -> int:
    """
    Get the embedding dimensions from a dataset.

    Args:
        config: Dataset configuration.

    Returns:
        int: Number of dimensions in the embedding vectors.

    Raises:
        ValueError: If embedding dimensions cannot be determined.
    """
    log = logger.bind(
        task="get_embedding_dimensions",
        dataset=config.name,
        embedding_column=config.embedding_column,
    )

    # Read just one row to get dimensions
    df = pd.read_parquet(config.data_file, columns=[config.embedding_column])
    if len(df) == 0:
        log.error("empty_dataset")
        raise ValueError(f"Dataset is empty: {config.data_file}")

    first_embedding = df[config.embedding_column].iloc[0]
    if hasattr(first_embedding, "__len__"):
        dims = len(first_embedding)
        log.info("embedding_dimensions_detected", dims=dims)
        return dims

    log.error("invalid_embedding_format")
    raise ValueError(f"Cannot determine embedding dimensions from column {config.embedding_column}")


# =============================================================================
# Schema Builder Utilities
# =============================================================================


def get_filter_field_types(config: DatasetConfig) -> dict[str, str]:
    """
    Get the data types for filter columns.

    Reads Parquet schema to determine appropriate field types for each
    filter column (e.g., VARCHAR, INT, etc.).

    Args:
        config: Dataset configuration.

    Returns:
        dict[str, str]: Mapping of column name to SQL-like type.
    """
    log = logger.bind(task="get_filter_field_types", dataset=config.name)

    schema = infer_schema_from_parquet(config.data_file)

    # Map Arrow types to SQL-like types
    type_mapping = {
        "string": "VARCHAR",
        "large_string": "VARCHAR",
        "int64": "INT64",
        "int32": "INT32",
        "float64": "FLOAT64",
        "float32": "FLOAT32",
        "bool": "BOOL",
        "timestamp": "TIMESTAMP",
        "date32": "DATE",
    }

    result = {}
    for col in schema["columns"]:
        if col["name"] in config.filter_columns:
            arrow_type = col["type"].split("[")[0]  # Handle list<...> types
            result[col["name"]] = type_mapping.get(arrow_type, "VARCHAR")

    log.info("filter_types_detected", types=result)
    return result
