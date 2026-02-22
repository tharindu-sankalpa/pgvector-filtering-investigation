import time
import psycopg2
import json
import sys
from pathlib import Path

# Add current directory to sys.path to import common
sys.path.append(str(Path(__file__).parent))
import common

# Import shared modules
sys.path.append(str(Path(__file__).parent.parent))
from shared.logger_structlog import setup_structlog

# Initialize structlog
logger = setup_structlog()

def create_index(index_type, params=None):
    """
    Creates an index on the embeddings table and measures build time.
    
    Args:
        index_type (str): 'HNSW', 'IVFFlat', or 'GIN' (for text search).
        params (dict): Index parameters (e.g., {'m': 16, 'ef_construction': 64}).
    """
    # Get connection to vector database (where indexes are created)
    conn = common.get_db_connection()
    
    # Ensure results tables exist in the results database
    results_conn = common.results_db.get_results_db_connection()
    common.results_db.setup_results_tables(results_conn)
    results_conn.close()
    
    logger.info("creating_index", type=index_type, params=params)

    sql_commands = []
    
    # Define SQL based on index type
    if index_type == "HNSW":
        # Default params if not provided
        m = params.get('m', 16)
        ef = params.get('ef_construction', 64)
        
        # vector_cosine_ops is standard for OpenAI embeddings
        sql_commands.append("SET maintenance_work_mem = '2GB';") # Boost memory for faster build
        sql_commands.append(f"""
            CREATE INDEX ON {common.TABLE_NAME} 
            USING hnsw (embedding vector_cosine_ops) 
            WITH (m = {m}, ef_construction = {ef});
        """)
        
    elif index_type == "IVFFlat":
        lists = params.get('lists', 100)
        sql_commands.append("SET maintenance_work_mem = '2GB';")
        sql_commands.append(f"""
            CREATE INDEX ON {common.TABLE_NAME} 
            USING ivfflat (embedding vector_cosine_ops) 
            WITH (lists = {lists});
        """)
        
    elif index_type == "GIN":
        # Full text search index on the 'content' column
        # Uses 'english' configuration by default
        sql_commands.append(f"""
            CREATE INDEX ON {common.TABLE_NAME} 
            USING gin (to_tsvector('english', content));
        """)

    elif index_type == "B-Tree":
        # B-tree index on metadata field for filtered search
        field = params.get('field')
        if not field:
            logger.error("missing_param_field_for_btree")
            return
            
        sql_commands.append(f"""
            CREATE INDEX ON {common.TABLE_NAME} 
            USING btree ((metadata->>'{field}'));
        """)

    else:
        logger.error("unknown_index_type", type=index_type)
        return

    # Execute and Measure
    try:
        with conn.cursor() as cur:
            # Drop existing indexes on the table to ensure clean build time
            # Only drop the specific index we are about to create to avoid wiping others
            logger.info("dropping_existing_indexes")
            
            if index_type in ["HNSW", "IVFFlat"]:
                cur.execute(f"DROP INDEX IF EXISTS {common.TABLE_NAME}_embedding_idx;")
            elif index_type == "GIN":
                cur.execute(f"DROP INDEX IF EXISTS {common.TABLE_NAME}_content_idx;")
            elif index_type == "B-Tree":
                field = params.get('field')
                cur.execute(f"DROP INDEX IF EXISTS {common.TABLE_NAME}_{field}_idx;")
            
            # Get row count for logging
            cur.execute(f"SELECT count(*) FROM {common.TABLE_NAME};")
            row_count = cur.fetchone()[0]
            
            logger.info("building_index")
            start_time = time.time()
            
            for cmd in sql_commands:
                logger.info("executing_sql", command=cmd.strip())
                cur.execute(cmd)
                
            conn.commit()
            end_time = time.time()
            
            build_time = end_time - start_time
            logger.info("index_created_successfully", build_time_seconds=round(build_time, 2))
            
            # Log results to results database
            log_result(index_type, params, row_count, build_time)
            
    except Exception as e:
        logger.error("index_creation_failed", error=str(e))
        conn.rollback()
    finally:
        conn.close()

def log_result(index_type, params, row_count, build_time):
    """Log index build stats to results database."""
    results_conn = common.results_db.get_results_db_connection()
    with results_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO benchmark_index_summary (
                database_name, test_location, dataset_size, database_config,
                index_type, index_parameters, table_row_count, total_build_time_seconds
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            );
        """, (common.get_database_name(), common.TEST_LOCATION, common.DATASET_SIZE,
              common.DATABASE_CONFIG, index_type, json.dumps(params) if params else None,
              row_count, build_time))
    results_conn.commit()
    results_conn.close()
    logger.info("results_saved_to_db")

def detect_dataset_type() -> str:
    """
    Detect dataset type based on table name.

    Returns:
        str: 'wot' or 'aerospace'
    """
    table_name = common.TABLE_NAME.lower()
    if 'wot' in table_name:
        return 'wot'
    return 'aerospace'


def get_filter_field(dataset_type: str) -> str:
    """
    Get the filter field name based on dataset type.

    Args:
        dataset_type: 'wot' or 'aerospace'

    Returns:
        str: Filter field name in metadata JSONB
    """
    if dataset_type == 'wot':
        return 'book_name'
    return 'aircraft_type'


def main():
    logger.info("pgvector_index_benchmark_init")

    # Detect dataset type
    dataset_type = detect_dataset_type()
    filter_field = get_filter_field(dataset_type)
    logger.info("dataset_detected", dataset_type=dataset_type, filter_field=filter_field)

    # 1. Create HNSW Index (Standard Configuration)
    create_index("HNSW", {"m": 16, "ef_construction": 64})

    # 2. Create GIN Index (For Hybrid Search)
    create_index("GIN")

    # 3. Create B-Tree Index (For Filtered Search)
    create_index("B-Tree", {"field": filter_field})

    logger.info("all_indexes_created_successfully")


if __name__ == "__main__":
    main()
