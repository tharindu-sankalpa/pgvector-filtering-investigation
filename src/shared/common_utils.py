import os

# Benchmark Configuration
# Allow overriding NUM_QUERIES via environment variable
NUM_QUERIES = int(os.getenv("BENCHMARK_NUM_QUERIES", "100"))

# Standard Field Values
# These constants help ensure consistency across src

# Database Names
DB_MILVUS_ZILLIZ_SERVERLESS = "Milvus Zilliz Cloud Serverless"
DB_MILVUS_ZILLIZ_DEDICATED = "Milvus Zilliz Cloud Dedicated"
DB_MILVUS_EKS_STANDALONE = "Milvus EKS Self-Hosted (Standalone)"
DB_MILVUS_EKS_DISTRIBUTED = "Milvus EKS Self-Hosted (Distributed)"
DB_PGVECTOR_AWS = "PGVector on CNPG PostgreSQL"
DB_MONGODB_ATLAS = "MongoDB Atlas Search"

# Test Locations
LOC_EKS_MUMBAI = "EKS ap-south-1"
LOC_LOCAL_PC = "Local PC (Sri Lanka – SLT Pipeline)"

def get_env_int(key, default):
    """Get environment variable as int or return default."""
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default

