import os

# Benchmark Configuration
# Allow overriding NUM_QUERIES via environment variable
NUM_QUERIES = int(os.getenv("BENCHMARK_NUM_QUERIES", "100"))

# Standard Field Values
# These constants help ensure consistency across src

# Database Names
DB_MILVUS_ZILLIZ_SERVERLESS = "Milvus Zilliz Cloud Serverless"
DB_MILVUS_ZILLIZ_DEDICATED = "Milvus Zilliz Cloud Dedicated"
DB_MILVUS_AKS_STANDALONE = "Milvus AKS Self-Hosted (Standalone)"
DB_MILVUS_AKS_DISTRIBUTED = "Milvus AKS Self-Hosted (Distributed)"
DB_PGVECTOR_AZURE = "PGVector on Azure PostgreSQL"
DB_MONGODB_ATLAS = "MongoDB Atlas Search"
DB_AZURE_AI_SEARCH = "Azure AI Search"

# Test Locations
LOC_AZURE_NORTH_EUROPE = "North Europe Azure Machine Learning Compute Instance"
LOC_AZURE_SOUTH_ASIA = "South Asia Azure Machine Learning Compute Instance"
LOC_LOCAL_PC = "Local PC (Sri Lanka – SLT Pipeline)"

def get_env_int(key, default):
    """Get environment variable as int or return default."""
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default

