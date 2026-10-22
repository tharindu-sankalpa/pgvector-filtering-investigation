"""Run the real insert/index/retrieval entrypoints on 1,000 worker-local rows.

This validates the EKS wiring. It is not a 2.5M-row performance measurement.
The source dataset is read only; the small sample lives in the worker's /tmp.
"""
import os
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import psycopg

sys.path.insert(0, '/app/src')
from shared import results_db


def main():
    vector = pq.ParquetFile('/app/data/gen_wot_2.5m.parquet')
    query = pq.ParquetFile('/app/data/wot_retrieval_queries.parquet')
    assert vector.metadata.num_rows == 2_500_000, vector.metadata.num_rows
    assert query.metadata.num_rows == 4_373, query.metadata.num_rows
    batch = next(vector.iter_batches(batch_size=1000))
    assert {'embedding', 'text', 'book_name'}.issubset(batch.schema.names), batch.schema.names
    embedding = batch.column(batch.schema.get_field_index('embedding'))[0].as_py()
    assert len(embedding) == 1536
    sample = '/tmp/eks-smoke.parquet'
    pq.write_table(pa.Table.from_batches([batch]), sample)
    print('Dataset metadata verified: 2,500,000 vectors; 4,373 queries; 1,536 dimensions', flush=True)
    env = os.environ.copy()
    env.update({'TABLE_NAME': 'wot_eks_smoke', 'DATA_FILE': sample, 'DATASET_SIZE': '1000',
                'QUERY_FILE': '/app/data/wot_retrieval_queries.parquet', 'VECTOR_PG_DATABASE': 'benchmark_vectors',
                'BATCH_SIZES': '[500]', 'INSERT_CHUNK_SIZE': '250', 'CONCURRENCY_LEVELS': '1',
                'TOP_K_VALUES': '10', 'BENCHMARK_NUM_QUERIES': '3', 'WARMUP_QUERIES': '3',
                'DATABASE_NAME_OVERRIDE': 'PostgreSQL CNPG Self-Hosted',
                'DATABASE_CONFIG': 'EKS smoke only: 1000 rows; not a 2.5M benchmark',
                'TEST_LOCATION': 'EKS ap-south-1', 'PGSSLMODE': 'require'})
    for entry in ['01_insert_benchmark_copy.py', '02_create_indexes.py', '03_retrieval_asyncpg.py']:
        print('SMOKE ENTRYPOINT', entry, flush=True)
        subprocess.run([sys.executable, '/app/src/pgvector/' + entry], env=env, check=True)
    with psycopg.connect(host=env['PG_HOST'], port=env.get('PG_PORT', '5432'),
                         dbname='benchmark_vectors', user=env['PG_USER'], password=env['PG_PASSWORD'], sslmode='require') as conn:
        assert conn.execute('SELECT count(*) FROM wot_eks_smoke').fetchone()[0] == 1000
        definitions = [r[0] for r in conn.execute("""SELECT pg_get_indexdef(i.indexrelid)
            FROM pg_index i WHERE i.indrelid='wot_eks_smoke'::regclass AND i.indisvalid""")]
        for required in ['USING hnsw (embedding vector_cosine_ops)',
                         "USING gin (to_tsvector('english'::regconfig, content))",
                         "USING btree (((metadata ->> 'book_name'::text)))"]:
            assert any(required in definition for definition in definitions), definitions
    with results_db.get_results_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT count(DISTINCT test_type) FROM benchmark_retrieval_summary WHERE dataset_size=1000 AND database_config=%s AND total_queries=3 AND qps>0", (env['DATABASE_CONFIG'],))
            assert cursor.fetchone()[0] == 3
    print('SMOKE PASSED: real COPY, HNSW/B-tree/GIN, three retrieval modes and results storage', flush=True)


if __name__ == '__main__':
    main()
