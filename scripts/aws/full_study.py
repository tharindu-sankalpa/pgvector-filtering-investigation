"""Run the full EKS study without a local orchestration dependency.

The Job executes the existing COPY, index and retrieval entrypoints in sequence.
A separate results-table record tracks phase, settings and failures by run_id.
"""
from datetime import datetime, timezone
import json
import os
import subprocess
import sys

import psycopg

sys.path.insert(0, '/app/src')
from shared import results_db


def audit(run_id, phase, settings, evidence=None, done=False):
    with results_db.get_results_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('''CREATE TABLE IF NOT EXISTS benchmark_study_runs (
                run_id text PRIMARY KEY,
                started_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now(),
                finished_at timestamptz,
                phase text NOT NULL,
                settings jsonb NOT NULL,
                evidence jsonb NOT NULL DEFAULT '{}'::jsonb
            )''')
            cur.execute('''INSERT INTO benchmark_study_runs (run_id, phase, settings, evidence, finished_at)
                VALUES (%s,%s,%s,%s,CASE WHEN %s THEN now() ELSE NULL END)
                ON CONFLICT (run_id) DO UPDATE SET phase=EXCLUDED.phase, updated_at=now(),
                settings=EXCLUDED.settings, evidence=benchmark_study_runs.evidence || EXCLUDED.evidence,
                finished_at=EXCLUDED.finished_at''',
                (run_id, phase, json.dumps(settings), json.dumps(evidence or {}), done))
    print(json.dumps({'run_id': run_id, 'phase': phase, 'utc': datetime.now(timezone.utc).isoformat()}), flush=True)


def vector_connection(env):
    return psycopg.connect(host=env['PG_HOST'], port=env.get('PG_PORT','5432'),
                           dbname='benchmark_vectors', user=env['PG_USER'], password=env['PG_PASSWORD'], sslmode='require')


def main():
    run_id = os.environ['RUN_ID']
    env = os.environ.copy()
    config = f'EKS {run_id} | m6i.4xlarge | PG18.6 pgvector0.8.7 | ef=max(64,k); iterative=strict_order'
    env.update({'TABLE_NAME': 'wot_chunks_2_5m', 'DATA_FILE': '/app/data/gen_wot_2.5m.parquet',
                'QUERY_FILE': '/app/data/wot_retrieval_queries.parquet', 'VECTOR_PG_DATABASE': 'benchmark_vectors',
                'DATASET_SIZE': '2500000', 'BATCH_SIZES': '[5000]', 'INSERT_CHUNK_SIZE': '100000',
                'CONCURRENCY_LEVELS': '1,2,4,8,16,32,50,100', 'TOP_K_VALUES': '1,5,10,20,50,100',
                'BENCHMARK_NUM_QUERIES': '4373', 'WARMUP_QUERIES': '50', 'HNSW_EF_SEARCH_MIN': '64',
                'HNSW_ITERATIVE_SCAN': 'strict_order', 'QUERY_TIMEOUT': '120',
                'DATABASE_NAME_OVERRIDE': 'PostgreSQL CNPG Self-Hosted', 'DATABASE_CONFIG': config,
                'TEST_LOCATION': 'EKS ap-south-1', 'PGSSLMODE': 'require'})
    settings = {k: env[k] for k in ['TABLE_NAME','DATASET_SIZE','BATCH_SIZES','INSERT_CHUNK_SIZE',
        'CONCURRENCY_LEVELS','TOP_K_VALUES','BENCHMARK_NUM_QUERIES','WARMUP_QUERIES','HNSW_EF_SEARCH_MIN',
        'HNSW_ITERATIVE_SCAN','QUERY_TIMEOUT','DATABASE_CONFIG','TEST_LOCATION']}
    settings['effective_warmup_queries'] = 48  # Existing entrypoint rounds down across four top_k values.
    settings['benchmark_image_digest'] = env['BENCHMARK_IMAGE_DIGEST']
    settings['configured_database_image_digest'] = env['POSTGRES_IMAGE_DIGEST']
    with vector_connection(env) as conn:
        settings['postgresql'] = conn.execute('SELECT version()').fetchone()[0]
        settings['pgvector'] = conn.execute("SELECT extversion FROM pg_extension WHERE extname='vector'").fetchone()[0]
        settings['postgresql_settings'] = dict(conn.execute("SELECT name, setting || COALESCE(unit,'') FROM pg_settings WHERE name IN ('shared_buffers','effective_cache_size','work_mem','maintenance_work_mem','random_page_cost','max_parallel_maintenance_workers')"))
    try:
        audit(run_id,'copy',settings)
        subprocess.run([sys.executable,'/app/src/pgvector/01_insert_benchmark_copy.py'],env=env,check=True)
        with vector_connection(env) as conn:
            rows=conn.execute('SELECT count(*) FROM wot_chunks_2_5m').fetchone()[0]
            if rows != 2_500_000:
                raise RuntimeError(f'Expected 2500000 rows, found {rows}')
            conn.execute('ANALYZE wot_chunks_2_5m')
        audit(run_id,'index',settings,{'inserted_rows':rows})
        subprocess.run([sys.executable,'/app/src/pgvector/02_create_indexes.py'],env=env,check=True)
        with vector_connection(env) as conn:
            indexes=list(conn.execute("SELECT indexname, indexdef, pg_relation_size(indexname::regclass) FROM pg_indexes WHERE tablename='wot_chunks_2_5m' ORDER BY indexname"))
            definitions = [r[0] for r in conn.execute("""SELECT pg_get_indexdef(i.indexrelid)
                FROM pg_index i WHERE i.indrelid='wot_chunks_2_5m'::regclass AND i.indisvalid""")]
            for required in ['USING hnsw (embedding vector_cosine_ops)',
                             "USING gin (to_tsvector('english'::regconfig, content))",
                             "USING btree (((metadata ->> 'book_name'::text)))"]:
                if not any(required in definition for definition in definitions):
                    raise RuntimeError(f'Missing valid index definition: {required}')
            conn.execute('ANALYZE wot_chunks_2_5m')
        audit(run_id,'retrieval',settings,{'indexes':[list(r) for r in indexes]})
        subprocess.run([sys.executable,'/app/src/pgvector/03_retrieval_asyncpg.py'],env=env,check=True)
        with results_db.get_results_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT count(*), COALESCE(sum(total_queries),0) FROM benchmark_retrieval_summary WHERE dataset_size=2500000 AND database_config=%s',(config,))
                cases, valid_queries=cur.fetchone()
        evidence={'retrieval_summaries':cases,'expected_scenarios':144,'valid_queries_in_summaries':int(valid_queries),'expected_queries':629712}
        if cases != 144:
            audit(run_id,'incomplete',settings,evidence,done=True)
            raise RuntimeError(f'Expected 144 scenario summaries, found {cases}; preserve failures and inspect logs')
        audit(run_id,'complete',settings,evidence,done=True)
        print('Full study complete. Validate query failures, row completeness and plans before publishing.',flush=True)
    except BaseException as error:
        audit(run_id,'failed',settings,{'error_type':type(error).__name__,'error':str(error)},done=True)
        raise


if __name__ == '__main__':
    main()
