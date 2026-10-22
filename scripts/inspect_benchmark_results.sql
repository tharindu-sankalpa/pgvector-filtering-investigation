-- DBeaver connection: benchmark_results, 127.0.0.1:15433, results_user.
-- Read-only inspection of study audit, ingestion, index and retrieval summaries.
-- Run individual statements with Ctrl+Enter; use auto-commit for fresh snapshots.
-- Each "latest" CTE selects the latest full study, excluding separate smoke data.
-- Summary rows appear only when the relevant phase/scenario finishes.

-- 1. Connection identity.
SELECT current_database() AS database_name, current_user AS connected_user,
       clock_timestamp() AS observed_at, pg_is_in_recovery() AS is_replica;

-- 2. Study history and recorded phase. updated_at changes at phase transitions;
-- it is NOT a heartbeat. An unchanged value during a long build is normal.
SELECT run_id, phase, started_at, updated_at, finished_at,
       COALESCE(finished_at,clock_timestamp())-started_at AS elapsed,
       settings->>'DATABASE_CONFIG' AS database_config,
       settings->>'postgresql' AS postgresql,
       settings->>'pgvector' AS pgvector, evidence
FROM benchmark_study_runs ORDER BY started_at DESC;

-- 3. Complete recorded settings/evidence for the latest full study.
SELECT run_id, jsonb_pretty(settings) AS settings, jsonb_pretty(evidence) AS evidence
FROM benchmark_study_runs ORDER BY started_at DESC LIMIT 1;

-- 4. Latest study's completed ingestion measurement. COPY appends a label
-- to database_config; include that exact suffix when matching its run.
WITH latest AS (SELECT settings->>'DATABASE_CONFIG' AS config
               FROM benchmark_study_runs ORDER BY started_at DESC LIMIT 1)
SELECT s.run_timestamp, s.total_records, s.batch_size, s.num_batches,
       round(s.total_time_seconds::numeric,2) AS elapsed_seconds,
       round(s.throughput_vectors_per_sec::numeric,2) AS vectors_per_second,
       s.database_name, s.database_config
FROM benchmark_insert_summary s JOIN latest
 ON s.database_config IN (latest.config, latest.config || ' | COPY BINARY')
ORDER BY s.run_timestamp DESC;

-- 5. Latest study's COMPLETED index builds. An active build has no summary yet;
-- inspect pg_stat_progress_create_index on the vector connection for live progress.
WITH latest AS (SELECT settings->>'DATABASE_CONFIG' AS config
               FROM benchmark_study_runs ORDER BY started_at DESC LIMIT 1)
SELECT s.run_timestamp, s.index_type, s.index_parameters, s.table_row_count,
       round(s.total_build_time_seconds::numeric,2) AS build_seconds,
       s.notes
FROM benchmark_index_summary s JOIN latest ON s.database_config=latest.config
ORDER BY s.run_timestamp;

-- 6. Retrieval progress by search mode. Each should finish 48 scenarios.
-- total_queries counts successful timed queries; absent/short counts need log review.
WITH latest AS (SELECT settings->>'DATABASE_CONFIG' AS config
               FROM benchmark_study_runs ORDER BY started_at DESC LIMIT 1),
patterns(test_type) AS (VALUES ('Vector Search'),('Filtered Search'),('Hybrid Search'))
SELECT p.test_type, count(s.id) AS recorded_summaries,
       count(DISTINCT (s.top_k,s.concurrency_level)) FILTER (WHERE s.id IS NOT NULL) AS distinct_scenarios,
       48 AS expected_scenarios, COALESCE(sum(s.total_queries),0) AS successful_queries,
       209904 AS expected_queries, max(s.run_timestamp) AS latest_summary_at
FROM patterns p CROSS JOIN latest
LEFT JOIN benchmark_retrieval_summary s ON s.database_config=latest.config AND s.test_type=p.test_type
GROUP BY p.test_type ORDER BY p.test_type;

-- 7. Individual measurements for the latest study. Latencies converted to ms.
WITH latest AS (SELECT settings->>'DATABASE_CONFIG' AS config
               FROM benchmark_study_runs ORDER BY started_at DESC LIMIT 1)
SELECT s.run_timestamp, s.test_type, s.top_k, s.concurrency_level,
       s.total_queries AS successful_queries, 4373 AS expected_queries,
       round(s.qps::numeric,2) AS qps,
       round((s.avg_latency_seconds*1000)::numeric,2) AS mean_ms,
       round((s.p50_latency_seconds*1000)::numeric,2) AS p50_ms,
       round((s.p95_latency_seconds*1000)::numeric,2) AS p95_ms,
       round((s.p99_latency_seconds*1000)::numeric,2) AS p99_ms,
       round(s.total_duration_seconds::numeric,2) AS scenario_seconds
FROM benchmark_retrieval_summary s JOIN latest ON s.database_config=latest.config
ORDER BY s.test_type,s.top_k,s.concurrency_level,s.run_timestamp;

-- 8. Missing, duplicate or short-query scenarios (144 expected combinations).
-- Pending combinations are normal while the matrix is running. No rows means
-- one summary with all 4,373 successful queries exists for every combination;
-- that alone does not establish recall or complete returned-row counts.
WITH latest AS (SELECT settings->>'DATABASE_CONFIG' AS config
               FROM benchmark_study_runs ORDER BY started_at DESC LIMIT 1),
expected AS (
  SELECT p.test_type,c.concurrency_level,k.top_k
  FROM (VALUES ('Vector Search'),('Filtered Search'),('Hybrid Search')) p(test_type)
  CROSS JOIN unnest(ARRAY[1,2,4,8,16,32,50,100]) c(concurrency_level)
  CROSS JOIN unnest(ARRAY[1,5,10,20,50,100]) k(top_k)
)
SELECT e.test_type,e.top_k,e.concurrency_level,count(s.id) AS summary_count,
       min(s.total_queries) AS min_successful_queries,
       max(s.total_queries) AS max_successful_queries
FROM expected e CROSS JOIN latest
LEFT JOIN benchmark_retrieval_summary s ON s.database_config=latest.config
 AND s.test_type=e.test_type AND s.top_k=e.top_k AND s.concurrency_level=e.concurrency_level
GROUP BY e.test_type,e.top_k,e.concurrency_level
HAVING count(s.id)<>1 OR min(s.total_queries) IS DISTINCT FROM 4373
ORDER BY e.test_type,e.top_k,e.concurrency_level;

-- 9. Inventory of all stored configurations, including explicitly labelled smoke.
SELECT database_config,dataset_size,count(*) AS retrieval_summary_rows,
       min(run_timestamp) AS first_summary_at,max(run_timestamp) AS last_summary_at
FROM benchmark_retrieval_summary
GROUP BY database_config,dataset_size ORDER BY last_summary_at DESC;

-- Returned-row/clipping statistics and query errors are currently in Job logs;
-- they are NOT persisted as columns in benchmark_retrieval_summary. An empty
-- granular-results table is expected: this harness writes aggregate summaries.
-- kubectl -n benchmarks logs -f job/<study-job-name>
