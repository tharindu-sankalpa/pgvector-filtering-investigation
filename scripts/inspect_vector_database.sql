-- DBeaver connection: benchmark_vectors, 127.0.0.1:15432, benchmark_user.
-- Read-only status queries. Run individual statements with Ctrl+Enter.
-- These avoid full-table scans and EXPLAIN ANALYZE during the benchmark.
-- Use DBeaver auto-commit so repeated checks see fresh statistics snapshots.
-- Durations and activity are live; statistics counters are cumulative estimates.

-- 1. Connection identity, runtime and extension.
SELECT current_database() AS database_name, current_user AS connected_user,
       inet_server_addr() AS server_address, pg_is_in_recovery() AS is_replica,
       clock_timestamp() AS observed_at, version();
SELECT extname, extversion FROM pg_extension ORDER BY extname;

-- 2. Table existence, estimated rows, statistics timestamps and storage.
-- estimated_rows can be stale until ANALYZE; it is not an exact load counter.
-- toast_bytes includes the TOAST relation and its indexes (large vector storage).
SELECT s.schemaname, s.relname, s.n_live_tup AS estimated_rows,
       s.n_dead_tup AS estimated_dead_rows,
       pg_size_pretty(pg_relation_size(s.relid)) AS main_heap,
       pg_size_pretty(pg_table_size(s.relid)) AS table_including_toast,
       pg_size_pretty(pg_indexes_size(s.relid)) AS indexes,
       pg_size_pretty(pg_total_relation_size(s.relid)) AS total,
       pg_size_pretty(CASE WHEN c.reltoastrelid <> 0
                           THEN pg_total_relation_size(c.reltoastrelid) ELSE 0 END) AS toast_bytes,
       s.last_analyze, s.last_autoanalyze, s.last_autovacuum
FROM pg_stat_user_tables s JOIN pg_class c ON c.oid=s.relid
WHERE s.schemaname='public' AND s.relname IN ('wot_chunks_2_5m','wot_eks_smoke')
ORDER BY s.relname;

-- 3. Committed indexes: actual generated names, method, validity, size, definition.
-- An index being built may not be visible here until CREATE INDEX commits.
SELECT t.relname AS table_name, i.relname AS index_name, am.amname AS access_method,
       x.indisvalid AS valid, x.indisready AS ready_for_inserts,
       pg_size_pretty(pg_relation_size(i.oid)) AS index_size,
       pg_get_indexdef(i.oid) AS index_definition
FROM pg_index x
JOIN pg_class t ON t.oid=x.indrelid
JOIN pg_namespace ns ON ns.oid=t.relnamespace
JOIN pg_class i ON i.oid=x.indexrelid
JOIN pg_am am ON am.oid=i.relam
WHERE ns.nspname='public' AND t.relname IN ('wot_chunks_2_5m','wot_eks_smoke')
ORDER BY t.relname, am.amname, i.relname;

-- 4. Current index build. No rows means no visible active CREATE INDEX.
-- blocks_done / blocks_total describes the reported phase, not a time estimate.
-- HNSW may report tuples_total=0; tuples_done is still useful.
SELECT p.pid, p.relid::regclass AS table_name, p.command, p.phase,
       p.blocks_done, p.blocks_total,
       round(100.0*p.blocks_done/NULLIF(p.blocks_total,0),2) AS phase_blocks_pct,
       p.tuples_done, p.tuples_total,
       p.lockers_done, p.lockers_total,
       clock_timestamp()-a.query_start AS elapsed,
       a.wait_event_type, a.wait_event
FROM pg_stat_progress_create_index p
LEFT JOIN pg_stat_activity a ON a.pid=p.pid
WHERE p.datname=current_database();

-- 5. COPY progress. The count is for one current COPY, not the complete load.
SELECT pid, relid::regclass AS table_name, command, type,
       bytes_processed, bytes_total, tuples_processed
FROM pg_stat_progress_copy WHERE datname=current_database();

-- 6. Active sessions and blockers. Same-role sessions are visible to the app user;
-- query text for other roles can be redacted unless monitoring rights are granted.
SELECT pid, usename, application_name, state,
       clock_timestamp()-query_start AS query_age,
       wait_event_type, wait_event, pg_blocking_pids(pid) AS blocking_pids,
       left(query,500) AS query
FROM pg_stat_activity
WHERE datname=current_database() AND pid<>pg_backend_pid()
  AND (state<>'idle' OR cardinality(pg_blocking_pids(pid))>0)
ORDER BY query_start;

-- 7. Index usage since the statistics reset (not latency or per-study counters).
SELECT relname AS table_name, indexrelname AS index_name,
       idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes
WHERE schemaname='public' AND relname='wot_chunks_2_5m'
ORDER BY indexrelname;

-- 8. This DBeaver session's settings, NOT the benchmark worker's session settings.
-- Loading the vector type makes pgvector's custom settings visible in this backend.
SELECT vector_dims('[1,2,3]'::vector) AS extension_loaded;
SELECT name, setting, unit, source
FROM pg_settings
WHERE name IN ('shared_buffers','effective_cache_size','work_mem',
              'maintenance_work_mem','max_parallel_maintenance_workers',
              'random_page_cost','cpu_operator_cost','statement_timeout',
              'enable_seqscan','enable_bitmapscan','enable_indexscan')
   OR name LIKE 'hnsw.%'
ORDER BY name;

-- Exact COUNT(*), metadata distributions, EXPLAIN ANALYZE and recall experiments
-- are in perf_audit_pgvector_2.5m_wot.sql. Run those after the timed study to avoid
-- changing CPU/I/O/cache conditions. This file does not change database settings.
