-- =============================================================================
-- Statistics trap diagnostic: filtered HNSW search with and without
-- expression-index statistics.
--
-- Run this script twice against benchmark_vectors and compare the output:
--
--   1. Trap state: the B-tree on metadata->>'book_name' exists, but ANALYZE has
--      not run since it was created, so pg_stats has no row for it.
--        DROP INDEX wot_chunks_2_5m_expr_idx;
--        CREATE INDEX wot_chunks_2_5m_expr_idx ON wot_chunks_2_5m ((metadata->>'book_name'));
--
--   2. Fixed state: statistics collected after the index exists.
--        ANALYZE wot_chunks_2_5m;
--
-- Usage (psql, server-side timings from EXPLAIN ANALYZE):
--   psql -d benchmark_vectors -f scripts/diagnose_statistics_trap.sql
--
-- Every timed query runs twice; read the second run. The first run warms the
-- buffer cache so the comparison is between plans, not between cache states.
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off
SET statement_timeout = '600s';

-- One fixed query vector for every experiment: a stored row's embedding.
SELECT embedding AS qvec FROM wot_chunks_2_5m ORDER BY id OFFSET 777 LIMIT 1 \gset

-- -----------------------------------------------------------------------------
-- 1. Which state is the database in?
--    Expression-index statistics appear in pg_stats under the INDEX name.
-- -----------------------------------------------------------------------------
\echo '=== 1. Expression statistics present? (0 rows = trap state)'
SELECT tablename, attname, n_distinct
FROM pg_stats
WHERE tablename = 'wot_chunks_2_5m_expr_idx';

\echo '=== 1b. Planner row estimate vs actual row count for each test filter'
SELECT b AS book_name,
       (SELECT count(*) FROM wot_chunks_2_5m WHERE metadata->>'book_name' = b) AS actual_rows
FROM unnest(ARRAY['00. New Spring', '13. Towers of Midnight', '06. Lord of Chaos']) AS b;
EXPLAIN SELECT id FROM wot_chunks_2_5m WHERE metadata->>'book_name' = '00. New Spring';

-- -----------------------------------------------------------------------------
-- 2. Tipping point: the first LIMIT at which the planner leaves HNSW,
--    for several ef_search values and three filters of different size.
--    Cost-only EXPLAIN, so this section is fast in both states.
-- -----------------------------------------------------------------------------
CREATE FUNCTION pg_temp.first_non_hnsw_limit(q vector, book text, ef int, max_k int)
RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    plan_line text;
    plan_text text;
    k int;
BEGIN
    EXECUTE format('SET LOCAL hnsw.ef_search = %s', ef);
    FOR k IN 1..max_k LOOP
        plan_text := '';
        FOR plan_line IN EXECUTE format(
            'EXPLAIN SELECT id FROM wot_chunks_2_5m
              WHERE metadata->>''book_name'' = %L
              ORDER BY embedding <=> %L::vector LIMIT %s', book, q::text, k)
        LOOP
            plan_text := plan_text || plan_line;
        END LOOP;
        IF plan_text !~ 'wot_chunks_2_5m_embedding_idx' THEN
            RETURN k::text;
        END IF;
    END LOOP;
    RETURN 'HNSW for every LIMIT up to ' || max_k;
END $$;

\echo '=== 2. First LIMIT that abandons HNSW (ef_search x filter)'
SELECT ef AS ef_search, b AS book_name,
       pg_temp.first_non_hnsw_limit(:'qvec', b, ef, 200) AS switches_at_limit
FROM unnest(ARRAY[40, 64, 100, 200, 400]) AS ef,
     unnest(ARRAY['00. New Spring', '13. Towers of Midnight', '06. Lord of Chaos']) AS b
ORDER BY ef, b;

-- -----------------------------------------------------------------------------
-- 3. The cliff itself: LIMIT 40 vs LIMIT 41, ef_search = 40, iterative off.
-- -----------------------------------------------------------------------------
SET hnsw.ef_search = 40;
SET hnsw.iterative_scan = off;

\echo '=== 3a. New Spring, LIMIT 40 (warm-up run, then measured run)'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 40;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 40;

\echo '=== 3b. New Spring, LIMIT 41'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 41;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 41;

\echo '=== 3c. Lord of Chaos (largest filter), LIMIT 41'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '06. Lord of Chaos' ORDER BY embedding <=> :'qvec'::vector LIMIT 41;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '06. Lord of Chaos' ORDER BY embedding <=> :'qvec'::vector LIMIT 41;

-- -----------------------------------------------------------------------------
-- 4. Result completeness on the HNSW path: how many of the requested rows
--    come back, with and without iterative scanning.
-- -----------------------------------------------------------------------------
\echo '=== 4a. HNSW, ef_search=40, iterative off, LIMIT 40 (rows returned)'
SELECT count(*) AS rows_returned FROM (
    SELECT id FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' = '00. New Spring'
    ORDER BY embedding <=> :'qvec'::vector LIMIT 40) s;

\echo '=== 4b. HNSW, ef_search=40, iterative strict_order, LIMIT 40'
SET hnsw.iterative_scan = strict_order;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 40;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 40;
RESET hnsw.iterative_scan;

-- -----------------------------------------------------------------------------
-- 5. Workarounds at LIMIT 50, New Spring, ef_search = 40 unless stated.
--    Read the plan node, the Limit's actual rows and the Execution Time.
-- -----------------------------------------------------------------------------
\echo '=== 5.0 Default'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;

\echo '=== 5.1 enable_bitmapscan = off'
SET enable_bitmapscan = off;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
RESET enable_bitmapscan;

\echo '=== 5.2 enable_seqscan, enable_bitmapscan, enable_indexscan = off (also disables the HNSW index scan)'
SET enable_seqscan = off; SET enable_bitmapscan = off; SET enable_indexscan = off;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
RESET enable_seqscan; RESET enable_bitmapscan; RESET enable_indexscan;

\echo '=== 5.3 hnsw.ef_search = 200'
SET hnsw.ef_search = 200;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
SET hnsw.ef_search = 40;

\echo '=== 5.4 hnsw.iterative_scan = relaxed_order'
SET hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;

\echo '=== 5.5 iterative relaxed_order + enable_seqscan, enable_bitmapscan, enable_indexscan = off'
SET enable_seqscan = off; SET enable_bitmapscan = off; SET enable_indexscan = off;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
RESET enable_seqscan; RESET enable_bitmapscan; RESET enable_indexscan;
RESET hnsw.iterative_scan;

\echo '=== 5.6a CTE over-fetch (inner LIMIT 50 x 40 = 2000), iterative off'
EXPLAIN (ANALYZE, BUFFERS)
WITH nearest AS (
    SELECT id, metadata, embedding <=> :'qvec'::vector AS dist
    FROM wot_chunks_2_5m ORDER BY embedding <=> :'qvec'::vector LIMIT 2000)
SELECT id FROM nearest WHERE metadata->>'book_name' = '00. New Spring' ORDER BY dist LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS)
WITH nearest AS (
    SELECT id, metadata, embedding <=> :'qvec'::vector AS dist
    FROM wot_chunks_2_5m ORDER BY embedding <=> :'qvec'::vector LIMIT 2000)
SELECT id FROM nearest WHERE metadata->>'book_name' = '00. New Spring' ORDER BY dist LIMIT 50;

\echo '=== 5.6b CTE over-fetch, iterative relaxed_order (lets the inner HNSW scan return 2000 rows)'
SET hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS)
WITH nearest AS (
    SELECT id, metadata, embedding <=> :'qvec'::vector AS dist
    FROM wot_chunks_2_5m ORDER BY embedding <=> :'qvec'::vector LIMIT 2000)
SELECT id FROM nearest WHERE metadata->>'book_name' = '00. New Spring' ORDER BY dist LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS)
WITH nearest AS (
    SELECT id, metadata, embedding <=> :'qvec'::vector AS dist
    FROM wot_chunks_2_5m ORDER BY embedding <=> :'qvec'::vector LIMIT 2000)
SELECT id FROM nearest WHERE metadata->>'book_name' = '00. New Spring' ORDER BY dist LIMIT 50;
RESET hnsw.iterative_scan;

\echo '=== 5.7 Benchmark settings: ef_search=64, iterative strict_order'
SET hnsw.ef_search = 64; SET hnsw.iterative_scan = strict_order;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' ORDER BY embedding <=> :'qvec'::vector LIMIT 50;
RESET hnsw.ef_search; RESET hnsw.iterative_scan;

-- -----------------------------------------------------------------------------
-- 6. Compound filter: book AND chapter. PostgreSQL multiplies the two
--    selectivities as if they were independent.
-- -----------------------------------------------------------------------------
\echo '=== 6. Compound filter: estimate vs actual'
SELECT count(*) AS actual_rows FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5';
SET hnsw.ef_search = 40;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5'
ORDER BY embedding <=> :'qvec'::vector LIMIT 10;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5'
ORDER BY embedding <=> :'qvec'::vector LIMIT 10;
RESET hnsw.ef_search;
