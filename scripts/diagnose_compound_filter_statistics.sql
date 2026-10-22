-- =============================================================================
-- Compound filters: per-column statistics vs extended statistics.
--
-- For "book_name = X AND chapter_number = Y" PostgreSQL multiplies the two
-- selectivities as if the columns were independent. Chapter numbers repeat in
-- every book, so the product underestimates the matching rows. Extended
-- statistics (CREATE STATISTICS, PostgreSQL 14+ for expressions) record the
-- combined distribution instead.
--
-- Run in the fixed state (after ANALYZE). The script creates the statistics
-- object, measures, and drops it again, leaving the schema unchanged.
--
-- Usage:
--   psql -d benchmark_vectors -f scripts/diagnose_compound_filter_statistics.sql
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off
SET hnsw.ef_search = 40;

SELECT embedding AS qvec FROM wot_chunks_2_5m ORDER BY id OFFSET 777 LIMIT 1 \gset

\echo '=== 1. Actual rows for the compound filter'
SELECT count(*) AS actual_rows FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5';

\echo '=== 2. Per-expression statistics only (estimate assumes independence)'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5'
ORDER BY embedding <=> :'qvec'::vector LIMIT 10;

\echo '=== 3. With extended statistics on both expressions'
CREATE STATISTICS wot_book_chapter_stats (mcv, dependencies)
    ON (metadata->>'book_name'), (metadata->>'chapter_number')
    FROM wot_chunks_2_5m;
ANALYZE wot_chunks_2_5m;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5'
ORDER BY embedding <=> :'qvec'::vector LIMIT 10;

\echo '=== 3b. Same, with iterative scan so the HNSW path can return all 10 rows'
SET hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5'
ORDER BY embedding <=> :'qvec'::vector LIMIT 10;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5'
ORDER BY embedding <=> :'qvec'::vector LIMIT 10;
RESET hnsw.iterative_scan;

\echo '=== 4. Clean up'
DROP STATISTICS wot_book_chapter_stats;
