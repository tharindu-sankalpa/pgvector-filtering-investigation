-- =============================================================================
-- Hybrid search decomposition: time each part of the RRF query on its own.
--
-- The benchmark's hybrid query has a vector CTE, a full-text CTE, a FULL OUTER
-- JOIN and a final RRF sort. This script runs each part separately with
-- EXPLAIN ANALYZE so the slow part is visible.
--
-- Usage:
--   psql -d benchmark_vectors -f scripts/diagnose_hybrid_components.sql
--
-- Every query runs twice; read the second run (warm cache).
-- =============================================================================

\set ON_ERROR_STOP on
\pset pager off
SET statement_timeout = '600s';
SET hnsw.ef_search = 40;

SELECT embedding AS qvec FROM wot_chunks_2_5m ORDER BY id OFFSET 777 LIMIT 1 \gset
\set keyword 'dragon'

\echo '=== 0. How many rows match the keyword?'
SELECT count(*) AS matching_rows FROM wot_chunks_2_5m
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', :'keyword');

\echo '=== 1. Pure vector search, LIMIT 10'
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
ORDER BY embedding <=> :'qvec'::vector LIMIT 10;
EXPLAIN (ANALYZE, BUFFERS) SELECT id FROM wot_chunks_2_5m
ORDER BY embedding <=> :'qvec'::vector LIMIT 10;

\echo '=== 2. Vector CTE with ROW_NUMBER, LIMIT 20'
EXPLAIN (ANALYZE, BUFFERS) SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> :'qvec'::vector) AS rank
FROM wot_chunks_2_5m LIMIT 20;
EXPLAIN (ANALYZE, BUFFERS) SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> :'qvec'::vector) AS rank
FROM wot_chunks_2_5m LIMIT 20;

\echo '=== 3. Text-search CTE alone, LIMIT 20'
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                               plainto_tsquery('english', :'keyword')) DESC) AS rank
FROM wot_chunks_2_5m
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', :'keyword')
LIMIT 20;
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                               plainto_tsquery('english', :'keyword')) DESC) AS rank
FROM wot_chunks_2_5m
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', :'keyword')
LIMIT 20;

\echo '=== 4. Full hybrid RRF query, top_k 10 (CTE limits 20)'
EXPLAIN (ANALYZE, BUFFERS)
WITH vector_search AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> :'qvec'::vector) AS rank
    FROM wot_chunks_2_5m LIMIT 20),
text_search AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                                   plainto_tsquery('english', :'keyword')) DESC) AS rank
    FROM wot_chunks_2_5m
    WHERE to_tsvector('english', content) @@ plainto_tsquery('english', :'keyword')
    LIMIT 20),
rrf AS (
    SELECT COALESCE(v.id, t.id) AS id,
           COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0) AS score
    FROM vector_search v FULL OUTER JOIN text_search t ON v.id = t.id)
SELECT id, score FROM rrf ORDER BY score DESC LIMIT 10;
EXPLAIN (ANALYZE, BUFFERS)
WITH vector_search AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> :'qvec'::vector) AS rank
    FROM wot_chunks_2_5m LIMIT 20),
text_search AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                                   plainto_tsquery('english', :'keyword')) DESC) AS rank
    FROM wot_chunks_2_5m
    WHERE to_tsvector('english', content) @@ plainto_tsquery('english', :'keyword')
    LIMIT 20),
rrf AS (
    SELECT COALESCE(v.id, t.id) AS id,
           COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0) AS score
    FROM vector_search v FULL OUTER JOIN text_search t ON v.id = t.id)
SELECT id, score FROM rrf ORDER BY score DESC LIMIT 10;

\echo '=== 5. Full hybrid RRF query, top_k 50 (CTE limits 100)'
EXPLAIN (ANALYZE, BUFFERS)
WITH vector_search AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> :'qvec'::vector) AS rank
    FROM wot_chunks_2_5m LIMIT 100),
text_search AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                                   plainto_tsquery('english', :'keyword')) DESC) AS rank
    FROM wot_chunks_2_5m
    WHERE to_tsvector('english', content) @@ plainto_tsquery('english', :'keyword')
    LIMIT 100),
rrf AS (
    SELECT COALESCE(v.id, t.id) AS id,
           COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0) AS score
    FROM vector_search v FULL OUTER JOIN text_search t ON v.id = t.id)
SELECT id, score FROM rrf ORDER BY score DESC LIMIT 50;
EXPLAIN (ANALYZE, BUFFERS)
WITH vector_search AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY embedding <=> :'qvec'::vector) AS rank
    FROM wot_chunks_2_5m LIMIT 100),
text_search AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                                   plainto_tsquery('english', :'keyword')) DESC) AS rank
    FROM wot_chunks_2_5m
    WHERE to_tsvector('english', content) @@ plainto_tsquery('english', :'keyword')
    LIMIT 100),
rrf AS (
    SELECT COALESCE(v.id, t.id) AS id,
           COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0) AS score
    FROM vector_search v FULL OUTER JOIN text_search t ON v.id = t.id)
SELECT id, score FROM rrf ORDER BY score DESC LIMIT 50;
