-- =============================================================================
-- DBeaver screenshot script: every DBeaver capture for the article, in order.
--
-- Connection: the "EKS vectors" connection (127.0.0.1:15432, benchmark_vectors,
-- benchmark_user). Open this file in ONE SQL editor tab and keep that tab for
-- the whole session: DBeaver keeps one database session per tab, and the SET
-- statements below only apply to that session.
--
-- How to run and capture:
--   * Run one statement at a time with Ctrl+Enter (cursor inside the statement).
--   * For EXPLAIN output, switch the result panel to Text view (the "Text"
--     toggle on the left edge of the results), so the whole plan is readable.
--   * Every timed EXPLAIN (ANALYZE ...) is listed once: run it twice and capture
--     the SECOND run, so the screenshot compares plans, not a cold and a warm cache.
--   * Keep the SET lines visible in the editor above the query when you capture.
--   * Save each image as doc/screenshots/dbeaver/<file name given in the header>.
--
-- Query vector: step 0b stores one embedding (the row at ORDER BY id OFFSET 777,
-- the same vector as every measurement in the article) in a session variable.
-- Every query reads it with (SELECT current_setting('myvars.embedding')::vector).
-- Keep the brackets and the SELECT: they make PostgreSQL build the vector ONCE
-- per query (shown as "InitPlan 1" in the plan). Without them, a brute-force
-- plan rebuilds the 1,536-number vector for every row it scans and runs about
-- 25 times slower than it really is. No vector literal appears in a screenshot. The
-- variable lives in this tab's session: if DBeaver reconnects (for example after
-- the port-forward restarts), run step 0b again before continuing.
--
-- Order: section A puts the database into the trap state (no statistics for
-- metadata->>'book_name') and captures C1-C8. Section B runs ANALYZE, restores
-- the correct state and captures C9-C15. Do not skip section B: it leaves the
-- database in the production-correct state.
--
-- Expected numbers may differ slightly from the article: the default estimate
-- is 0.5% of the table's current row estimate (about 12,494-12,500), and each
-- ANALYZE samples randomly (New Spring estimate about 60,000-68,000).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0. Check the connection (no screenshot)
--    Expect PostgreSQL 18.6, vector 0.8.7, benchmark_user.
-- -----------------------------------------------------------------------------
SELECT version(),
       (SELECT extversion FROM pg_extension WHERE extname = 'vector') AS pgvector,
       current_user;

-- 0b. Store the query vector for this session (no screenshot).
--     Expect one row: "vector stored, 1536 dims". set_config returns the value it
--     stored, so it is wrapped to avoid printing 1,536 numbers in the result grid.
SELECT 'vector stored, '
       || vector_dims(set_config('myvars.embedding', embedding::text, false)::vector)
       || ' dims' AS query_vector
FROM wot_chunks_2_5m
ORDER BY id
OFFSET 777
LIMIT 1;


-- =============================================================================
-- A. TRAP STATE: the expression index exists, but ANALYZE has not run since.
-- =============================================================================

-- A.1 Enter the trap state (no screenshot). Takes about a second each.
DROP INDEX wot_chunks_2_5m_expr_idx;

CREATE INDEX wot_chunks_2_5m_expr_idx ON wot_chunks_2_5m ((metadata->>'book_name'));


-- -----------------------------------------------------------------------------
-- C1  ->  d01-trap-no-statistics.png
-- Show: the pg_stats query returns NO rows, and the EXPLAIN plan says rows=12494
--       (or 12500): the planner's 0.5% default guess for every book.
-- To get both results in one capture: select both statements and press
-- Alt+X (Execute script). DBeaver opens one result tab per statement; drag the
-- second tab beside the first, then capture.
-- -----------------------------------------------------------------------------
SELECT tablename, attname, n_distinct, most_common_vals
FROM pg_stats
WHERE tablename = 'wot_chunks_2_5m_expr_idx';

EXPLAIN
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring';


-- Session settings for C2-C5 (pgvector defaults). Run both before C2.
SET hnsw.ef_search = 40;
SET hnsw.iterative_scan = off;


-- -----------------------------------------------------------------------------
-- C2  ->  d02-trap-limit-40-hnsw.png
-- Show: "Index Scan using wot_chunks_2_5m_embedding_idx", estimated rows=12494,
--       the Limit's actual rows (fewer than 40, about 18), Rows Removed by
--       Filter, and Execution Time (a few ms).
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 40;


-- -----------------------------------------------------------------------------
-- C3  ->  d03-trap-limit-41-bitmap.png      ** the key image of the article **
-- Show: Limit -> Sort -> Bitmap Heap Scan -> Bitmap Index Scan on
--       wot_chunks_2_5m_expr_idx; the estimate rows=12494 next to the actual
--       rows=63945 on the Bitmap Heap Scan line; Heap Blocks; Execution Time
--       (about 350-400 ms).
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 41;


-- -----------------------------------------------------------------------------
-- C4  ->  d04-trap-limit-41-largest-book.png
-- Show: the same bitmap plan for the largest book: actual rows=209226 and an
--       Execution Time of about 1.1 s. Same estimate (rows=12494).
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '06. Lord of Chaos'
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 41;


-- -----------------------------------------------------------------------------
-- C5  ->  d05-trap-ef400-limit-25.png      (optional; listed in the plan, not embedded)
-- Show: with ef_search = 400 the bitmap plan appears already at LIMIT 25.
-- -----------------------------------------------------------------------------
SET hnsw.ef_search = 400;

EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 25;

SET hnsw.ef_search = 40;


-- -----------------------------------------------------------------------------
-- C6  ->  d06-trap-iterative-scan-ignored.png      (used in doc/workarounds.md)
-- Show: the SET line, and the plan still a Bitmap Heap Scan: iterative scan
--       cannot change the planner's choice.
-- -----------------------------------------------------------------------------
SET hnsw.iterative_scan = relaxed_order;

EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 50;

SET hnsw.iterative_scan = off;


-- -----------------------------------------------------------------------------
-- C7  ->  d07-cte-overfetch-iterative-off.png      (used in doc/workarounds.md)
-- Show: HNSW Index Scan inside the CTE, the inner Limit returning only about 40
--       rows, and the outer Limit returning about 18 rows instead of 50.
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
WITH nearest AS (
    SELECT id, metadata,
           embedding <=> (SELECT current_setting('myvars.embedding')::vector) AS dist
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
    LIMIT 2000
)
SELECT id FROM nearest
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY dist
LIMIT 50;


-- -----------------------------------------------------------------------------
-- C8  ->  d08-cte-overfetch-iterative-relaxed.png      (used in doc/workarounds.md)
-- Show: the SET line, HNSW in the CTE, the outer Limit returning 50 rows, and
--       Execution Time (a few ms).
-- -----------------------------------------------------------------------------
SET hnsw.iterative_scan = relaxed_order;

EXPLAIN (ANALYZE, BUFFERS)
WITH nearest AS (
    SELECT id, metadata,
           embedding <=> (SELECT current_setting('myvars.embedding')::vector) AS dist
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
    LIMIT 2000
)
SELECT id FROM nearest
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY dist
LIMIT 50;

SET hnsw.iterative_scan = off;


-- =============================================================================
-- B. FIXED STATE: statistics collected after the index exists.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- C9  ->  d09-analyze-statistics.png
-- Run ANALYZE first (takes a few seconds), then the pg_stats query.
-- Show: the ANALYZE statement, then one pg_stats row with n_distinct = 16 and
--       the book names with their frequencies (most_common_freqs).
-- -----------------------------------------------------------------------------
ANALYZE wot_chunks_2_5m;

SELECT attname, n_distinct, most_common_vals, most_common_freqs
FROM pg_stats
WHERE tablename = 'wot_chunks_2_5m_expr_idx';


-- -----------------------------------------------------------------------------
-- C10  ->  d10-fixed-limit-41-hnsw.png      (also cropped for the LinkedIn carousel)
-- Show: the SAME query as C3, now "Index Scan using wot_chunks_2_5m_embedding_idx",
--       with a realistic estimate (about 60,000-68,000 rows) and an Execution
--       Time around 1 ms. Same settings as C3.
-- -----------------------------------------------------------------------------
SET hnsw.ef_search = 40;
SET hnsw.iterative_scan = off;

EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 41;


-- -----------------------------------------------------------------------------
-- C11  ->  d11-fixed-limit-100-strict-order.png      (optional; listed in the plan)
-- Show: HNSW with iterative scan returning the full 100 rows, Rows Removed by
--       Filter, Execution Time.
-- -----------------------------------------------------------------------------
SET hnsw.ef_search = 100;
SET hnsw.iterative_scan = strict_order;

EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 100;

SET hnsw.ef_search = 40;
SET hnsw.iterative_scan = off;


-- -----------------------------------------------------------------------------
-- C12  ->  d12-compound-filter-estimate.png
-- Show: the estimated rows on the Bitmap Heap Scan (about 300) against the
--       actual rows=4129, and Rows Removed by Filter. Even with statistics,
--       the planner multiplies the two filters' selectivities.
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
  AND metadata->>'chapter_number' = '5'
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 10;


-- -----------------------------------------------------------------------------
-- C13  ->  d13-indexes.png
-- Show: the four indexes with access method (hnsw, gin, btree x2), validity and
--       size (HNSW about 19 GB, GIN about 193 MB, primary key about 54 MB,
--       metadata B-tree about 17 MB) and their definitions.
-- -----------------------------------------------------------------------------
SELECT i.relname AS index_name,
       am.amname AS access_method,
       x.indisvalid AS valid,
       pg_size_pretty(pg_relation_size(i.oid)) AS index_size,
       pg_get_indexdef(i.oid) AS index_definition
FROM pg_index x
JOIN pg_class t ON t.oid = x.indrelid
JOIN pg_class i ON i.oid = x.indexrelid
JOIN pg_am am ON am.oid = i.relam
WHERE t.relname = 'wot_chunks_2_5m'
ORDER BY pg_relation_size(i.oid) DESC;


-- -----------------------------------------------------------------------------
-- C14  ->  d14-pure-vector-hnsw.png
-- Show: a vector search with no filter: "Index Scan using
--       wot_chunks_2_5m_embedding_idx", 100 rows, Execution Time.
-- ef_search must be at least the LIMIT: with ef_search = 40 HNSW returns only
-- 40 rows here, because it never holds more than ef_search candidates.
-- -----------------------------------------------------------------------------
SET hnsw.ef_search = 100;

EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
ORDER BY embedding <=> (SELECT current_setting('myvars.embedding')::vector)
LIMIT 100;

SET hnsw.ef_search = 40;


-- -----------------------------------------------------------------------------
-- C15  ->  d15-hybrid-text-search.png
-- Show: the text-search half of the hybrid query on its own: Bitmap Index Scan
--       on wot_chunks_2_5m_to_tsvector_idx finding 76,198 rows, the (Parallel)
--       Bitmap Heap Scan reading them all, the Sort on ts_rank, and Execution
--       Time (about 600 ms).
-- -----------------------------------------------------------------------------
EXPLAIN (ANALYZE, BUFFERS)
SELECT id,
       ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content),
                                           plainto_tsquery('english', 'dragon')) DESC) AS rank
FROM wot_chunks_2_5m
WHERE to_tsvector('english', content) @@ plainto_tsquery('english', 'dragon')
LIMIT 20;


-- -----------------------------------------------------------------------------
-- Done. Confirm the database is left in the fixed state (no screenshot):
-- expect one row with n_distinct = 16.
-- -----------------------------------------------------------------------------
SELECT attname, n_distinct
FROM pg_stats
WHERE tablename = 'wot_chunks_2_5m_expr_idx';

RESET hnsw.ef_search;
RESET hnsw.iterative_scan;
