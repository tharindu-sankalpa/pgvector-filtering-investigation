-- START HERE: connect to benchmark_vectors (local port 15432).
-- For current indexes, sizes, active SQL, COPY/index progress and session settings,
-- open scripts/inspect_vector_database.sql first (lightweight read-only checks).
-- For saved metrics and full-study phase, use scripts/inspect_benchmark_results.sql
-- on the SEPARATE benchmark_results connection (local port 15433).
-- Run selected statements, not this entire file: EXPLAIN ANALYZE actually executes
-- searches; later sections change session settings and create/drop temp tables.
-- Wait for the timed study to finish before running these experiments, so they
-- do not change the benchmark's CPU, I/O or cache conditions.
--
-- ==============================================================================
-- POSTGRESQL + PGVECTOR PERFORMANCE INVESTIGATION SCRIPTS
-- ==============================================================================
-- This script contains queries designed to execute in DBeaver (or any SQL client)
-- against a CloudNativePG PostgreSQL instance with pgvector. It facilitates
-- understanding of regular vector search, filtered vector search, and hybrid search.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. DATABASE EXPLORATION & SAMPLING
-- ------------------------------------------------------------------------------

-- 1.1. Basic Sample
-- Retrieve a basic sample of rows to understand the table schema.
SELECT * FROM wot_chunks_2_5m LIMIT 10;

-- 1.2. Metadata Distribution
-- Analyze the distribution of data across different 'books'. This helps
-- identify the selectivity of our metadata filters.
SELECT
    metadata->>'book_name' AS book_name,
    COUNT(*) AS chunk_count
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' IS NOT NULL
GROUP BY metadata->>'book_name'
ORDER BY chunk_count DESC;

-- 1.3. Sample Vector Extraction
-- Extract a sample vector from a specific book to use as our query vector
-- in subsequent search queries.
SELECT
    metadata->>'book_name' AS sample_book,
    embedding::text AS sample_vector
FROM wot_chunks_2_5m
WHERE
    metadata->>'book_name' IS NOT NULL
    AND metadata->>'book_name' = '00. New Spring'
LIMIT 50;

-- ------------------------------------------------------------------------------
-- 2. QUERY CONTEXT SETUP
-- ------------------------------------------------------------------------------

-- Select a sample embedding from your own dataset for this session.
-- No source-corpus embedding is bundled with this script.
SELECT set_config('myvars.embedding', embedding::text, false)
FROM wot_chunks_2_5m
WHERE embedding IS NOT NULL
ORDER BY id
LIMIT 1;

-- Set a session variable for the text search keyword (used in Hybrid Search).
SET myvars.keyword = 'foretelling';

-- (Optional) Experiment with pgvector HNSW iterative scan settings.
-- strict_order: guarantees exact distance ordering (can be slower with filters)
-- relaxed_order: allows some relaxation for better performance
-- off: disables iterative scan
-- SET hnsw.iterative_scan = strict_order;
-- SET hnsw.iterative_scan = relaxed_order;
-- SET hnsw.iterative_scan = off;

-- Set ef_search to at least match your LIMIT (or higher for better recall)
SET hnsw.ef_search = 100;

-- ------------------------------------------------------------------------------
-- 3. SCENARIO A: PURE VECTOR SEARCH
-- ------------------------------------------------------------------------------

-- Use EXPLAIN (ANALYZE, BUFFERS) to see the execution plan.
-- This query performs a simple K-Nearest Neighbors (KNN) search over the entire
-- dataset without any filters. It should ideally use an Index Scan on the HNSW index.
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 100;

-- Actual Query Configuration:
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
    content,
    metadata
FROM wot_chunks_2_5m
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 100;


-- ------------------------------------------------------------------------------
-- 4. SCENARIO B: VECTOR SEARCH + METADATA FILTERING (FILTERED SEARCH)
-- ------------------------------------------------------------------------------

-- This scenario combines a vector similarity search with a metadata hard filter.
-- The query planner has to decide whether to use the vector index, the metadata
-- index, or a combination. Adjusting LIMIT (top_k) can drastically change the plan!
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '13. Towers of Midnight'
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 38;

-- Actual Query Configuration:
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
    content,
    metadata
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '13. Towers of Midnight'
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 38;


-- ------------------------------------------------------------------------------
-- 5. SCENARIO C: HYBRID SEARCH (VECTOR + FULL-TEXT SEARCH)
-- ------------------------------------------------------------------------------

-- Hybrid search combines semantic vector similarity with lexical full-text search
-- using Reciprocal Rank Fusion (RRF). This is usually implemented via CTEs (Common
-- Table Expressions).

-- Use EXPLAIN (ANALYZE, BUFFERS) to observe the planner evaluating multiple CTEs
-- and performing the FULL OUTER JOIN.
EXPLAIN (ANALYZE, BUFFERS)
WITH vector_search AS (
  -- 1. Retrieve top candidates using semantic vector search
  SELECT
    id,
    ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
  FROM wot_chunks_2_5m
  LIMIT 200 -- Fetch more candidates typically (e.g., top_k * 2)
),
text_search AS (
  -- 2. Retrieve top candidates using lexical full-text search (BM25/ts_rank)
  SELECT
    id,
    ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content), plainto_tsquery('english', current_setting('myvars.keyword'))) DESC) AS rank,
    ts_rank(to_tsvector('english', content), plainto_tsquery('english', current_setting('myvars.keyword'))) AS similarity
  FROM wot_chunks_2_5m
  WHERE to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
  LIMIT 200
),
rrf_fusion AS (
  -- 3. Combine results using Reciprocal Rank Fusion
  -- The constant 60 is a standard RRF tuning parameter
  SELECT
    COALESCE(v.id, t.id) AS id,
    COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0) AS rrf_score,
    v.similarity AS vector_similarity,
    t.similarity AS text_similarity
  FROM vector_search v
  FULL OUTER JOIN text_search t ON v.id = t.id
)
-- 4. Return the final sorted results
SELECT
    f.id,
    f.rrf_score,
    f.vector_similarity,
    f.text_similarity
FROM rrf_fusion f
ORDER BY f.rrf_score DESC
LIMIT 100;

-- Actual Query Configuration:
WITH vector_search AS (
  SELECT
    id,
    ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank
  FROM wot_chunks_2_5m
  LIMIT 82
),
text_search AS (
  SELECT
    id,
    ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', content), plainto_tsquery('english', current_setting('myvars.keyword'))) DESC) AS rank
  FROM wot_chunks_2_5m
  WHERE to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
  LIMIT 82
),
rrf_fusion AS (
  SELECT
    COALESCE(v.id, t.id) AS id,
    COALESCE(1.0 / (60 + v.rank), 0.0) + COALESCE(1.0 / (60 + t.rank), 0.0) AS rrf_score
  FROM vector_search v
  FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT
    f.id,
    f.rrf_score,
    w.content,
    w.metadata
FROM rrf_fusion f
JOIN wot_chunks_2_5m w ON f.id = w.id
ORDER BY f.rrf_score DESC
LIMIT 41;


-- ==============================================================================
-- 6. DEEP-DIVE METADATA PROFILING & DATA SKEW
-- ==============================================================================
-- Objective: Map the full cardinality and distribution of the JSONB metadata
-- columns (book_name, chapter_number, chapter_title) to understand filter
-- selectivity before running any planner threshold sweeps.

-- 6.1. Book-Level Distribution
-- For each book: row count, % of dataset, chapter range, and distinct chapter/title counts.
-- Use this to choose representative filters for the planner sweep in Section 8.
SELECT
    metadata->>'book_name'                                             AS book_name,
    COUNT(*)                                                           AS chunk_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 4)                AS pct_of_total,
    MIN((metadata->>'chapter_number')::int)                            AS min_chapter,
    MAX((metadata->>'chapter_number')::int)                            AS max_chapter,
    COUNT(DISTINCT metadata->>'chapter_number')                        AS distinct_chapters,
    COUNT(DISTINCT metadata->>'chapter_title')                         AS distinct_titles
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' IS NOT NULL
GROUP BY metadata->>'book_name'
ORDER BY chunk_count DESC;


-- 6.2. Chapter-Level Distribution Within Each Book
-- Maps every distinct (book_name, chapter_number, chapter_title) triple.
-- Use this to pick specific (book, chapter) pairs for compound-filter experiments (Section 8.3).
SELECT
    metadata->>'book_name'                                             AS book_name,
    (metadata->>'chapter_number')::int                                 AS chapter_number,
    metadata->>'chapter_title'                                         AS chapter_title,
    COUNT(*)                                                           AS chunk_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 6)                AS pct_of_total
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' IS NOT NULL
GROUP BY
    metadata->>'book_name',
    (metadata->>'chapter_number')::int,
    metadata->>'chapter_title'
ORDER BY book_name, chapter_number;


-- 6.3. Total Distinct Combination Count
-- Quick sanity check: how many unique (book, chapter, title) triples exist?
SELECT COUNT(*) AS total_distinct_combos
FROM (
    SELECT DISTINCT
        metadata->>'book_name',
        metadata->>'chapter_number',
        metadata->>'chapter_title'
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' IS NOT NULL
) AS combos;


-- 6.4. Data Skew: Top 10 Most-Populated vs Bottom 10 Least-Populated Chapters
-- Identifies which (book, chapter) pairs will give the most vs least selective filters.
-- The least-populated pairs are most likely to trigger early HNSW abandonment.
(
    SELECT
        'top_10_most'                       AS cohort,
        metadata->>'book_name'              AS book_name,
        (metadata->>'chapter_number')::int  AS chapter_number,
        COUNT(*)                            AS chunk_count
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' IS NOT NULL
    GROUP BY metadata->>'book_name', (metadata->>'chapter_number')::int
    ORDER BY chunk_count DESC
    LIMIT 10
)
UNION ALL
(
    SELECT
        'bottom_10_least'                   AS cohort,
        metadata->>'book_name'              AS book_name,
        (metadata->>'chapter_number')::int  AS chapter_number,
        COUNT(*)                            AS chunk_count
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' IS NOT NULL
    GROUP BY metadata->>'book_name', (metadata->>'chapter_number')::int
    ORDER BY chunk_count ASC
    LIMIT 10
)
ORDER BY cohort, chunk_count DESC;


-- 6.5. Missing / Null Metadata Audit
-- Confirm no rows have partial metadata that could silently skew filter row-count estimates.
SELECT
    COUNT(*)                                                           AS total_rows,
    COUNT(*) FILTER (WHERE metadata IS NULL)                           AS null_metadata,
    COUNT(*) FILTER (WHERE metadata->>'book_name' IS NULL)             AS null_book_name,
    COUNT(*) FILTER (WHERE metadata->>'chapter_number' IS NULL)        AS null_chapter_number,
    COUNT(*) FILTER (WHERE metadata->>'chapter_title' IS NULL)         AS null_chapter_title
FROM wot_chunks_2_5m;


-- ==============================================================================
-- 7. INDEX HEALTH & STORAGE BREAKDOWN
-- ==============================================================================
-- Objective: Understand the on-disk footprint, RAM cost, and actual usage of
-- every index on the table — especially the HNSW vector index.

-- 7.1. All Indexes: Type, Definition, and Disk Footprint
-- Sorted by size descending so the HNSW index appears first.
SELECT
    i.indexname                                              AS index_name,
    i.indexdef                                              AS definition,
    pg_size_pretty(pg_relation_size(c.oid))                 AS index_size_pretty,
    pg_relation_size(c.oid)                                 AS index_size_bytes
FROM pg_indexes i
JOIN pg_class c ON c.relname = i.indexname
WHERE i.tablename = 'wot_chunks_2_5m'
ORDER BY index_size_bytes DESC;


-- 7.2. Table vs. Total Index Size Comparison
-- Shows how much storage each index tier consumes relative to the raw heap.
SELECT
    pg_size_pretty(pg_total_relation_size('wot_chunks_2_5m'))          AS total_with_indexes,
    pg_size_pretty(pg_relation_size('wot_chunks_2_5m'))                 AS heap_only,
    pg_size_pretty(
        pg_total_relation_size('wot_chunks_2_5m')
        - pg_relation_size('wot_chunks_2_5m')
    )                                                                   AS all_indexes_combined;


-- 7.3. Per-Index Usage Statistics
-- idx_scan: times this index was chosen by the planner — low counts reveal unused indexes.
-- idx_tup_read: rows read from the index pages.
-- idx_tup_fetch: rows actually fetched from the heap using index pointers.
SELECT
    s.indexrelname                                          AS index_name,
    s.idx_scan                                              AS times_chosen_by_planner,
    s.idx_tup_read                                          AS index_tuples_read,
    s.idx_tup_fetch                                         AS heap_tuples_fetched,
    pg_size_pretty(pg_relation_size(s.indexrelid))          AS index_size
FROM pg_stat_user_indexes s
WHERE s.relname = 'wot_chunks_2_5m'
ORDER BY s.idx_scan DESC;


-- 7.4. HNSW Graph RAM Overhead Estimate
-- Formula per node (m=16, dim=1536, ~2 average layers):
--   Edge storage : m * 8 bytes per layer * 2 layers = 256 bytes
--   Vector storage: dim * 4 bytes (float32)        = 6,144 bytes
--   Total per node ≈ 6,400 bytes
-- Over 2.5M nodes: ~16 GB  (explains the large work_mem needed during HNSW build).
SELECT
    2500000                                                            AS total_vectors,
    16                                                                 AS m_param,
    1536                                                               AS dimensions,
    pg_size_pretty(2500000 * ((16 * 8 * 2) + (1536 * 4))::bigint)    AS estimated_graph_ram;


-- 7.5. Buffer Cache Hit Rates for Heap and Indexes
-- High hit rates confirm that frequently-accessed index pages are served from shared_buffers.
-- Low rates indicate cold I/O and will inflate query latency for the first runs.
SELECT
    relname                                                            AS table_name,
    heap_blks_read                                                     AS heap_disk_reads,
    heap_blks_hit                                                      AS heap_cache_hits,
    ROUND(
        heap_blks_hit::numeric / NULLIF(heap_blks_hit + heap_blks_read, 0) * 100, 2
    )                                                                  AS heap_cache_hit_pct,
    idx_blks_read                                                      AS index_disk_reads,
    idx_blks_hit                                                       AS index_cache_hits,
    ROUND(
        idx_blks_hit::numeric / NULLIF(idx_blks_hit + idx_blks_read, 0) * 100, 2
    )                                                                  AS index_cache_hit_pct
FROM pg_statio_user_tables
WHERE relname = 'wot_chunks_2_5m';


-- 7.6. Table Bloat & Vacuum Status
-- Dead tuple ratio above ~5% signals the table needs VACUUM ANALYZE before
-- running planner experiments — stale statistics corrupt cost estimates.
SELECT
    n_live_tup                                                         AS live_tuples,
    n_dead_tup                                                         AS dead_tuples,
    ROUND(n_dead_tup::numeric / NULLIF(n_live_tup, 0) * 100, 2)       AS dead_tuple_pct,
    last_vacuum,
    last_autovacuum,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE relname = 'wot_chunks_2_5m';


-- 7.7. Cluster Uptime vs. Stats Reset Detection
-- pg_stat_user_indexes counters reset to zero on every PostgreSQL restart,
-- pg_stat_reset() call, or table rebuild. If cluster_uptime is shorter than
-- the time since your last benchmark run, the idx_scan = 0 readings in 7.3
-- are a stats reset artifact — not evidence the index was unused.
SELECT
    pg_postmaster_start_time()                                AS postgres_started_at,
    now() - pg_postmaster_start_time()                        AS cluster_uptime,
    now()                                                     AS checked_at;


-- 7.8. GIN Index Selectivity for the Current Keyword
-- Run this before any hybrid search experiment (Sections 9 and 10.8) to know
-- how many rows the text CTE will need to fetch and score. The selectivity
-- band predicts text CTE latency and tells you whether the hybrid search
-- bottleneck will dominate total query time for this particular keyword.
--
-- Rule of thumb at 2.5M rows, warm cache:
--   < 0.1%  (~2,500 rows)  → text CTE < 100 ms   → hybrid comparable to vector search
--   0.1–1%  (~25,000 rows) → text CTE ~200 ms     → text CTE noticeable but acceptable
--   1–5%    (~125,000 rows)→ text CTE ~500–1,500 ms→ text CTE dominates
--   > 5%    (>125,000 rows)→ text CTE > 1,500 ms  → keyword too common for useful hybrid search
SELECT
    current_setting('myvars.keyword')                         AS keyword,
    COUNT(*)                                                  AS matching_rows,
    ROUND(COUNT(*) * 100.0 / 2500000, 4)                     AS pct_of_total,
    CASE
        WHEN COUNT(*) * 100.0 / 2500000 < 0.1  THEN 'rare    — fast text CTE  (<100 ms)'
        WHEN COUNT(*) * 100.0 / 2500000 < 1.0  THEN 'moderate — acceptable    (~200 ms)'
        WHEN COUNT(*) * 100.0 / 2500000 < 5.0  THEN 'common  — slow text CTE (~1,500 ms)'
        ELSE                                         'very common — text CTE dominates (>1,500 ms)'
    END                                                       AS selectivity_band
FROM wot_chunks_2_5m
WHERE to_tsvector('english', content)
      @@ plainto_tsquery('english', current_setting('myvars.keyword'));


-- 7.9. HNSW Scan Delivery Check — ef_search Ceiling Effect
-- Reveals how many rows the HNSW index actually produces vs. the requested LIMIT.
-- With ef_search = 40 (default) and a cold buffer cache, the HNSW graph scan
-- may exhaust before reaching a large LIMIT (e.g., LIMIT 200 returns only 100 rows).
-- This silently reduces the vector candidate pool in the hybrid search vector CTE,
-- lowering RRF recall without any error or warning.
--
-- Run twice: once on a cold cache (first run after idle), once after a few warm-up
-- queries to see whether warm cache resolves the delivery gap.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    id,
    ROW_NUMBER() OVER (
        ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    ) AS rank,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 200;
-- Interpretation:
--   actual rows = 200 → HNSW delivered the full request; ef_search is sufficient.
--   actual rows < 200 → ef_search ceiling hit; the hybrid vector CTE gets fewer
--                        candidates than vector_limit = top_k * 2 intended.
--   If rows < 200 on cold cache but = 200 on warm cache → I/O bound, not ef_search bound.
--   If rows < 200 on warm cache too → raise hnsw.ef_search or reduce top_k multiplier.


-- ==============================================================================
-- 8. GRANULAR FILTERED SEARCH & PLANNER THRESHOLD SWEEP
-- ==============================================================================
-- Objective: Find the exact top_k where the planner switches from HNSW to
-- brute-force bitmap scan, and test whether compound filters (lower selectivity)
-- shift that tipping point to a lower LIMIT value.

-- 8.0. Session Variables for Filter Experiments
-- Set these values before running any EXPLAIN blocks in this section.
-- Adjust myvars.filter_book_chapter_chapter to a real chapter number from Section 6.2.

-- HIGH selectivity — largest book (~8.4% of rows). Hypothesis: tipping point at ~42.
SET myvars.filter_book_high           = '06. Lord of Chaos';

-- LOW selectivity — smallest book (~2.6% of rows). Hypothesis: tipping point at ~42.
SET myvars.filter_book_low            = '00. New Spring';

-- VERY LOW selectivity — single chapter within a small book (~0.01-0.1% of rows).
-- Hypothesis: compound filter causes planner to abandon HNSW at a much smaller LIMIT.
SET myvars.filter_compound_book       = '00. New Spring';
SET myvars.filter_compound_chapter    = '1';  -- replace with a real chapter number from 6.2


-- 8.1. Row-Count Confirmation for Each Filter Level
-- Always confirm actual selectivity before running EXPLAIN — it drives everything.
SELECT
    '00. New Spring (single book)'  AS filter_description,
    COUNT(*)                        AS matching_rows,
    ROUND(COUNT(*) * 100.0 / 2500000, 4) AS pct_of_total
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
UNION ALL
SELECT
    '06. Lord of Chaos (single book)',
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / 2500000, 4)
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '06. Lord of Chaos';

-- Confirm compound filter selectivity (replace chapter value as needed).
SELECT
    COUNT(*)                             AS matching_rows,
    ROUND(COUNT(*) * 100.0 / 2500000, 6) AS pct_of_total
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int;


-- ----------------------------------------------------------------------------
-- 8.2. LOW-Selectivity Single-Filter Sweep (book = '00. New Spring', ~2.6%)
-- Watch for: plan changes from "Index Scan using *_embedding_idx" (HNSW)
--            to "Bitmap Heap Scan" + "Sort" (brute-force).
-- Run each block separately and record the switch point.
-- ----------------------------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 10;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 20;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 30;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 35;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 38;

-- Without expression statistics this switches to "Bitmap Heap Scan" + "Sort" (brute-force).
-- Measured timings for both database states: doc/results/eks-20261022-trap-no-stats.txt and eks-20261022-fixed-stats.txt.


-- ----------------------------------------------------------------------------
-- 8.3. HIGH-Selectivity Single-Filter Sweep (book = '06. Lord of Chaos', ~8.4%)
-- Hypothesis: more matching rows → brute-force appears cheaper sooner →
-- planner switches at the same or lower LIMIT than the low-selectivity case.
-- ----------------------------------------------------------------------------


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 20;

-- HNSW


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 30;

-- HNSW


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 35;

-- HNSW

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 37;

-- HNSW


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_high')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 40;

-- Without expression statistics this switches to "Bitmap Heap Scan" + "Sort" (brute-force).
-- Measured timings for both database states: doc/results/eks-20261022-trap-no-stats.txt and eks-20261022-fixed-stats.txt.


-- ----------------------------------------------------------------------------
-- 8.4. VERY LOW-Selectivity Compound Filter Sweep (book AND chapter, ~0.01-0.1%)
-- Hypothesis: the planner abandons HNSW at a much smaller LIMIT because each
-- discarded HNSW candidate requires the graph to explore ~1/selectivity nodes
-- before finding one match, making the HNSW path cost estimate rise sharply.
-- ----------------------------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 5;

-- Compound filter: compare the estimated and actual rows. See doc/results/eks-20261022-compound-filter.txt.

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 10;

-- Compound filter: compare the estimated and actual rows. See doc/results/eks-20261022-compound-filter.txt.


EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 20;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;


-- 8.5. Cost-Only Inspection (no actual execution — fast plan comparison)
-- Use EXPLAIN (COSTS) to read raw planner cost numbers without running the query.
-- Compare "total cost" of the HNSW path vs the Bitmap Heap Scan path at the boundary.
-- The plan that shows the LOWER total cost is what the planner will actually choose.


EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 35;

EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 40;

EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 42;

EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;


-- ==============================================================================
-- 9. HYBRID FILTERED SEARCH DIAGNOSTICS
-- ==============================================================================
-- Objective: Understand how adding a WHERE clause to the vector CTE or text CTE
-- inside a hybrid RRF query affects which indexes the planner chooses and how
-- long each component takes. Adds a metadata filter dimension to the Section 5
-- hybrid search baseline.

-- 9.1. Filtered Text CTE in Isolation
-- Does filtering the text CTE reduce the number of rows fetched from the heap?
-- Key question: can the GIN + B-tree combination prune rows before ts_rank scoring,
-- or does it still fetch all keyword-matching rows regardless of book_name?
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    id,
    ROW_NUMBER() OVER (
        ORDER BY ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) DESC
    ) AS rank,
    ts_rank(
        to_tsvector('english', content),
        plainto_tsquery('english', current_setting('myvars.keyword'))
    ) AS text_score
FROM wot_chunks_2_5m
WHERE
    to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
    AND metadata->>'book_name' = current_setting('myvars.filter_book_low')
LIMIT 100;


-- 9.2. Hybrid RRF: Filter on Text CTE Only (vector CTE stays pure HNSW)
-- Safe approach: the vector CTE has no WHERE clause so HNSW is always used.
-- The filter on the text CTE may reduce the heap rows scored by ts_rank.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH vector_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank,
        1 - (embedding <=> current_setting('myvars.embedding')::vector) AS vector_similarity
    FROM wot_chunks_2_5m
    LIMIT 200
),
text_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY ts_rank(
                to_tsvector('english', content),
                plainto_tsquery('english', current_setting('myvars.keyword'))
            ) DESC
        ) AS rank,
        ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) AS text_score
    FROM wot_chunks_2_5m
    WHERE
        to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
        AND metadata->>'book_name' = current_setting('myvars.filter_book_low')
    LIMIT 200
),
rrf_fusion AS (
    SELECT
        COALESCE(v.id, t.id)                                           AS id,
        COALESCE(1.0 / (60 + v.rank), 0.0)
        + COALESCE(1.0 / (60 + t.rank), 0.0)                          AS rrf_score,
        v.vector_similarity,
        t.text_score
    FROM vector_search v
    FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT id, rrf_score, vector_similarity, text_score
FROM rrf_fusion
ORDER BY rrf_score DESC
LIMIT 50;


-- 9.3. Hybrid RRF: Filter Applied to BOTH CTEs
-- The vector CTE now has a WHERE clause → same HNSW tipping-point risk as Section 8.
-- Compare this plan against 9.2 to quantify the cost of filtering the vector CTE.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH vector_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank,
        1 - (embedding <=> current_setting('myvars.embedding')::vector) AS vector_similarity
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')  -- filter on vector CTE
    LIMIT 200
),
text_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY ts_rank(
                to_tsvector('english', content),
                plainto_tsquery('english', current_setting('myvars.keyword'))
            ) DESC
        ) AS rank,
        ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) AS text_score
    FROM wot_chunks_2_5m
    WHERE
        to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
        AND metadata->>'book_name' = current_setting('myvars.filter_book_low')  -- filter on text CTE
    LIMIT 200
),
rrf_fusion AS (
    SELECT
        COALESCE(v.id, t.id)                                           AS id,
        COALESCE(1.0 / (60 + v.rank), 0.0)
        + COALESCE(1.0 / (60 + t.rank), 0.0)                          AS rrf_score,
        v.vector_similarity,
        t.text_score
    FROM vector_search v
    FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT id, rrf_score, vector_similarity, text_score
FROM rrf_fusion
ORDER BY rrf_score DESC
LIMIT 50;


-- 9.4. Hybrid RRF: Compound Filter on Both CTEs (book AND chapter)
-- Maximum selectivity restriction on a hybrid query. Tests HNSW abandonment
-- in the hybrid context with very few matching rows per CTE.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH vector_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY embedding <=> current_setting('myvars.embedding')::vector) AS rank,
        1 - (embedding <=> current_setting('myvars.embedding')::vector) AS vector_similarity
    FROM wot_chunks_2_5m
    WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
      AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
    LIMIT 200
),
text_search AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY ts_rank(
                to_tsvector('english', content),
                plainto_tsquery('english', current_setting('myvars.keyword'))
            ) DESC
        ) AS rank,
        ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) AS text_score
    FROM wot_chunks_2_5m
    WHERE
        to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
        AND metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
        AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
    LIMIT 200
),
rrf_fusion AS (
    SELECT
        COALESCE(v.id, t.id)                                           AS id,
        COALESCE(1.0 / (60 + v.rank), 0.0)
        + COALESCE(1.0 / (60 + t.rank), 0.0)                          AS rrf_score,
        v.vector_similarity,
        t.text_score
    FROM vector_search v
    FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT id, rrf_score, vector_similarity, text_score
FROM rrf_fusion
ORDER BY rrf_score DESC
LIMIT 50;


-- ==============================================================================
-- 10. WORKAROUND EVALUATION & DIAGNOSTICS
-- ==============================================================================
-- Each approach is tested with:
--   (a) EXPLAIN ANALYZE — to verify whether the planner selects HNSW
--   (b) Actual result query — to collect IDs for accuracy comparison (Section 10.8)
-- All settings are reset after each block to prevent cross-contamination.
-- Run against: WHERE book_name = '00. New Spring' LIMIT 50 (confirmed brute-force zone).

-- ------------------------------------------------------------------------------
-- 10.0. BASELINE — Default plan at the known tipping point
-- This is the problem state. Captures the brute-force plan and result IDs.
-- ------------------------------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

-- Baseline actual results (store these IDs mentally or in a temp table — see Section 10.8).
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;


-- ------------------------------------------------------------------------------
-- 10.1. WORKAROUND: enable_bitmapscan = off
-- Forces the planner away from Bitmap Heap Scan. It typically falls back to a
-- regular B-tree index scan + sort — still brute-force, but avoids the bitmap path.
-- Measured without statistics: B-tree index scan + sort, ~340 ms (warm cache). Still brute force.
-- ------------------------------------------------------------------------------

SET enable_bitmapscan = off;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET enable_bitmapscan;


-- ------------------------------------------------------------------------------
-- 10.2. WORKAROUND: Disable all non-HNSW scan types
-- Removes every alternative except the vector index. PostgreSQL treats enable_*
-- settings as strong preferences, not hard constraints — it may still use a
-- B-tree index scan if it calculates no viable alternative.
-- Measured without statistics: bitmap plan, ~360 ms. enable_indexscan = off also penalises
-- the HNSW scan (it is an index scan), so this cannot force HNSW.
-- ------------------------------------------------------------------------------

SET enable_seqscan    = off;
SET enable_bitmapscan = off;
SET enable_indexscan  = off;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;


-- ------------------------------------------------------------------------------
-- 10.3. WORKAROUND: hnsw.iterative_scan = relaxed_order
-- Gives the HNSW index the mechanical ability to resume scanning in batches
-- past the initial ef_search window. Solves the index-level limitation only.
-- Measured without statistics: bitmap plan, ~360 ms. Iterative scan never activates because
-- it requires the planner to select HNSW first. After ANALYZE: HNSW, 50 rows, ~5 ms.
-- ------------------------------------------------------------------------------

SET hnsw.iterative_scan = relaxed_order;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET hnsw.iterative_scan;


-- ------------------------------------------------------------------------------
-- 10.4. WORKAROUND: iterative_scan + disable all other scan types
-- Combines both approaches from 10.2 and 10.3. Even with no alternatives,
-- the planner may still choose a B-tree index scan over the HNSW index.
-- Measured without statistics: bitmap plan, ~360 ms. Still no HNSW usage.
-- ------------------------------------------------------------------------------

SET hnsw.iterative_scan = relaxed_order;
SET enable_seqscan      = off;
SET enable_bitmapscan   = off;
SET enable_indexscan    = off;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET hnsw.iterative_scan;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET enable_indexscan;


-- ------------------------------------------------------------------------------
-- 10.5. WORKAROUND: iterative_scan strict_order + large ef_search
-- Strict ordering guarantees results in exact distance order; large ef_search
-- gives the HNSW graph a wide enough candidate pool to find 50 filtered results.
-- Still requires the planner to select HNSW — which it typically will not at LIMIT 50.
-- Measured without statistics: bitmap plan, ~360 ms, and the tipping point drops to LIMIT 33.
-- The ef_search change is irrelevant if the planner never enters the HNSW index.
-- ------------------------------------------------------------------------------

SET hnsw.iterative_scan = strict_order;
SET hnsw.ef_search      = 400;

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET hnsw.iterative_scan;
RESET hnsw.ef_search;


-- ------------------------------------------------------------------------------
-- 10.6. WORKAROUND (BEST): CTE Over-Fetch Pattern
-- The inner CTE performs a pure unfiltered vector scan — no WHERE clause means
-- the planner always chooses HNSW. The metadata filter is applied AFTER the HNSW
-- results are materialized. Over-fetch = ceil(top_k / filter_selectivity).
-- For 2.6% selectivity and top_k=50: ceil(50 / 0.026) ≈ 1,924 → use 2,000.
-- Measured: HNSW is used. With iterative scan off the inner scan stops at ef_search candidates
-- and only 18 of 50 rows come back (~2 ms); with hnsw.iterative_scan = relaxed_order, 50 rows in ~7 ms.
-- ------------------------------------------------------------------------------

-- 10.6a. Plan inspection (over-fetch 2000 candidates for top_k=50 at 2.6% selectivity).
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 2000  -- over-fetch: 50 * ceil(1 / 0.026)
)
SELECT id, similarity
FROM nearest
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY similarity DESC
LIMIT 50;

-- 10.6b. Actual results for accuracy comparison (see Section 10.8).
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 2000
)
SELECT id, similarity
FROM nearest
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY similarity DESC
LIMIT 50;

-- 10.6c. Sensitivity: what happens if the over-fetch is halved (1000 instead of 2000)?
-- Expected: returns fewer than 50 rows when over-fetch is too low for the selectivity.
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 1000  -- 50 * 20 — may not yield 50 results at 2.6% selectivity
)
SELECT id, similarity
FROM nearest
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY similarity DESC
LIMIT 50;

-- 10.6d. CTE over-fetch with compound filter (very low selectivity).
-- For a chapter representing ~0.1% of data, over-fetch = ceil(50 / 0.001) = 50,000.
-- At that scale the over-fetch itself becomes a bottleneck — demonstrates the
-- practical limit of this workaround for highly selective compound filters.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 5000  -- adjust based on actual chapter selectivity from Section 8.1
)
SELECT id, similarity
FROM nearest
WHERE metadata->>'book_name'            = current_setting('myvars.filter_compound_book')
  AND (metadata->>'chapter_number')::int = current_setting('myvars.filter_compound_chapter')::int
ORDER BY similarity DESC
LIMIT 50;


-- ------------------------------------------------------------------------------
-- 10.7. WORKAROUND: Raise cpu_operator_cost (session-level planner tuning)
-- Increases the planner's per-operator cost so vector distance is no longer
-- priced like a scalar comparison. Shifts the HNSW vs brute-force break-even
-- point higher, keeping HNSW active at larger LIMIT values.
-- Default: 0.0025. Try: 0.05 (20x), 0.1 (40x), 0.5 (200x).
-- This is a session-only change — reset it after testing.
-- ------------------------------------------------------------------------------

SET cpu_operator_cost = 0.1;  -- 40x the default

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

-- Run at higher top_k to see how far the break-even point shifts.
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 100;

RESET cpu_operator_cost;

-- Try a more conservative multiplier to find the minimum value that keeps HNSW active.
SET cpu_operator_cost = 0.05;  -- 20x

EXPLAIN (COSTS)
SELECT id, 1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;

RESET cpu_operator_cost;


-- ------------------------------------------------------------------------------
-- 10.8. WORKAROUND: Hybrid Filtered Search — CTE Over-Fetch Pattern
-- Applies the over-fetch technique to the vector CTE inside a hybrid RRF query.
-- The text CTE can safely carry the WHERE clause (GIN index, no HNSW to abandon).
-- Vector candidates are over-fetched unfiltered, then filtered post-materialization.
-- ------------------------------------------------------------------------------

EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
WITH vector_candidates AS (
    -- Pure unfiltered HNSW scan — always uses the vector index.
    SELECT
        id,
        1 - (embedding <=> current_setting('myvars.embedding')::vector) AS vector_similarity,
        metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 2000  -- over-fetch; filter applied in the next CTE
),
filtered_vector AS (
    -- Apply metadata filter after HNSW materialization.
    SELECT
        id,
        ROW_NUMBER() OVER (ORDER BY vector_similarity DESC) AS rank,
        vector_similarity
    FROM vector_candidates
    WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
    LIMIT 100
),
text_search AS (
    -- Full-text search with filter — safe because GIN is not subject to the same planner flaw.
    SELECT
        id,
        ROW_NUMBER() OVER (
            ORDER BY ts_rank(
                to_tsvector('english', content),
                plainto_tsquery('english', current_setting('myvars.keyword'))
            ) DESC
        ) AS rank,
        ts_rank(
            to_tsvector('english', content),
            plainto_tsquery('english', current_setting('myvars.keyword'))
        ) AS text_score
    FROM wot_chunks_2_5m
    WHERE
        to_tsvector('english', content) @@ plainto_tsquery('english', current_setting('myvars.keyword'))
        AND metadata->>'book_name' = current_setting('myvars.filter_book_low')
    LIMIT 100
),
rrf_fusion AS (
    SELECT
        COALESCE(v.id, t.id)                                           AS id,
        COALESCE(1.0 / (60 + v.rank), 0.0)
        + COALESCE(1.0 / (60 + t.rank), 0.0)                          AS rrf_score,
        v.vector_similarity,
        t.text_score
    FROM filtered_vector v
    FULL OUTER JOIN text_search t ON v.id = t.id
)
SELECT id, rrf_score, vector_similarity, text_score
FROM rrf_fusion
ORDER BY rrf_score DESC
LIMIT 50;


-- ==============================================================================
-- 11. ACCURACY & RECALL MEASUREMENT
-- ==============================================================================
-- Compares the result sets produced by each workaround against a ground-truth
-- sequential scan. The ground truth is computed by forcing a full seq scan
-- (disable all indexes), which guarantees exact nearest-neighbor ordering.
-- Recall = overlap_count / ground_truth_count * 100.

-- 11.1. Compute ground-truth result set (exact brute-force, no index).
-- WARNING: On 2.5M rows at 1536 dimensions this computes 2.5 million distances; expect it to be slow.
-- Run once, store in a temp table, reuse for all comparisons.

DROP TABLE IF EXISTS gt_results;
CREATE TEMP TABLE gt_results AS
SELECT
    id,
    1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
    ROW_NUMBER() OVER (
        ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    ) AS gt_rank
FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY embedding <=> current_setting('myvars.embedding')::vector
LIMIT 50;
-- Note: because of the WHERE clause and no HNSW at LIMIT=50, this IS the brute-force plan.
-- Verify with EXPLAIN before treating it as ground truth.


-- 11.2. Compute CTE over-fetch result set (Workaround 10.6).
DROP TABLE IF EXISTS cte_results;
CREATE TEMP TABLE cte_results AS
WITH nearest AS (
    SELECT id,
           1 - (embedding <=> current_setting('myvars.embedding')::vector) AS similarity,
           metadata
    FROM wot_chunks_2_5m
    ORDER BY embedding <=> current_setting('myvars.embedding')::vector
    LIMIT 2000
)
SELECT
    id,
    similarity,
    ROW_NUMBER() OVER (ORDER BY similarity DESC) AS cte_rank
FROM nearest
WHERE metadata->>'book_name' = current_setting('myvars.filter_book_low')
ORDER BY similarity DESC
LIMIT 50;


-- 11.3. Recall: How many IDs from the over-fetch workaround appear in ground truth?
SELECT
    (SELECT COUNT(*) FROM gt_results)                                        AS gt_count,
    (SELECT COUNT(*) FROM cte_results)                                       AS cte_count,
    COUNT(*)                                                                 AS overlap_count,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM gt_results), 0), 2) AS recall_pct
FROM gt_results g
JOIN cte_results c ON g.id = c.id;


-- 11.4. Rank displacement: for each overlapping ID, how far is its rank in the
-- CTE result from its rank in the ground truth? High displacement means the
-- workaround returns the right IDs but in a different order.
SELECT
    g.id,
    g.gt_rank,
    c.cte_rank,
    ABS(g.gt_rank - c.cte_rank) AS rank_displacement,
    g.similarity                AS gt_similarity,
    c.similarity                AS cte_similarity,
    ABS(g.similarity - c.similarity) AS similarity_delta
FROM gt_results g
JOIN cte_results c ON g.id = c.id
ORDER BY g.gt_rank;


-- 11.5. Missing results: IDs in ground truth but NOT returned by the CTE workaround.
-- Any row here is a recall miss — the over-fetch multiplier was too small, or the
-- HNSW index did not explore that region of the graph.
SELECT g.id, g.gt_rank, g.similarity AS gt_similarity
FROM gt_results g
LEFT JOIN cte_results c ON g.id = c.id
WHERE c.id IS NULL
ORDER BY g.gt_rank;


-- 11.6. False positives: IDs returned by CTE workaround but absent from ground truth.
-- These rows exist because HNSW is approximate — it may return slightly different
-- neighbors than an exact brute-force scan. Expected for ANN indexes.
SELECT c.id, c.cte_rank, c.similarity AS cte_similarity
FROM cte_results c
LEFT JOIN gt_results g ON c.id = g.id
WHERE g.id IS NULL
ORDER BY c.cte_rank;


-- Debug
-- ==================================================


SELECT tablename, attname, n_distinct, most_common_vals
FROM pg_stats
WHERE tablename = 'wot_chunks_2_5m_expr_idx';


EXPLAIN SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
LIMIT 1;


DROP INDEX wot_chunks_2_5m_expr_idx;
CREATE INDEX wot_chunks_2_5m_expr_idx
    ON wot_chunks_2_5m ((metadata->>'book_name'));


SELECT * FROM pg_stats WHERE tablename = 'wot_chunks_2_5m_expr_idx';

SELECT * FROM pg_stats;


ANALYZE wot_chunks_2_5m;



