-- Cost-only plan comparison on benchmark_vectors; no EXPLAIN ANALYZE.
-- Uses one server-local dataset vector; no vector is exported in output.
-- Session settings and temporary table only; no persistent data/index changes.
-- This isolates ef_search and iterative_scan, including limits above ef_search.
-- It also compares forced custom and generic prepared plans because asyncpg
-- prepares parameterized queries. These are post-run diagnostics, not captured
-- historical benchmark plans. Blocks below must run in one session.
SET statement_timeout='60s';
CREATE TEMP TABLE filter_plan_diagnosis (
    ef_search int, iterative_scan text, plan_mode text, book_name text,
    top_k int, plan jsonb
);
DO $diagnosis$
DECLARE
    embedding text;
    book text;
    ef int;
    k int;
    iterative text;
    mode text;
    explanation json;
BEGIN
    SELECT w.embedding::text INTO embedding FROM public.wot_chunks_2_5m w ORDER BY id LIMIT 1;
    FOREACH ef IN ARRAY ARRAY[40,64,100] LOOP
      PERFORM set_config('hnsw.ef_search',ef::text,false);
      FOREACH iterative IN ARRAY ARRAY['off','strict_order'] LOOP
        PERFORM set_config('hnsw.iterative_scan',iterative,false);
        FOREACH mode IN ARRAY ARRAY['force_custom_plan','force_generic_plan'] LOOP
          PERFORM set_config('plan_cache_mode',mode,false);
          FOREACH book IN ARRAY ARRAY['00. New Spring','13. Towers of Midnight','01. The Eye of the World'] LOOP
            FOREACH k IN ARRAY ARRAY[10,20,37,38,40,41,50,64,65,100,101,150,200,500,1000,2000,5000,10000] LOOP
              EXECUTE format('PREPARE filter_diagnosis (vector,text) AS SELECT id,1-(embedding <=> $1) AS similarity FROM public.wot_chunks_2_5m WHERE metadata->>''book_name''=$2 ORDER BY embedding <=> $1 LIMIT %s',k);
              EXECUTE format('EXPLAIN (FORMAT JSON) EXECUTE filter_diagnosis (%L::vector,%L)',embedding,book) INTO explanation;
              INSERT INTO filter_plan_diagnosis VALUES(ef,iterative,mode,book,k,explanation::jsonb);
              DEALLOCATE filter_diagnosis;
            END LOOP;
          END LOOP;
        END LOOP;
      END LOOP;
    END LOOP;
END
$diagnosis$;
-- Recursive traversal captures actual index/scan nodes without serializing the
-- plan's vector constant. It also drops predicates, query text and filter values
-- beyond the public book label, so no embeddings are saved in this export.
WITH RECURSIVE nodes AS (
  SELECT ef_search,iterative_scan,plan_mode,book_name,top_k,
         plan->0->'Plan' AS node FROM filter_plan_diagnosis
  UNION ALL
  SELECT n.ef_search,n.iterative_scan,n.plan_mode,n.book_name,n.top_k,child.node
  FROM nodes n CROSS JOIN LATERAL jsonb_array_elements(COALESCE(n.node->'Plans','[]'::jsonb)) child(node)
), summarized AS (
  SELECT ef_search,iterative_scan,plan_mode,book_name,top_k,
    bool_or(node->>'Index Name'='wot_chunks_2_5m_embedding_idx') AS uses_hnsw,
    jsonb_agg(jsonb_build_object('node_type',node->>'Node Type','index',node->>'Index Name',
      'startup_cost',node->'Startup Cost','total_cost',node->'Total Cost','rows',node->'Plan Rows')) AS nodes
  FROM nodes GROUP BY ef_search,iterative_scan,plan_mode,book_name,top_k
)
SELECT jsonb_agg(to_jsonb(s) ORDER BY ef_search,iterative_scan,plan_mode,book_name,top_k)
FROM summarized s;
RESET plan_cache_mode;
RESET hnsw.ef_search;
RESET hnsw.iterative_scan;
RESET statement_timeout;
