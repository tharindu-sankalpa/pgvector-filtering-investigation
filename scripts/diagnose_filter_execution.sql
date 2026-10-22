-- Run on benchmark_vectors after the timed study. Uses one server-local vector.
-- Four actual executions plus two cost-only alternatives; does not clear caches.
-- Session settings and a temporary table only. Each statement is capped at 60s.
-- The +0 alternative deliberately prevents matching HNSW's distance ORDER BY;
-- it is a diagnostic of the competing metadata-filter-and-sort work, not a fix.
SET statement_timeout='60s';
CREATE TEMP TABLE filter_execution_diagnosis(label text,plan jsonb);
DO $diagnosis$
DECLARE embedding text; explanation json; mode text; k int;
BEGIN
 SELECT w.embedding::text INTO embedding FROM public.wot_chunks_2_5m w ORDER BY id LIMIT 1;
 PERFORM set_config('hnsw.ef_search','40',false);
 PERFORM set_config('plan_cache_mode','force_custom_plan',false);
 FOREACH mode IN ARRAY ARRAY['off','strict_order'] LOOP
   PERFORM set_config('hnsw.iterative_scan',mode,false);
   FOREACH k IN ARRAY ARRAY[50,100] LOOP
     EXECUTE format('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT id,1-(embedding <=> %L::vector) AS similarity FROM public.wot_chunks_2_5m WHERE metadata->>''book_name''=''00. New Spring'' ORDER BY embedding <=> %L::vector LIMIT %s',embedding,embedding,k) INTO explanation;
     INSERT INTO filter_execution_diagnosis VALUES(format('ef40_%s_k%s',mode,k),explanation::jsonb);
   END LOOP;
 END LOOP;
 PERFORM set_config('hnsw.iterative_scan','strict_order',false);
 FOREACH k IN ARRAY ARRAY[50,100] LOOP
   EXECUTE format('EXPLAIN (FORMAT JSON) SELECT id,1-(embedding <=> %L::vector) AS similarity FROM public.wot_chunks_2_5m WHERE metadata->>''book_name''=''00. New Spring'' ORDER BY (embedding <=> %L::vector)+0 LIMIT %s',embedding,embedding,k) INTO explanation;
   INSERT INTO filter_execution_diagnosis VALUES(format('metadata_sort_cost_only_k%s',k),explanation::jsonb);
 END LOOP;
END
$diagnosis$;
-- Strip predicates and sort keys to keep the embedding out of exported evidence.
WITH RECURSIVE nodes AS (
 SELECT label,plan->0 AS summary,plan->0->'Plan' AS node FROM filter_execution_diagnosis
 UNION ALL
 SELECT n.label,n.summary,child.node FROM nodes n CROSS JOIN LATERAL
 jsonb_array_elements(COALESCE(n.node->'Plans','[]'::jsonb)) child(node)
), summarized AS (
 SELECT label,max((summary->>'Execution Time')::numeric) AS execution_ms,
  jsonb_agg(jsonb_build_object('node_type',node->>'Node Type','index',node->>'Index Name',
    'startup_cost',node->'Startup Cost','total_cost',node->'Total Cost','estimated_rows',node->'Plan Rows',
    'actual_rows',node->'Actual Rows','removed_by_filter',node->'Rows Removed by Filter',
    'shared_hits',node->'Shared Hit Blocks','shared_reads',node->'Shared Read Blocks')) AS nodes
 FROM nodes GROUP BY label
)
SELECT jsonb_agg(to_jsonb(s) ORDER BY label) FROM summarized s;
RESET plan_cache_mode;
RESET hnsw.ef_search;
RESET hnsw.iterative_scan;
RESET statement_timeout;
