# Screenshot capture list

Every screenshot the article and the LinkedIn carousel embed, with the exact filename, when to take it, and what must be visible. Capture real interfaces only; never use an image model for console or EXPLAIN evidence.

Before publishing any capture, mask account IDs, ARNs' account segments, email addresses, public IPs and passwords. Keep unmasked originals under `.local/screenshots/` (ignored by Git).

## A. AWS console (`doc/screenshots/aws/`)

Keep the AWS header, region selector (Asia Pacific (Mumbai)) and left navigation visible in each capture: the page should read as the AWS console.

| # | File | Where | What must be visible |
|---|---|---|---|
| A1 | `01-eks-cluster-overview.png` | EKS → Clusters → `pgvector-investigation` → Overview | Cluster name, status Active, Kubernetes version 1.36, region |
| A2 | `02-eks-node-groups.png` | EKS → cluster → Compute | Three node groups `vector-db`, `benchmarks`, `services` with instance types m6i.4xlarge, m6i.2xlarge, m6i.xlarge, desired size 1 each |
| A3 | `03-eks-workloads.png` | EKS → cluster → Resources → Pods, filter by namespace | Pods in `cnpg-pgvector`, `benchmark-results`, `monitoring`, `cnpg-system` running |
| A4 | `04-ebs-volumes.png` | EC2 → Volumes, filter by tag `kubernetes.io/created-for/pvc/namespace` | The 200 GiB gp3 volume with 6,000 IOPS / 250 MiB/s, encrypted, same AZ as the others |
| A5 | `05-ecr-repositories.png` | ECR → Private repositories → `pgvector-investigation/postgresql` | Tag `18.6-pgvector0.8.7`, digest, size |
| A6 | `06-s3-dataset-objects.png` | S3 → dataset bucket → `datasets/` | `gen_wot_2.5m.parquet` (30.9 GB), `wot_retrieval_queries.parquet`, `SHA256SUMS`, encryption column |

## B. Grafana (`doc/screenshots/grafana/`)

Open Grafana with `kubectl --kubeconfig .local/kubeconfig -n monitoring port-forward svc/prometheus-grafana 3000:80`, dashboard **CNPG pgvector overview**. Set the time range explicitly (top right) so it appears in the capture. Prometheus keeps 7 days, so both windows are available until 29 October 2026.

| # | File | Time range (UTC, 22 Oct 2026) | What must be visible |
|---|---|---|---|
| B1 | `g1-fixed-state-benchmark.png` | 06:47 – 07:20 | The retrieval matrix with statistics: CPU, sequential vs index scans, cache hit ratio, buffer reads |
| B2 | `g2-trap-state-benchmark.png` | 11:15 – 12:23 | Same panels during the run without statistics: the jump in CPU and heap block reads during the top_k 50/100 filtered scenarios |

The pair should use the same panels and the same panel order so they can be shown side by side.

## C. DBeaver (`doc/screenshots/dbeaver/`)

**Use [`scripts/dbeaver_screenshots.sql`](../scripts/dbeaver_screenshots.sql).** It contains every capture below in order, with the state switches, the exact SQL and, for each query, the file name and what must be visible. Open it in one DBeaver editor tab and run it top to bottom with Ctrl+Enter.

Connect as described in [execution-guide.md](execution-guide.md#7-query-results-and-use-dbeaver-locally) (vector connection, `127.0.0.1:15432`, `benchmark_vectors`). Keep the SQL editor (with the `SET` lines) and the plan output both in view, so readers see the session settings. Use the **Execution Plan as text** output (`EXPLAIN (ANALYZE, BUFFERS) …`), not DBeaver's graphical plan.

Use one fixed query vector for every capture:

```sql
-- Run once per editor tab; DBeaver keeps one session per tab.
-- Then replace :qvec below with the subquery, or use DBeaver's @set variable.
SELECT embedding FROM wot_chunks_2_5m ORDER BY id OFFSET 777 LIMIT 1;
```

The simplest form inlines it:

```sql
SET hnsw.ef_search = 40;
SET hnsw.iterative_scan = off;

EXPLAIN (ANALYZE, BUFFERS)
SELECT id FROM wot_chunks_2_5m
WHERE metadata->>'book_name' = '00. New Spring'
ORDER BY embedding <=> (SELECT embedding FROM wot_chunks_2_5m ORDER BY id OFFSET 777 LIMIT 1)
LIMIT 41;
```

Run each query twice and capture the second run, so the screenshot compares plans rather than cold and warm caches.

### State 1: the trap (no expression statistics)

Set it with:

```sql
DROP INDEX wot_chunks_2_5m_expr_idx;
CREATE INDEX wot_chunks_2_5m_expr_idx ON wot_chunks_2_5m ((metadata->>'book_name'));
```

| # | File | Query | What must be visible |
|---|---|---|---|
| C1 | `d01-trap-no-statistics.png` | `SELECT * FROM pg_stats WHERE tablename = 'wot_chunks_2_5m_expr_idx';` then `EXPLAIN SELECT id FROM wot_chunks_2_5m WHERE metadata->>'book_name' = '00. New Spring';` | Zero rows from `pg_stats`; the plan's `rows=12500` |
| C2 | `d02-trap-limit-40-hnsw.png` | ef_search 40, iterative off, New Spring, `LIMIT 40` | `Index Scan using wot_chunks_2_5m_embedding_idx`, estimated `rows=12500`, execution time in ms, actual rows returned (fewer than 40) |
| C3 | `d03-trap-limit-41-bitmap.png` | Same, `LIMIT 41` | `Bitmap Heap Scan` + `Sort`, estimated `rows=12500` against actual `rows=63945`, execution time. **This is the key image of the article.** |
| C4 | `d04-trap-limit-41-largest-book.png` | Same, `'06. Lord of Chaos'`, `LIMIT 41` | Actual `rows=209226`, execution time |
| C5 | `d05-trap-ef400-limit-25.png` | `SET hnsw.ef_search = 400`, New Spring, `LIMIT 25` | Bitmap plan already at LIMIT 25 |
| C6 | `d06-trap-iterative-scan-ignored.png` (used in `doc/workarounds.md`) | `SET hnsw.iterative_scan = relaxed_order`, New Spring, `LIMIT 50` | Still `Bitmap Heap Scan`: iterative scan never runs |
| C7 | `d07-cte-overfetch-iterative-off.png` (used in `doc/workarounds.md`) | The CTE over-fetch from the article (inner `LIMIT 2000`), iterative off | HNSW used, but `rows=18` returned out of 50 |
| C8 | `d08-cte-overfetch-iterative-relaxed.png` (used in `doc/workarounds.md`) | Same with `SET hnsw.iterative_scan = relaxed_order` | HNSW used, 50 rows returned, execution time |

### State 2: fixed (statistics collected)

Set it with:

```sql
ANALYZE wot_chunks_2_5m;
```

| # | File | Query | What must be visible |
|---|---|---|---|
| C9 | `d09-analyze-statistics.png` | `ANALYZE wot_chunks_2_5m;` then `SELECT attname, n_distinct, most_common_vals, most_common_freqs FROM pg_stats WHERE tablename = 'wot_chunks_2_5m_expr_idx';` | `n_distinct = 16`, the book names and their frequencies |
| C10 | `d10-fixed-limit-41-hnsw.png` | ef_search 40, iterative off, New Spring, `LIMIT 41` | HNSW `Index Scan`, estimated rows between about 60,000 and 68,000 (it varies with each `ANALYZE` sample), execution time |
| C11 | `d11-fixed-limit-100-strict-order.png` | `SET hnsw.ef_search = 100; SET hnsw.iterative_scan = strict_order;` New Spring, `LIMIT 100` | HNSW, `rows=100` returned, `Rows Removed by Filter`, execution time |
| C12 | `d12-compound-filter-estimate.png` | `EXPLAIN ANALYZE` with `metadata->>'book_name' = '00. New Spring' AND metadata->>'chapter_number' = '5'`, `LIMIT 10` | Estimated rows against actual 4,129 |
| C13 | `d13-indexes.png` | The index-size query in [inspect_vector_database.sql](../scripts/inspect_vector_database.sql) | Four indexes with access method and size (HNSW about 19 GB) |
| C14 | `d14-pure-vector-hnsw.png` | No filter, `LIMIT 100` | HNSW `Index Scan`, execution time |
| C15 | `d15-hybrid-text-search.png` | The text-search CTE alone for `'dragon'` (from [diagnose_hybrid_components.sql](../scripts/diagnose_hybrid_components.sql)) | GIN `Bitmap Index Scan` finding all matches, then heap scan and sort over all of them |

Leave the database in State 2 when finished: it is the correct production configuration.

## D. Crops for the LinkedIn carousel (`doc/screenshots/crops/`)

Cut after the originals are reviewed. Keep the console header or DBeaver toolbar in the crop.

| Crop | From | Content |
|---|---|---|
| `02-eks-node-groups.png` | A2 | The node-group table with the header |
| `d03-trap-limit-41-bitmap.png` | C3 | The plan output from `Limit` to `Execution Time` |
| `d10-fixed-limit-41-hnsw.png` | C10 | The plan output from `Limit` to `Execution Time` |

## Capture log

| Group | Captured (UTC) | Masked | Reviewed |
|---|---|---|---|
| A. AWS console | | | |
| B. Grafana | | | |
| C. DBeaver, state 1 | | | |
| C. DBeaver, state 2 | | | |
| D. Crops | | | |
