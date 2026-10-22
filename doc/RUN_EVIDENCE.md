# EKS Run Evidence

What ran, when, on which images and settings, and where the raw evidence is. All runs on 22 October 2026, cluster `pgvector-investigation`, region `ap-south-1`.

## Environment

- Kubernetes 1.36 (EKS); CloudNativePG operator 1.30.1, Helm chart 0.29.1; kube-prometheus-stack chart 92.11.0.
- PostgreSQL 18.6 (`PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2)`, x86_64, GCC 14.2.0); extension `vector` 0.8.7, built from tag `v0.8.7`, commit `f37c13f68b57d2c3472b2214fbcff699d6d34876`. See [VERSION_REVIEW.md](VERSION_REVIEW.md).
- Vector database node: `m6i.4xlarge`, 16 vCPU / 64 GiB; pod requests 12 CPU / 48 GiB, limits 14 CPU / 56 GiB.
- Vector database storage: 200 GiB gp3, 6,000 IOPS / 250 MiB/s, encrypted.
- PostgreSQL settings: `shared_buffers=4GB`, `effective_cache_size=12GB`, `work_mem=256MB`, `maintenance_work_mem=1GB`, `random_page_cost=1.1`, `max_parallel_workers_per_gather=4`.
- Benchmark node `m6i.2xlarge`; services node `m6i.xlarge`. Zero LoadBalancer services.
- Database image digest: `sha256:228be64c6ca136bcceee447c92f7a09140a2cdd26d6bfb70849d7d99cba95e35`.
- Benchmark image digest (both runs): `sha256:fb1c9fb76bcecf265251939e9a1aad2a297e8c0f48abd852195ae4dee082ad4c` (tag `eks-20261022.1`).
- Later tag `eks-20261022.2` adds `ANALYZE` at the end of `02_create_indexes.py`, so the index script on its own also leaves the table with statistics. It was not used for any measured run; both runs above used `eks-20261022.1`, where `full_study.py` ran `ANALYZE`.
- Prometheus: the vector database target and all three node-exporter targets UP.

## Dataset

| Object | Bytes | SHA-256 |
| --- | ---: | --- |
| `datasets/gen_wot_2.5m.parquet` | 30,940,143,579 | `be67e40b513116e3a386b84c41740d4316c083ae5ac02a18a880a9447b4d8561` |
| `datasets/wot_retrieval_queries.parquet` | 40,132,555 | `c6ad7c1fb2085f3f3ebe9359b2a3a3b7576ed3ee525f46c1639bfb36bb933a01` |

An in-cluster worker streamed the source files into S3 multipart uploads and independently read the completed objects back to verify SHA-256. The staging Job verified the same hashes again on the EBS cache. Worker-side checks confirmed 2,500,000 vector rows, 4,373 queries and 1,536 dimensions.

## Run 1: full study, with statistics

- Run ID `20261022T045130Z`, Job `pgvector-study-20261022t045130z`, defined by [full-study-job.yaml](../infrastructure/aws/full-study-job.yaml) and [full_study.py](../scripts/aws/full_study.py).
- 04:51:46 to 07:19:38 UTC. Sequence: COPY → row-count check → `ANALYZE` → three indexes → `ANALYZE` → 144-scenario retrieval matrix.
- Insert: 2,500,000 rows, 1,135 s (binary COPY, 100,000-row Parquet chunks, 5,000-row COPY batches).
- Index builds: HNSW (`m=16`, `ef_construction=64`) 5,775 s, 19 GB; GIN 24 s, 193 MB; B-tree on `metadata->>'book_name'` 1 s, 17 MB.
- Retrieval: 144 of 144 scenarios, **629,712 of 629,712 queries completed**, no timeouts. Every query returned its requested number of rows. Settings: `ef_search = max(64, top_k)` (hybrid `max(64, 2 × top_k)`), filtered `iterative_scan = strict_order`, 48 warmup queries.
- Results label: `EKS 20261022T045130Z | m6i.4xlarge | PG18.6 pgvector0.8.7 | ef=max(64,k); iterative=strict_order`.
- Evidence: [retrieval CSV](results/eks-20261022-retrieval-with-stats.csv), [per-scenario completeness](results/eks-20261022-scenario-completeness-with-stats.json), [plan sweep, 648 plans](results/eks-20261022-plan-sweep-with-stats.json) from [diagnose_filter_plans.sql](../scripts/diagnose_filter_plans.sql), [execution checks](results/eks-20261022-execution-checks-with-stats.json) from [diagnose_filter_execution.sql](../scripts/diagnose_filter_execution.sql).

## Run 2: retrieval matrix, without expression statistics

- Job `pgvector-trap-no-stats-20261022`, defined by [retrieval-only-job.yaml](../infrastructure/aws/retrieval-only-job.yaml). Same image, client resources (4 CPU / 8 GiB requested, 6 CPU / 24 GiB limit), query set and settings as run 1.
- Database state: the B-tree on `metadata->>'book_name'` was dropped and recreated without a following `ANALYZE`, so `pg_stats` had no row for `wot_chunks_2_5m_expr_idx`. Data, HNSW and GIN indexes unchanged from run 1. See [execution-guide.md, section 6a](execution-guide.md#6a-reproduce-both-database-states-with-and-without-filter-statistics).
- 11:15:44 to 12:22:06 UTC.
- Retrieval: 144 of 144 scenarios; **570,579 of 629,712 queries completed; 59,133 timed out**, all in the 16 filtered scenarios at top_k 50 and 100. Each query's budget is 160 s from submission, including queue time. Every other scenario completed all 4,373 queries.
- Results label: `EKS 20261022 no-expression-stats | m6i.4xlarge | PG18.6 pgvector0.8.7 | ef=max(64,k); iterative=strict_order`.
- Evidence: [retrieval CSV](results/eks-20261022-retrieval-no-stats.csv), [per-scenario completeness](results/eks-20261022-scenario-completeness-no-stats.json).

## Diagnostics

All server-side `EXPLAIN (ANALYZE, BUFFERS)`, one fixed query vector (the embedding of the row at `ORDER BY id OFFSET 777`), each timed query run twice with the second run reported.

| File | Database state | Contents |
| --- | --- | --- |
| [eks-20261022-trap-no-stats.txt](results/eks-20261022-trap-no-stats.txt) | No expression statistics | Row estimates, tipping points for ef_search 40/64/100/200/400 × three books, LIMIT 40 vs 41, completeness, seven workarounds, compound filter |
| [eks-20261022-fixed-stats.txt](results/eks-20261022-fixed-stats.txt) | After `ANALYZE` | The same script |
| [eks-20261022-compound-filter.txt](results/eks-20261022-compound-filter.txt) | After `ANALYZE` | Compound filter with and without extended statistics and iterative scan |
| [eks-20261022-hybrid-components.txt](results/eks-20261022-hybrid-components.txt) | After `ANALYZE` | Hybrid query decomposition for keyword `'dragon'` |
| [doc/plots/tipping_point_ef*.csv](plots/) | No expression statistics | 16-book LIMIT sweeps at ef_search 40/100/200/400 |

Scripts: [diagnose_statistics_trap.sql](../scripts/diagnose_statistics_trap.sql), [diagnose_compound_filter_statistics.sql](../scripts/diagnose_compound_filter_statistics.sql), [diagnose_hybrid_components.sql](../scripts/diagnose_hybrid_components.sql), [cnpg_planner_tipping_point_all_books.py](../scripts/cnpg_planner_tipping_point_all_books.py).

The cost comparison quoted at `LIMIT 50` with an estimate of 67,978 rows (HNSW 3,883 against a bitmap-and-sort plan of 59,545) came from an ad hoc `EXPLAIN` on the same table after run 1's `ANALYZE`, forcing the alternative with `enable_indexscan = off`. A similar measurement is in [eks-20261022-execution-checks-with-stats.json](results/eks-20261022-execution-checks-with-stats.json) (HNSW 3,883.52; altered-ordering alternative 59,678.30).

## Key measurements

| Measurement | No statistics | After `ANALYZE` |
| --- | --- | --- |
| Planner estimate, `'00. New Spring'` (actual 63,945) | 12,500 | 62,172 to 67,978 across samples |
| Plan at `LIMIT 41`, ef_search 40 | Bitmap + sort, 358 ms | HNSW, 0.9 ms |
| Plan at `LIMIT 41`, largest book (actual 209,226) | Bitmap + sort, 1.08 s | HNSW, 0.8 ms (0 rows with iterative scan off) |
| First LIMIT without HNSW, ef_search 40/64/100/200/400 | 41 / 39 / 38 / 33 / 25, all books | HNSW up to 200 at every value |
| Benchmark, filtered C:50 top_k 50 | 7,999 ms, 954 of 4,373 completed, 5.95 QPS | 89.93 ms, 4,373 of 4,373, 451 QPS |

## Limits of this evidence

- Single runs; no repetitions or confidence intervals.
- Recall against exact nearest neighbours was not measured. "Completed" means the query returned within its time budget; returned-row counts are in the Job logs and the completeness files.
- Diagnostic timings are warm-cache. A cold-cache measurement was not taken; uncached pages add disk reads to the brute-force plan.
- One query vector for the diagnostics; the benchmark itself uses all 4,373 query rows.

Private command logs and account-specific manifests are under ignored `.local/`.
