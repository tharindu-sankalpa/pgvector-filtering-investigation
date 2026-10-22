# Benchmark Execution on One EKS Cluster

The benchmark Jobs, vector database, results database and monitoring all run in `pgvector-investigation`, region `ap-south-1`. Complete [CNPG setup](CNPG_PGVECTOR_SETUP.md) and [monitoring](CNPG_MONITORING_SETUP.md) first. Review [dataset schemas](DATASET.md) before loading data.

## 1. Environment and internal connections

```bash
export AWS_DEFAULT_REGION=ap-south-1
export KUBECONFIG="$PWD/.local/kubeconfig"
export TAG=eks-20261022.2  # .1 ran both measured studies; .2 adds ANALYZE to 02_create_indexes.py
```

The vector service is `ml-pgvector-benchmark-rw.cnpg-pgvector.svc.cluster.local:5432`; the results service is `benchmark-results-rw.benchmark-results.svc.cluster.local:5432`. CNPG supplies the credentials. `deploy.py secrets` creates `cnpg-pgvector-secrets` and `pg-results-secrets` in `benchmarks` without printing their values. All timed workloads use these internal services.

## 2. Put the dataset in S3 without a local dataset download

The source URLs are private inputs, not repository configuration. Provide read-only, time-limited HTTPS URLs for the two Parquet files through a Kubernetes Secret named `dataset-source-urls`. Its `SOURCE_FILES_JSON` value is a JSON list with `name`, `url` and exact integer `size`. Do not paste signed URLs into shell history, a ConfigMap, screenshots or Git.

The temporary [copy worker](../infrastructure/aws/source-copy-job.yaml) uses the `dataset-uploader` IRSA service account. The [streaming script](../scripts/aws/stream_to_s3.py) reads source ranges directly into bounded worker memory (four 32 MiB parts) and uploads S3 multipart parts. No dataset bytes pass through the local administrative machine or its filesystem. It verifies object length and independently rereads the S3 object to compare SHA-256. It writes `datasets/SHA256SUMS` after both files pass.

```bash
uv run python scripts/aws/source-secret.py
uv run python scripts/aws/deploy.py copy
kubectl -n benchmarks logs -f job/copy-datasets-to-s3
kubectl -n benchmarks wait job/copy-datasets-to-s3 --for=condition=complete --timeout=3h
```

A failed transfer does not produce a valid checksum manifest. Use a fresh source credential if it has expired. After success, remove the temporary source URL Secret and the upload Job/service account once no longer needed. Keep the read-only benchmark-data-reader service account for dataset staging. S3 stays private.

## 3. Stage S3 files to the EBS cache

```bash
uv run python scripts/aws/deploy.py stage
kubectl -n benchmarks logs -f job/stage-datasets
kubectl -n benchmarks wait job/stage-datasets --for=condition=complete --timeout=1h
kubectl -n benchmarks get pvc benchmark-dataset-pvc
```

The staging Job copies both Parquet files and the checksum manifest to the 80 GiB EBS PVC, then runs `sha256sum -c`. It writes temporary filenames and renames only completed downloads. Do not start timed work until checksums pass. S3 download time is excluded from database benchmarks. Parquet loading remains part of the insert benchmark's measured/reporting behavior; distinguish input-read time from database COPY time in analysis.

## 4. Smoke test before the full workload

First verify the runtime versions and connection paths using the setup guide. Run the integrated smoke Job before a full load:

```bash
uv run python scripts/aws/deploy.py smoke
kubectl -n benchmarks logs -f job/eks-stack-smoke
kubectl -n benchmarks wait job/eks-stack-smoke --for=condition=complete --timeout=30m
```

It verifies Parquet counts/dimensions, extracts 1,000 rows on the AWS worker, and executes the real COPY, index and retrieval entrypoints against `wot_eks_smoke`. It stores explicitly labelled smoke summaries and checks all three index/search modes. Filters may match fewer than top_k rows in the small sample; this run validates execution and connectivity, not retrieval quality at scale. It does not change the original Parquet files and is not a 2.5M-row performance run.

Verify the actual Parquet schemas/row counts on the worker. The intended sizes are 2.5 million vectors and 4,373 queries, with 1,536 dimensions. Results tables are created by the insert/retrieval scripts through `setup_results_tables`.

The COPY insert script **drops and recreates the configured table**. It is appropriate for this isolated benchmark database; check `TABLE_NAME` and the target service before running it. Repeating an insert is a fresh load, not an append.

For the complete study, use one durable EKS Job. It waits for the successful smoke Job and refuses to start while another Job is active:

```bash
uv run python scripts/aws/deploy.py full
# Follow the Job name printed by the command (also saved in .local/full-study.json):
kubectl -n benchmarks logs -f job/<study-job-name>
kubectl -n benchmarks get job <study-job-name>
```

The Job runs COPY, verifies 2,500,000 rows, builds and validates all three index definitions, runs ANALYZE, then executes the 144-scenario retrieval matrix. It pins the benchmark image digest and mounts an immutable copy of its orchestration script. Its run ID, settings, image digests, phase and failure evidence are stored in `benchmark_study_runs` on the results database. It keeps running when the local terminal closes. A failed Job does not automatically restart or erase its evidence; investigate before submitting another fresh load.

```sql
SELECT run_id, phase, started_at, updated_at, finished_at, settings, evidence
FROM benchmark_study_runs ORDER BY started_at DESC;
```

The matrix is 3 patterns × 8 concurrency levels × 6 top_k values = 144 scenarios, each with 4,373 queries (629,712 measured query attempts). `WARMUP_QUERIES=50` is the requested setting; the existing entrypoint rounds down across four top_k values and executes 48 warmup queries. Warmup does not prove the full index is resident in memory. Validate successful-query counts, clipped results and failure logs before publishing.

Do not start another Job that touches the vector database while the study is running; the timings would interfere.

## 5. What the index phase does

The study's index phase runs [02_create_indexes.py](../src/pgvector/02_create_indexes.py). It creates the HNSW index (`m=16`, `ef_construction=64`, `maintenance_work_mem=2GB`), the GIN full-text index on `to_tsvector('english', content)` and the B-tree on `metadata->>'book_name'`, records each build time in `benchmark_index_summary`, and then runs `ANALYZE wot_chunks_2_5m`. That last step collects the statistics for the `book_name` expression. Its log event `expression_statistics_present` lists the indexes that now have statistics; an empty list means the table is in the trap state described in section 6a. `full_study.py` also runs `ANALYZE` after the index phase, so the full study is covered twice.

The script drops named indexes before timing each build, but PostgreSQL generates its own names for expression indexes, so an index-only rerun can leave duplicates. Inspect the catalog before repeating a timing. The full study's COPY recreates the table, so its index build always starts clean. The index script logs an individual failure and continues; `full_study.py` therefore validates all three index definitions before it starts retrieval.

## 6. Rerun the retrieval matrix without reloading

To rerun only the retrieval matrix against a table that is already loaded and indexed, use [retrieval-only-job.yaml](../infrastructure/aws/retrieval-only-job.yaml). It uses the same image, client resources and settings as the full study, so two runs differ only in database state. Copy it to `.local/`, fill the placeholders (account ID, image digest, Job name and a `DATABASE_CONFIG` label that names the database state), then apply it:

```bash
cp infrastructure/aws/retrieval-only-job.yaml .local/retrieval-only-job.yaml
# edit the placeholders in .local/retrieval-only-job.yaml
kubectl -n benchmarks apply -f .local/retrieval-only-job.yaml
kubectl -n benchmarks logs -f job/<job-name>
```

For a short check first, lower `CONCURRENCY_LEVELS`, `TOP_K_VALUES` and `BENCHMARK_NUM_QUERIES` in the copy (for example `1`, `10` and `10`), then restore them for the full run.

The full configuration sweeps concurrency `1,2,4,8,16,32,50,100` and top_k `1,5,10,20,50,100`, with 4,373 query rows per scenario. The three patterns are pure vector, metadata-filtered vector and hybrid vector/full-text search with reciprocal rank fusion. Inspect QPS, p50/p95/p99, timeouts and returned-row counts. A short result set is not a complete nearest-neighbour answer.

The async benchmark sets `ef_search=max(HNSW_EF_SEARCH_MIN, top_k)`, default minimum 64, and defaults filtered searches to `hnsw.iterative_scan=strict_order`. The separate diagnostic sweep defaults to ef_search 40. Record these as distinct experiments; never combine their thresholds into one results table without settings.

## 6a. Reproduce both database states: with and without filter statistics

The article compares two states of the same table, data and indexes. They differ only in whether PostgreSQL has statistics for the expression `metadata->>'book_name'`. PostgreSQL collects those statistics only when `ANALYZE` runs **after** the expression index exists. `CREATE INDEX` does not collect them, and autovacuum's own `ANALYZE` usually ran during the insert, before the index existed.

**Trap state** (what a load followed by `CREATE INDEX` leaves behind): drop and recreate only the metadata B-tree. It builds in about a second.

```sql
DROP INDEX wot_chunks_2_5m_expr_idx;
CREATE INDEX wot_chunks_2_5m_expr_idx ON wot_chunks_2_5m ((metadata->>'book_name'));
-- Verify: 0 rows, and EXPLAIN on a book filter shows rows=12500 (0.5% of 2.5M)
SELECT * FROM pg_stats WHERE tablename = 'wot_chunks_2_5m_expr_idx';
```

**Fixed state:**

```sql
ANALYZE wot_chunks_2_5m;
-- Verify: one row with n_distinct = 16
SELECT attname, n_distinct FROM pg_stats WHERE tablename = 'wot_chunks_2_5m_expr_idx';
```

Nothing writes to the table after loading, so autovacuum does not re-analyze it and the trap state persists until you run `ANALYZE`. Check `pg_stats` before every measurement anyway.

Run the diagnostics in each state and compare:

```bash
# Through the vector port-forward (127.0.0.1:15432), or with kubectl exec into the database pod
psql -h 127.0.0.1 -p 15432 -U benchmark_user -d benchmark_vectors -f scripts/diagnose_statistics_trap.sql
psql -h 127.0.0.1 -p 15432 -U benchmark_user -d benchmark_vectors -f scripts/diagnose_hybrid_components.sql
# Fixed state only; creates and drops an extended statistics object
psql -h 127.0.0.1 -p 15432 -U benchmark_user -d benchmark_vectors -f scripts/diagnose_compound_filter_statistics.sql
```

To rerun the retrieval matrix against an already loaded table, without reloading or rebuilding the HNSW index, use [retrieval-only-job.yaml](../infrastructure/aws/retrieval-only-job.yaml). It uses the same image, client resources and settings as the full study. Fill its placeholders, including a `DATABASE_CONFIG` label that names the database state, then apply it:

```bash
kubectl -n benchmarks apply -f .local/retrieval-only-job.yaml
kubectl -n benchmarks logs -f job/<job-name>
```

In the trap state, filtered searches at top_k 50 and 100 use the bitmap plan, and many queries exceed the harness's budget. Each query gets `POOL_ACQUIRE_TIMEOUT + QUERY_TIMEOUT + 10` = 160 seconds **from the moment the scenario submits it**, including time spent waiting for a concurrency slot. A scenario therefore ends after at most about 160 seconds. Queries that have not finished by then count as failed, and the stored latency percentiles describe the successful queries only. Read the Job log's `queries_failed` and `successful_queries` events together with the summary table.

## 7. Query results and use DBeaver locally

Use the **`-rw` services** to reach each current primary; see [CNPG service semantics](https://cloudnative-pg.io/docs/current/service_management/). CNPG's `-r` service includes all instances; `-ro` targets replicas. Each database currently has one primary and no replicas, so use `-rw` for both DBeaver connections.

Open two terminals from the repository root. Run the relevant command and keep each terminal open:

```bash
# Terminal 1: vector database
kubectl --kubeconfig .local/kubeconfig -n cnpg-pgvector \
  port-forward --address 127.0.0.1 svc/ml-pgvector-benchmark-rw 15432:5432
```

```bash
# Terminal 2: results database
kubectl --kubeconfig .local/kubeconfig -n benchmark-results \
  port-forward --address 127.0.0.1 svc/benchmark-results-rw 15433:5432
```

In k9s, select the corresponding `-rw` service and press **Shift+F**. Choose local port `15432` for vectors and `15433` for results, forwarding remote port `5432`. Local ports must differ because both servers listen on remote port 5432. If a local port is occupied, select another unused port and use it in DBeaver. Reconnect the forward if its target pod is replaced.

Create two **PostgreSQL** connections in DBeaver:

| Field | Vector connection | Results connection |
| --- | --- | --- |
| Connection name | EKS vectors | EKS benchmark results |
| Host | `127.0.0.1` | `127.0.0.1` |
| Port | `15432` | `15433` |
| Database | `benchmark_vectors` | `benchmark_results` |
| User | `benchmark_user` | `results_user` |
| Password Secret namespace | `cnpg-pgvector` | `benchmark-results` |
| Password Secret name | `ml-pgvector-benchmark-app` | `benchmark-results-app` |
| Secret field | `password` | `password` |
| SSL mode | `require` | `require` |

These are application-owner credentials; superuser access is unnecessary for the supplied inspection queries. CNPG's application Secrets also contain `username`, `dbname`, `host` and `port`. The `host` value is an internal service name: use `127.0.0.1` and the local forwarded port in DBeaver.

On macOS, copy each decoded password directly into the clipboard and paste it into its DBeaver connection. The following commands do not print the password:

```bash
kubectl --kubeconfig .local/kubeconfig -n cnpg-pgvector \
  get secret ml-pgvector-benchmark-app -o jsonpath='{.data.password}' \
  | python3 -c 'import base64,sys; sys.stdout.write(base64.b64decode(sys.stdin.buffer.read()).decode())' \
  | pbcopy
```

```bash
kubectl --kubeconfig .local/kubeconfig -n benchmark-results \
  get secret benchmark-results-app -o jsonpath='{.data.password}' \
  | python3 -c 'import base64,sys; sys.stdout.write(base64.b64decode(sys.stdin.buffer.read()).decode())' \
  | pbcopy
```

For another OS, omit `| pbcopy` to display the decoded password privately, or pipe into its clipboard utility. The copied benchmark-worker Secrets in namespace `benchmarks` contain the same connection credentials under `PG_*` (`cnpg-pgvector-secrets`) and `RESULTS_PG_*` (`pg-results-secrets`); use the original CNPG application Secrets above for DBeaver. Keep passwords out of screenshots and Git.

In DBeaver's SSL settings, use PostgreSQL driver property `sslmode=require`, then **Test Connection**. If certificate verification is enabled separately, configure the CNPG CA and a certificate-matching service hostname; a certificate issued for cluster service names will not match `127.0.0.1`. Port-forwarding is local administrative access, not a public database endpoint.

Open the appropriate file in an SQL editor attached to its connection:

| File | Connection | Contents |
| --- | --- | --- |
| [inspect_vector_database.sql](../scripts/inspect_vector_database.sql) | Vectors | Runtime, estimated rows, table/TOAST/index sizes, index definitions/validity, active index/COPY progress, SQL sessions/blockers, index usage, this session's settings |
| [inspect_benchmark_results.sql](../scripts/inspect_benchmark_results.sql) | Results | Study phase/settings/evidence, ingestion and index measurements, retrieval progress, per-scenario QPS/p50/p95/p99, missing/duplicate/short-query cases, all configurations including smoke |
| [perf_audit_pgvector_2.5m_wot.sql](../scripts/perf_audit_pgvector_2.5m_wot.sql) | Vectors | Existing detailed metadata profiles, pure/filtered/hybrid searches, LIMIT sweeps, EXPLAIN plans, workarounds and recall experiments |

Use **Ctrl+Enter** to run the current/selected statement, and keep auto-commit enabled for fresh monitoring snapshots. The two inspection files use read-only statements and avoid full-vector-table scans. The performance audit contains expensive queries, session-setting changes and temporary-table experiments: run selected sections after the timed study completes. It should not be executed wholesale while measuring the benchmark.

`benchmark_study_runs.updated_at` changes at phase transitions, not continuously. Index summaries appear after each index commits; retrieval summaries appear after each scenario finishes. An empty retrieval result during index creation is expected. Failed-query and clipped-result diagnostics are in Job logs; the current summary schema does not persist returned-row counts. DBeaver's HNSW session settings do not reveal the worker connection's session overrides; consult the recorded study settings as well.

## 8. Diagnostic sweeps and article charts

Use the supplied diagnostic scripts inside the worker network or through the local port-forward. Shape private `.env.aws` from [example.env.aws](../example.env.aws). Source it using `set -a; source .env.aws; set +a` after checking its contents. The file remains ignored.

```bash
EF_SEARCH=40 uv run python scripts/cnpg_planner_tipping_point_all_books.py
EF_SEARCH=100 uv run python scripts/cnpg_planner_tipping_point_all_books.py
EF_SEARCH=200 uv run python scripts/cnpg_planner_tipping_point_all_books.py
EF_SEARCH=400 uv run python scripts/cnpg_planner_tipping_point_all_books.py
```

Run the sweep in the database state you want to chart (section 6a); the article's cliff charts use the state without statistics. For filtered-search plans and hybrid query components, use the SQL diagnostics in section 6a.

Use the **same query vector** when comparing adjacent LIMITs. Save the selected plan, cost, BUFFERS, execution time, returned rows, GUCs and cache state. The sweep overwrites its per-ef_search CSV filename; save each complete run under a dated evidence directory before another sweep. Results charts must be regenerated from recorded EKS data rather than edited to imply new measurements.

```bash
uv run python scripts/plot_article_benchmarks.py '<exact database_config from this run>'
uv run python scripts/plot_tipping_point_article.py
python scripts/sync_article.py
```

See [RUN_EVIDENCE.md](RUN_EVIDENCE.md) for the publication gate and [SCREENSHOT_PLAN.md](SCREENSHOT_PLAN.md) for the full AWS/Grafana/DBeaver capture list. The Medium draft retains the full canonical README; publication may need Markdown tables adapted to Medium's editor.

## 9. Failure checks and cleanup

- `Pending` PVC: confirm EBS CSI IAM/add-on health and a matching untainted/tolerated node in the volume's AZ.
- `ImagePullBackOff`: verify ECR URI/tag, amd64 architecture, node-role ECR permissions and outbound DNS/network.
- Connection failure: verify namespace DNS, CNPG readiness and distinct vector/results credentials.
- Dataset failure: check URL expiry, source length, S3 permissions, worker memory and SHA-256 verification.
- OOM: lower Parquet chunk size; avoid loading the 30.94 GB input (28.82 GiB) into RAM as one DataFrame.
- Missing indexes: inspect index logs/catalogs, not only the Job exit status.
- Slow retrieval: correlate the actual plan with I/O/cache/CPU. Do not assume every index scan is HNSW.

Keep logs and result exports before deleting completed Jobs. Retain the dataset cache for repeated runs. Follow the setup guide's explicit teardown procedure when the investigation is complete.
