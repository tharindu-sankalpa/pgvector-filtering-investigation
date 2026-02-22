# CNPG PostgreSQL Monitoring Setup (Prometheus + Grafana)

This guide sets up the same persistent observability stack used by the Milvus clusters, adapted for the CNPG PostgreSQL pgVector benchmarking cluster.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│           CNPG AKS Cluster (aks-cnpg-pgvector)                      │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                 monitoring namespace                          │   │
│  │  ┌────────────────────┐    ┌────────────────────────────┐   │   │
│  │  │ Prometheus         │    │ Grafana                     │   │   │
│  │  │ - 20Gi PVC         │◄───│ - 10Gi PVC                  │   │   │
│  │  │ - 7d retention     │    │ - Port-forward access       │   │   │
│  │  │ - Node exporter    │    │ - Auto-provisioned dashboard│   │   │
│  │  │ - kube-state-metrics│    └────────────────────────────┘   │   │
│  │  └─────────┬──────────┘                                      │   │
│  └────────────┼─────────────────────────────────────────────────┘   │
│               │ scrapes :9187                                        │
│  ┌────────────▼─────────────────────────────────────────────────┐   │
│  │                 cnpg-pgvector namespace                        │   │
│  │  ┌──────────────────────────────────────────────────────────┐│   │
│  │  │ ml-pgvector-benchmark-1 (PostgreSQL + pgVector)          ││   │
│  │  │ - Built-in CNPG metrics exporter (:9187/metrics)         ││   │
│  │  │ - PodMonitor auto-discovered by Prometheus               ││   │
│  │  │ - Custom queries: cache hit, connections, locks, WAL     ││   │
│  │  └──────────────────────────────────────────────────────────┘│   │
│  └──────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- `aks-cnpg-pgvector` cluster running with CNPG PostgreSQL deployed
- `kubectl` configured with `aks-cnpg-pgvector` context
- `helm` 3.x installed

## Step 1: Switch to CNPG Cluster

```bash
kubectl config use-context aks-cnpg-pgvector
kubectl config current-context
# Should show: aks-cnpg-pgvector
```

## Step 2: Add Helm Repos

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

## Step 3: Deploy Prometheus + Grafana Stack

This uses the same `kube-prometheus-stack` chart as the Milvus clusters, with persistent storage that survives cluster stop/start.

```bash
# Create monitoring namespace
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# Install the stack
helm upgrade --install prometheus prometheus-community/kube-prometheus-stack \
    --namespace monitoring \
    -f infrastructure/cnpg-pgvector/monitoring/values-prometheus-grafana.yaml \
    --wait --timeout 10m
```

### Verify Deployment

```bash
# Check all pods are running
kubectl get pods -n monitoring
# Expected:
#   prometheus-kube-prometheus-operator-xxx     Running
#   prometheus-prometheus-kube-prometheus-prometheus-0  Running
#   prometheus-grafana-xxx                      Running
#   prometheus-kube-state-metrics-xxx           Running
#   prometheus-prometheus-node-exporter-xxx     Running

# Verify PVCs are bound (persistent storage)
kubectl get pvc -n monitoring
# Expected:
#   prometheus-prometheus-kube-prometheus-prometheus-db-...  Bound  20Gi
#   prometheus-grafana                                      Bound  10Gi
```

## Step 4: Deploy CNPG Monitoring Resources

Apply the custom PostgreSQL metrics ConfigMap, the manual PodMonitor, and update the CNPG cluster.

> **Note:** CNPG 1.28+ deprecated `spec.monitoring.enablePodMonitor`. We create a PodMonitor resource manually instead (see `cnpg-podmonitor.yaml`).

```bash
# Apply the custom queries ConfigMap
kubectl apply -f infrastructure/cnpg-pgvector/monitoring/cnpg-extra-monitoring.yaml

# Create the PodMonitor (tells Prometheus to scrape CNPG pod :9187/metrics)
kubectl apply -f infrastructure/cnpg-pgvector/monitoring/cnpg-podmonitor.yaml

# Update the CNPG cluster to reference custom queries
kubectl apply -f infrastructure/cnpg-pgvector/cluster.yaml
```

### Verify CNPG Metrics Export

```bash
# Check PodMonitor was created
kubectl get podmonitor -n cnpg-pgvector
# Should show: ml-pgvector-benchmark

# Port-forward to verify metrics endpoint directly
kubectl port-forward -n cnpg-pgvector pod/ml-pgvector-benchmark-1 9187:9187 &

# Scrape metrics (in another terminal or after backgrounding)
curl -s http://localhost:9187/metrics | head -50
# Should show cnpg_* metrics

# Kill the port-forward
kill %1
```

## Step 5: Import Grafana Dashboard

```bash
# Create the dashboard ConfigMap (auto-loaded by Grafana sidecar)
kubectl create configmap cnpg-pgvector-dashboard \
    --namespace monitoring \
    --from-file=cnpg-pgvector-overview.json=infrastructure/cnpg-pgvector/monitoring/dashboards/cnpg-pgvector-overview.json \
    --dry-run=client -o yaml | kubectl label --local -f - grafana_dashboard=1 -o yaml | kubectl apply -f -
```

## Step 6: Access Grafana

Since we use ClusterIP (to save public IPs), access Grafana via port-forward:

```bash
# Port-forward Grafana to localhost:3000
kubectl port-forward svc/prometheus-grafana -n monitoring 3000:80
```

Then open `http://localhost:3000` in your browser.

**Credentials:**
- Username: `admin`
- Password: `admin123`

Navigate to **Dashboards** → find **"CNPG pgVector Benchmark Overview"**.

## Step 7: Verify Prometheus Targets

Access Prometheus UI to verify all targets are being scraped:

```bash
# Port-forward Prometheus
kubectl port-forward svc/prometheus-kube-prometheus-prometheus -n monitoring 9090:9090
```

Open `http://localhost:9090/targets` and verify:
- `podMonitor/cnpg-pgvector/ml-pgvector-benchmark` → State: UP
- `serviceMonitor/monitoring/...node-exporter` → State: UP
- `serviceMonitor/monitoring/...kube-state-metrics` → State: UP

---

## Dashboard Reference: Metrics & Interpretation Guide

The "CNPG pgVector Benchmark Overview" dashboard is organized into sections that help identify performance bottlenecks during vector search benchmarks and guide optimization decisions (replicas, PgBouncer, resource limits, PG config tuning).

### Row 1: Cluster Health & Key Indicators

Quick-glance stat panels that provide immediate health assessment.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **PostgreSQL** | `cnpg_collector_up` | Whether PostgreSQL is responsive (1=UP, 0=DOWN) | Should always be 1 during benchmarks. 0 means the pod crashed or OOM-killed. |
| **PG Version** | `cnpg_collector_postgres_version` | PostgreSQL version (18.1) | Informational. Confirms pgVector 0.8.1 compatible version. |
| **DB Size** | `cnpg_pg_database_size_bytes` | Disk space consumed by `benchmark_vectors` database | ~1.7GB at 100K rows, ~40GB at 2.5M rows. Watch for unexpected growth. |
| **Cache Hit Ratio** | `cnpg_pg_cache_hit_ratio_ratio` | Fraction of data reads served from shared_buffers vs disk | **Below 95% = critical bottleneck.** At 100K it should be ~100%. At 2.5M, if it drops significantly, `shared_buffers` needs increasing or read replicas can distribute load. |
| **Active Backends** | `cnpg_backends_total` | Number of PostgreSQL backends (connections) actively serving the benchmark DB | Compare against `max_connections` (200). If approaching 150+, consider PgBouncer connection pooling. |
| **Waiting Locks** | `cnpg_pg_locks_waiting_waiting` | Sessions blocked waiting to acquire a lock | **Any non-zero value during retrieval = lock contention.** This directly reduces throughput. Investigate the lock type (row-level, table-level, or advisory). |
| **WAL Buffer Full** | `cnpg_collector_wal_buffers_full` | Cumulative count of times WAL data was flushed because `wal_buffers` was full | High values mean backends stall waiting for WAL I/O. Current value of 50K+ after inserts suggests `wal_buffers` (64MB) may need increasing. |
| **Temp Files Written** | `cnpg_pg_stat_database_temp_bytes` | Total bytes spilled to temporary files (when `work_mem` is exceeded) | **Non-zero = memory pressure.** PostgreSQL couldn't fit sort/hash operations in `work_mem` (256MB) and had to use disk. Increase `work_mem` for better query performance. |

### Row 2: Node Resources (Standard_D16s_v3 — 16 vCPU, 64GB)

Infrastructure-level metrics from the Kubernetes node running the CNPG pod.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **Node CPU by Mode** | `node_cpu_seconds_total` (stacked by mode) | CPU time split into: **User** (application code), **System** (kernel), **I/O Wait** (blocked on disk), **Steal** (VM hypervisor), **Idle** | **Key investigation panel.** If user+system stays at 30-40% during max concurrency retrieval (same as Azure managed PG), it confirms the CPU underutilization problem is inherent to pgVector's single-threaded HNSW scan, not Azure PG infrastructure. **High iowait** = disk is the bottleneck. **High steal** = noisy neighbor or VM throttling. |
| **Node Memory Breakdown** | `node_memory_*` | Total RAM split into: Used, Page Cache, Buffers, Available | The **Page Cache** line is critical — it represents the OS filesystem cache that PostgreSQL uses beyond `shared_buffers` for `effective_cache_size`. At 2.5M scale, if Available drops near zero, the node needs more RAM or fewer pods. |

### Row 3: PostgreSQL Pod Resources (vs Limits)

Container-level metrics showing actual resource consumption against Kubernetes resource limits.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **Pod CPU Usage vs Limits** | `container_cpu_usage_seconds_total`, `kube_pod_container_resource_limits` | CPU cores consumed by the PG pod vs the 8-core request / 16-core limit | If CPU stays at 5-6 cores (30-40%) during concurrent retrieval at concurrency=32, it proves pgVector HNSW index scan doesn't parallelize. This is the exact same pattern seen on Azure managed PG. Adding more CPU won't help — read replicas would, by spreading queries across pods. |
| **Pod Memory Usage vs Limits** | `container_memory_working_set_bytes`, `container_memory_rss`, `kube_pod_container_resource_limits` | Memory consumption vs the 64Gi limit | **Working set** = currently active memory. **RSS** = total resident memory. If working set approaches the limit, the pod will be OOM-killed. At 2.5M scale, watch for memory pressure from: `shared_buffers` (4GB) + `work_mem` × concurrent_queries (256MB × 32 = 8GB) + OS overhead. |

### Row 4: Query & Index Performance (Filtered/Hybrid Search Investigation)

**The most critical section for benchmarking.** These panels directly reveal why filtered and hybrid search performance collapses at higher top_k or scale.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **Sequential Scans vs Index Scans** | `cnpg_pg_seq_scan_vs_idx_scan_seq_scan`, `cnpg_pg_seq_scan_vs_idx_scan_idx_scan` | Rate of sequential (full table) scans vs index (HNSW) scans on `wot_chunks` | **THE most important panel for filtered search.** During pure vector search, index scans should dominate. During filtered search, if PostgreSQL's query planner decides the filter is too broad, it falls back to sequential scan — reading every row in the table. This is exactly why filtered search QPS drops 99% from 1,804 to 14.5. If you see seq scan rate spike during filtered retrieval, the planner is choosing full table scan over the HNSW index. |
| **Tuples Read: Seq Scan vs Index Fetch** | `cnpg_pg_seq_scan_vs_idx_scan_seq_tup_read`, `cnpg_pg_seq_scan_vs_idx_scan_idx_tup_fetch` | Volume of tuples processed per scan type | The magnitude difference matters. If `seq_tup_read` is 100K+ per second while `idx_tup_fetch` is near zero, the HNSW index is being bypassed entirely. At 2.5M rows, seq scan reads 2.5M tuples per query vs HNSW reading ~top_k × ef_search tuples. |
| **Tuple Throughput** | `cnpg_pg_stat_database_tup_returned`, `tup_fetched`, `tup_inserted`, `tup_updated`, `tup_deleted` | Row-level throughput breakdown for `benchmark_vectors` | **Returned/Fetched ratio** is a scan efficiency indicator. High returned with low fetched = PostgreSQL scans many rows but uses few (wasteful). During efficient index scans, returned ≈ fetched. During insert benchmarks, the `inserted/s` rate shows actual ingestion throughput. |
| **Transactions Per Second** | `cnpg_pg_stat_database_xact_commit`, `xact_rollback` | Transaction commit and rollback rates | **Commit rate ≈ benchmark QPS** (each vector query is typically one transaction). Rollbacks indicate query failures, timeouts, or deadlocks. Compare this directly with the benchmark tool's reported QPS. |

### Row 5: Connections & Concurrency (PgBouncer Decision)

Metrics to determine whether connection pooling (PgBouncer) would improve performance.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **Connections by State** | `cnpg_pg_connections_by_state_count` | Breakdown of connections: active, idle, idle in transaction, backend | **High idle count = PgBouncer candidate.** Each idle connection holds ~5-10MB of memory. With `max_connections=200` and concurrency=32, you might see 32 active + 100+ idle from repeated benchmark runs. PgBouncer in transaction-mode pooling reduces the backend connection count. **"Idle in transaction"** is worse — it holds locks and blocks autovacuum. The `max_connections` reference line shows headroom. |
| **Backends Waiting & Lock Contention** | `cnpg_backends_waiting_total`, `cnpg_pg_locks_waiting_waiting`, `cnpg_backends_max_tx_duration_seconds` | Blocked backends + longest active transaction duration | **Waiting backends** = queries queued behind other queries. **Lock waiters** = queries blocked by locks held by other queries. **Long transaction duration** during retrieval indicates a slow query that holds resources. Spikes during concurrent benchmarks point to contention as the throughput ceiling. |

### Row 6: Buffer Cache & I/O (Memory Pressure Detection)

Metrics that reveal whether PostgreSQL has enough memory to serve data from cache or is hitting disk.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **Cache Hit Ratio Over Time** | `cnpg_pg_cache_hit_ratio_ratio` | Buffer cache effectiveness as a time series | At 100K dataset (~1.7GB data + ~830MB indexes), this stays at 100% because everything fits in `shared_buffers` (4GB). **At 2.5M scale, this is THE metric to watch.** The dataset grows to ~40GB, far exceeding 4GB shared_buffers. If the ratio drops below 99%, queries will hit disk for every cache miss, dramatically increasing latency. Solutions: increase `shared_buffers` to 8-16GB, or deploy read replicas so each replica caches a warm subset. |
| **Block Reads vs Cache Hits Rate** | `cnpg_pg_stat_database_blks_hit`, `cnpg_pg_stat_database_blks_read` | Rate of pages served from buffer cache vs read from disk | **Block reads rising = cache misses.** Each 8KB block read from disk is ~100x slower than from cache. During retrieval, if disk reads appear, it means the HNSW index or table data doesn't fit in cache. Cross-reference with Pod Disk I/O to see actual disk throughput. |
| **Temp Files & Buffer Allocations** | `cnpg_pg_stat_database_temp_files`, `temp_bytes`, `cnpg_pg_stat_bgwriter_buffers_alloc` | Temp file spills (work_mem overflow) + buffer allocation rate | **Temp files** = a sort or hash operation exceeded `work_mem` and spilled to disk. Currently 6 temp files / 12.8MB — likely from index creation. If temp files appear during retrieval, increase `work_mem`. **Buffer alloc rate** = how fast new pages are loaded into shared_buffers. High sustained rate = cache churn (working set exceeds cache). |

### Row 7: Checkpoint & WAL (Write Performance)

Metrics related to PostgreSQL's Write-Ahead Log and checkpoint system. Most relevant during insert and index-build benchmarks.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **Checkpoint Activity** | `cnpg_pg_stat_checkpointer_checkpoints_timed`, `checkpoints_req`, `buffers_written` | Checkpoint frequency (timed vs forced) and volume of dirty buffers flushed | **Timed checkpoints** occur every `checkpoint_timeout` (5 min) — normal. **Requested checkpoints** are forced when the WAL grows to `max_wal_size` (1GB) — indicates very heavy writes. During inserts, requested checkpoints slow down because they force a full buffer flush. The `buffers_written` rate shows how much data is being flushed to disk. |
| **WAL Generation Rate** | `cnpg_collector_wal_bytes`, `cnpg_collector_wal_records` | Volume of WAL data generated per second | During insert benchmarks, this will be high (every inserted row generates WAL). **During retrieval benchmarks, this should be near zero** — read-only queries don't generate WAL. If WAL generation appears during retrieval, something unexpected is writing (vacuum, statistics updates). |
| **WAL Buffer Full Events & Archival** | `cnpg_collector_wal_buffers_full`, `cnpg_pg_stat_archiver_archived_count`, `cnpg_pg_stat_archiver_failed_count` | WAL buffer pressure and archival status | **WAL buffer full events** mean backends had to wait for WAL buffers to be flushed before they could write more WAL. The current 50K+ events suggest `wal_buffers` (64MB) was saturated during heavy inserts. Consider increasing to 128MB. **Archive failures** > 0 means backup WAL shipping is failing. |

### Row 8: Table & Database Size

Storage metrics for capacity planning and growth tracking.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **Table Size: Heap vs Indexes** | `cnpg_pg_table_size_table_bytes`, `cnpg_pg_table_size_index_bytes`, `cnpg_pg_table_size_total_bytes` | Breakdown of `wot_chunks` table storage | Currently: ~876MB heap + ~830MB indexes = ~1.7GB total for 100K rows. The HNSW index on 1536-dimensional vectors is almost as large as the data itself. At 2.5M rows, expect ~22GB heap + ~20GB indexes = ~42GB. This exceeds `shared_buffers` (4GB) by 10x, which is why cache hit ratio will drop. |
| **Database Size Over Time** | `cnpg_pg_database_size_bytes`, `cnpg_collector_pg_wal{value="size"}` | Total database size + WAL directory size over time | Track growth during insert benchmarks and verify stability during retrieval. WAL directory size spikes during writes and shrinks as WAL is archived/recycled. Persistent WAL growth means archival isn't keeping up. |

### Row 9: Network & Disk I/O

Infrastructure I/O metrics from the container runtime.

| Panel | Metric Source | What It Shows | What to Watch For |
|-------|--------------|---------------|-------------------|
| **Pod Network I/O** | `container_network_receive_bytes_total`, `container_network_transmit_bytes_total` | Network throughput to/from the PostgreSQL pod | **RX** = incoming queries from the benchmark cluster. **TX** = result data sent back. During retrieval at high concurrency, network should scale linearly with QPS. If TX plateaus while CPU is underutilized, the bottleneck may be network serialization or result set size. Compare with Milvus network patterns for the same workload. |
| **Pod Disk I/O** | `container_fs_reads_bytes_total`, `container_fs_writes_bytes_total` | Disk read/write throughput at the container level | **During retrieval**, disk reads indicate cache misses — data not in shared_buffers or OS page cache. At 100K scale, reads should be near zero. At 2.5M, rising disk reads directly correlate with latency increases. **During inserts/index builds**, high write throughput is expected. |

### Row 10: Key PostgreSQL Configuration (Tuning Reference)

Current PostgreSQL configuration values exposed as metrics. Shows the settings most relevant to benchmark tuning.

| Panel | Setting | Current Value | Tuning Notes |
|-------|---------|---------------|-------------|
| **shared_buffers** | `shared_buffers` | 4GB (524288 × 8KB pages) | PostgreSQL's internal buffer cache. Recommended: 25% of RAM = 16GB for 64GB node. At 2.5M scale, increasing to 8-16GB would significantly improve cache hit ratio. |
| **work_mem** | `work_mem` | 256MB | Per-operation sort/hash memory. Each concurrent query can use this much. At concurrency=32: 256MB × 32 = 8GB potential. If temp files appear, increase. But be cautious: too high × high concurrency = OOM. |
| **effective_cache_size** | `effective_cache_size` | 12GB | Planner hint (not actual allocation). Tells the query planner how much total cache (shared_buffers + OS cache) is available. Should be ~75% of RAM = 48GB. Increasing this makes the planner prefer index scans over seq scans. |
| **max_connections** | `max_connections` | 200 | Maximum client connections. Each connection uses ~5-10MB. With PgBouncer, you can run 1000+ client connections through 50-100 backend connections. |
| **max_parallel_workers** | `max_parallel_workers` | 8 | Total parallel worker processes across all queries. **Important:** pgVector HNSW index scans are NOT parallelizable — this is the fundamental reason for CPU underutilization during vector search. Parallel workers only help for non-vector operations (aggregations, seq scans, hash joins). |
| **max_parallel_workers_per_gather** | `max_parallel_workers_per_gather` | 4 | Maximum workers per individual query. See above — irrelevant for HNSW scans. |
| **wal_buffers** | `wal_buffers` | 64MB | WAL write buffer size. Given 50K+ buffer-full events, consider increasing to 128-256MB for write-heavy workloads (inserts, index builds). |

---

## Custom Metrics Queries (cnpg-extra-monitoring.yaml)

The `cnpg-extra-monitoring.yaml` ConfigMap defines SQL queries that CNPG runs periodically and exposes as Prometheus metrics. These supplement the built-in CNPG exporter metrics.

### Query: `pg_cache_hit_ratio`
```sql
SELECT datname,
  CASE WHEN blks_hit + blks_read = 0 THEN 0
       ELSE blks_hit::float / (blks_hit + blks_read)
  END AS ratio
FROM pg_stat_database
WHERE datname IN ('benchmark_vectors', 'postgres')
```
**Purpose:** Calculates the buffer cache hit ratio (0.0 to 1.0). A ratio of 1.0 means 100% of data reads were served from `shared_buffers` without disk I/O. This is the single most important metric for detecting memory pressure at scale.

### Query: `pg_connections_by_state`
```sql
SELECT COALESCE(state, '') AS state, COUNT(*) AS count
FROM pg_stat_activity
GROUP BY state
```
**Purpose:** Breaks down all connections by their current state (active, idle, idle in transaction, etc.). Helps decide whether PgBouncer connection pooling is needed.

### Query: `pg_locks_waiting`
```sql
SELECT COUNT(*) AS waiting
FROM pg_stat_activity
WHERE wait_event_type = 'Lock'
```
**Purpose:** Counts sessions blocked waiting to acquire a lock. Non-zero during retrieval benchmarks indicates contention that directly reduces throughput.

### Query: `pg_replication_lag_bytes`
```sql
SELECT CASE WHEN pg_is_in_recovery() THEN
    pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())
  ELSE 0 END AS lag_bytes
```
**Purpose:** Measures replication lag in bytes between primary and standby. Relevant when read replicas are added — high lag means replicas serve stale data.

### Query: `pg_tx_commits_rollbacks`
```sql
SELECT datname,
  xact_commit,
  xact_rollback
FROM pg_stat_database
WHERE datname IN ('benchmark_vectors', 'postgres')
```
**Purpose:** Cumulative transaction counts for commit rate (≈ QPS) and rollback rate (≈ error rate) calculations.

### Query: `pg_table_size`
```sql
SELECT relname AS table_name,
  pg_table_size(c.oid) AS table_bytes,
  pg_indexes_size(c.oid) AS index_bytes,
  pg_total_relation_size(c.oid) AS total_bytes
FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r'
  AND c.relname IN ('wot_chunks')
```
**Purpose:** Tracks table heap size vs index size. The HNSW index on 1536-dim vectors is almost as large as the data — this is inherent to the algorithm and explains the ~50% index-to-data ratio.

### Query: `pg_seq_scan_vs_idx_scan`
```sql
SELECT relname AS table_name,
  seq_scan,
  seq_tup_read,
  idx_scan,
  idx_tup_fetch
FROM pg_stat_user_tables
WHERE relname IN ('wot_chunks')
```
**Purpose:** The most critical custom query for filtered search investigation. Compares sequential scan count/volume vs index scan count/volume. If filtered queries trigger sequential scans instead of index scans, the query planner has determined the filter selectivity is too low for the HNSW index to be efficient.

---

## Built-in CNPG Exporter Metrics Reference

These metrics are automatically exposed by the CNPG operator at `:9187/metrics` without any custom queries.

### Cluster Health
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_collector_up` | Gauge | 1 if PostgreSQL is responsive, 0 if down |
| `cnpg_collector_postgres_version` | Gauge | PostgreSQL version with labels `{cluster, full}` |
| `cnpg_collector_fencing_on` | Gauge | 1 if the instance is fenced (isolated from writes) |
| `cnpg_collector_nodes_used` | Gauge | Number of distinct K8s nodes hosting instances (1 = no HA) |
| `cnpg_collector_replica_mode` | Gauge | 1 if the cluster is operating as a replica of another cluster |
| `cnpg_collector_manual_switchover_required` | Gauge | 1 if a manual failover/switchover is needed |

### Connections & Backends
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_backends_total` | Gauge | Number of backends by `{datname, state, usename}` |
| `cnpg_backends_waiting_total` | Gauge | Total backends waiting on other queries |
| `cnpg_backends_max_tx_duration_seconds` | Gauge | Duration of the longest running transaction |

### Database Statistics (`pg_stat_database`)
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_pg_stat_database_xact_commit` | Counter | Committed transactions (cumulative) |
| `cnpg_pg_stat_database_xact_rollback` | Counter | Rolled-back transactions |
| `cnpg_pg_stat_database_blks_hit` | Counter | Buffer cache hits (data served from memory) |
| `cnpg_pg_stat_database_blks_read` | Counter | Disk block reads (cache misses) |
| `cnpg_pg_stat_database_blk_read_time` | Counter | Time spent reading blocks (requires `track_io_timing=on`) |
| `cnpg_pg_stat_database_blk_write_time` | Counter | Time spent writing blocks |
| `cnpg_pg_stat_database_tup_returned` | Counter | Rows returned by queries (includes scanned rows) |
| `cnpg_pg_stat_database_tup_fetched` | Counter | Rows actually used by queries |
| `cnpg_pg_stat_database_tup_inserted` | Counter | Rows inserted |
| `cnpg_pg_stat_database_tup_updated` | Counter | Rows updated |
| `cnpg_pg_stat_database_tup_deleted` | Counter | Rows deleted |
| `cnpg_pg_stat_database_temp_files` | Counter | Number of temp files created (work_mem overflow) |
| `cnpg_pg_stat_database_temp_bytes` | Counter | Total bytes written to temp files |
| `cnpg_pg_stat_database_deadlocks` | Counter | Deadlocks detected |
| `cnpg_pg_stat_database_conflicts` | Counter | Queries cancelled due to recovery conflicts (replica) |

### Database Size & Age
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_pg_database_size_bytes` | Gauge | Disk space used by each database |
| `cnpg_pg_database_xid_age` | Gauge | Transaction ID age (autovacuum freeze indicator) |
| `cnpg_pg_database_mxid_age` | Gauge | Multixact ID age |

### Checkpointer (`pg_stat_checkpointer`)
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_pg_stat_checkpointer_checkpoints_timed` | Counter | Scheduled checkpoints completed |
| `cnpg_pg_stat_checkpointer_checkpoints_req` | Counter | Forced checkpoints (WAL growth exceeded `max_wal_size`) |
| `cnpg_pg_stat_checkpointer_buffers_written` | Counter | Dirty buffers flushed during checkpoints |
| `cnpg_pg_stat_checkpointer_write_time` | Counter | Time spent writing checkpoint buffers (ms) |
| `cnpg_pg_stat_checkpointer_sync_time` | Counter | Time spent syncing checkpoint files to disk (ms) |

### Background Writer (`pg_stat_bgwriter`)
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_pg_stat_bgwriter_buffers_clean` | Counter | Buffers written by background writer |
| `cnpg_pg_stat_bgwriter_maxwritten_clean` | Counter | Times bgwriter stopped due to write limit |
| `cnpg_pg_stat_bgwriter_buffers_alloc` | Counter | New buffers allocated in shared_buffers |

### WAL Statistics
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_collector_wal_bytes` | Gauge | Total WAL generated (cumulative bytes) |
| `cnpg_collector_wal_records` | Gauge | Total WAL records generated |
| `cnpg_collector_wal_fpi` | Gauge | Full page images in WAL (after `full_page_writes`) |
| `cnpg_collector_wal_buffers_full` | Gauge | Times WAL buffers were full (backends had to wait) |
| `cnpg_collector_pg_wal` | Gauge | WAL directory statistics: count, size, keep, min, max |

### WAL Archiver (`pg_stat_archiver`)
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_pg_stat_archiver_archived_count` | Counter | Successfully archived WAL files |
| `cnpg_pg_stat_archiver_failed_count` | Counter | Failed WAL archive attempts |
| `cnpg_pg_stat_archiver_seconds_since_last_archival` | Gauge | Seconds since last successful archive |

### Replication
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_pg_replication_in_recovery` | Gauge | 1 if this instance is a standby (in recovery) |
| `cnpg_pg_replication_lag` | Gauge | Replication lag behind primary (seconds) |
| `cnpg_pg_replication_streaming_replicas` | Gauge | Number of streaming replicas connected |
| `cnpg_pg_replication_is_wal_receiver_up` | Gauge | 1 if the WAL receiver process is running (standby) |
| `cnpg_collector_sync_replicas` | Gauge | Synchronous replica counts: expected, observed, min, max |

### Extensions
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_pg_extensions_update_available` | Gauge | 1 if a newer version of the extension is available |

### PostgreSQL Settings
| Metric | Type | Description |
|--------|------|-------------|
| `cnpg_pg_settings_setting` | Gauge | Current value of every PostgreSQL GUC parameter, labeled by `{name}`. Covers all `pg_settings` entries — useful for tracking configuration drift. |

---

## Optimization Decision Framework

Use the dashboard metrics to guide these optimization decisions:

### 1. Read Replicas — When to Add

**Indicators that read replicas would help:**
- Pod CPU usage is 30-40% (single-connection bottleneck, not total capacity)
- Cache hit ratio is high but QPS is still limited
- Active connections are concentrated on one pod
- Transactions/s plateaus before CPU is saturated

**Why it helps:** Each replica maintains its own `shared_buffers` warm cache and can serve queries independently. For pure vector search (which is single-threaded per query), spreading 32 concurrent queries across 4 replicas = 8 queries per replica = better cache locality and less contention.

### 2. PgBouncer — When to Enable

**Indicators that PgBouncer is needed:**
- High idle connection count (>50) in "Connections by State"
- Connection churn (frequent connect/disconnect from benchmark tool)
- Memory pressure partly from connection overhead
- `max_connections` approaching limit

**Why it helps:** Each PostgreSQL backend uses 5-10MB of memory. PgBouncer maintains a pool of backend connections and multiplexes client connections through them. Transaction-mode pooling is ideal for short benchmark queries.

### 3. Memory Tuning — What to Adjust

| Symptom | Solution |
|---------|----------|
| Cache hit ratio drops below 99% | Increase `shared_buffers` (4GB → 8-16GB) |
| Temp files created during retrieval | Increase `work_mem` (256MB → 512MB) but watch total: work_mem × max_connections |
| OS page cache being evicted | Increase `effective_cache_size` to hint planner (12GB → 48GB) |
| Pod memory approaching limit | Increase pod memory limit or reduce `shared_buffers` + `work_mem` |

### 4. CPU Tuning — Understanding Underutilization

**Key insight:** pgVector HNSW index scans are single-threaded. PostgreSQL's `max_parallel_workers_per_gather` does NOT apply to custom index types. This means:
- A single vector similarity query uses exactly 1 CPU core
- At concurrency=32, maximum theoretical CPU = 32 cores (if no contention)
- In practice, lock contention, cache misses, and I/O reduce effective parallelism
- Adding more CPU cores per pod has diminishing returns; adding replicas is more effective

### 5. WAL & Checkpoint Tuning — Write Performance

| Symptom | Solution |
|---------|----------|
| High WAL buffer full events | Increase `wal_buffers` (64MB → 128-256MB) |
| Frequent requested checkpoints | Increase `max_wal_size` (1GB → 2-4GB) |
| Checkpoint write time spikes | Increase `checkpoint_completion_target` (0.9 is good) |
| High WAL generation during retrieval | Investigate unexpected writes (vacuum, statistics) |

---

## Exploring All Available Metrics

To discover ALL metrics available from CNPG for building more specific dashboards:

```bash
# Port-forward the PostgreSQL metrics endpoint
kubectl port-forward -n cnpg-pgvector pod/ml-pgvector-benchmark-1 9187:9187

# List all CNPG metrics (built-in + custom)
curl -s http://localhost:9187/metrics | grep -E "^cnpg_" | cut -d'{' -f1 | sort -u

# Full metrics dump
curl -s http://localhost:9187/metrics > /tmp/cnpg-metrics.txt
```

## Persistence Across Cluster Stop/Start

Both Prometheus and Grafana use Azure Premium SSD PVCs:

| Component | PVC Size | Storage Class | Retention |
|-----------|----------|---------------|-----------|
| Prometheus | 20Gi | managed-csi-premium | 7 days |
| Grafana | 10Gi | managed-csi-premium | Persistent |

When you stop the AKS cluster (`az aks stop`), the PVCs are retained. When you start it again (`az aks start`), Prometheus and Grafana resume with all historical data and dashboards intact.

## Cleanup

### Remove monitoring stack only
```bash
helm uninstall prometheus -n monitoring
kubectl delete configmap cnpg-pgvector-dashboard -n monitoring
kubectl delete pvc --all -n monitoring
kubectl delete namespace monitoring
```

### Remove CNPG monitoring config only (keep stack)
```bash
kubectl delete configmap cnpg-extra-monitoring -n cnpg-pgvector
kubectl delete podmonitor ml-pgvector-benchmark -n cnpg-pgvector
# Then edit cluster.yaml to remove customQueriesConfigMap
kubectl apply -f infrastructure/cnpg-pgvector/cluster.yaml
```
