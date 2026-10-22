# Versions and what they contain

Checked on 22 October 2026.

| Component | Version | How it was verified |
| --- | --- | --- |
| PostgreSQL | 18.6 | `SELECT version()` on the running database: `PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2)` |
| pgvector | 0.8.7 | Built from tag `v0.8.7`, commit `f37c13f68b57d2c3472b2214fbcff699d6d34876` (the build checks the commit); `SELECT extversion FROM pg_extension` returns `0.8.7` |
| CloudNativePG | 1.30.1 (Helm chart 0.29.1) | `helm list -n cnpg-system`; operator image `ghcr.io/cloudnative-pg/cloudnative-pg:1.30.1` |
| Kubernetes (EKS) | 1.36 | `kubectl get nodes` |

These were the newest stable releases on that date. PostgreSQL 18.6 is the newest minor release that CloudNativePG publishes images for; PostgreSQL 19 was not yet released. pgvector's development branch carries an unreleased 0.9.0 section in its changelog; 0.8.7 is the newest tag. Sources: [PostgreSQL versioning](https://www.postgresql.org/support/versioning/), [pgvector tags](https://github.com/pgvector/pgvector/tags), [CloudNativePG releases](https://github.com/cloudnative-pg/cloudnative-pg/releases).

The database image is based on `ghcr.io/cloudnative-pg/postgresql:18.6-system-trixie` and compiles pgvector from source. That base flavour already contains the PGDG `postgresql-18-pgvector 0.8.7` package; the source build replaces its `vector.so` with a build of the same release. For a new image, use the `standard` flavour (the `system` flavour is deprecated) and either rely on its bundled pgvector or keep the source build on the `minimal` flavour.

## What pgvector 0.8.x contains for filtered search

Two features that matter for this investigation arrived in **pgvector 0.8.0**: iterative index scans (`hnsw.iterative_scan`) and improved cost estimation for index selection when filtering. Releases 0.8.1 to 0.8.7 add PostgreSQL 18 support, `EXPLAIN` accounting fixes for PostgreSQL 18, HNSW vacuum and parallel-build correctness fixes, IVFFlat fixes, and hardening. See the [changelog at v0.8.7](https://github.com/pgvector/pgvector/blob/v0.8.7/CHANGELOG.md).

The HNSW cost estimator (`hnswcostestimate` in [`src/hnsw.c`](https://github.com/pgvector/pgvector/blob/v0.8.7/src/hnsw.c)) estimates the work of an HNSW scan from the number of rows, `m` and `hnsw.ef_search`. It does not read `hnsw.iterative_scan`, so turning iterative scanning on or off does not change which plan the planner picks. The distance functions in [`sql/vector.sql`](https://github.com/pgvector/pgvector/blob/v0.8.7/sql/vector.sql) are declared without a `COST`, so the planner charges each 1,536-dimension distance like one ordinary operator call.

## What CloudNativePG does and does not affect

CloudNativePG manages PostgreSQL processes, Kubernetes resources, storage, failover, backups and metrics. It does not take part in query planning. The planner behaviour in the article depends on PostgreSQL, pgvector, the table statistics and the session settings, not on the operator version.
