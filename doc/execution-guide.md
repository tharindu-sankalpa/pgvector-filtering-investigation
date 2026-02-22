# Benchmark Execution Guide (pgvector / CNPG)

This guide covers everything needed to reproduce the pgvector benchmarks: building the Docker image, provisioning infrastructure, creating secrets, and running benchmark jobs via Helm.

For the CNPG PostgreSQL cluster setup (operator, CRD, LoadBalancer), see [CNPG_PGVECTOR_SETUP.md](./CNPG_PGVECTOR_SETUP.md).
For Prometheus/Grafana monitoring, see [CNPG_MONITORING_SETUP.md](./CNPG_MONITORING_SETUP.md).

---

## 1. Docker Build & Push

Before running any benchmark, you must build the Docker image and push it to your container registry.

**IMPORTANT: There is NO `latest` tag. Always use explicit versioned tags.**

### Prerequisites

- You have an Azure Container Registry (ACR) created.
- **Login to Registry**:

```bash
export $(grep -v '^#' .env.azure | xargs)
docker login $ACR_NAME.azurecr.io --username $ACR_USERNAME --password $ACR_PASSWORD
```

### Build Command

Run this command from the root of the repository:

```bash
export ACR_NAME="benchmarkregistry504646de"
export IMAGE_NAME="benchmark-engine"
export TAG="v5.2.8-wot-8rep"

docker build -t $ACR_NAME.azurecr.io/$IMAGE_NAME:$TAG -f infrastructure/docker/Dockerfile .
```

### Push Command

```bash
docker push $ACR_NAME.azurecr.io/$IMAGE_NAME:$TAG
```

---

## 2. Datasets

The benchmarks use the **Wheel of Time (WoT)** dataset: 1536-dimensional OpenAI embeddings of text chunks from 16 books. Two scales are tested:

| Dataset | Records | File | Size |
|---------|---------|------|------|
| 100K (base) | 100,105 | `wot_chunks_with_embeddings_100pct.parquet` | ~872 MB |
| 2.5M (synthetic) | 2,500,000 | `gen_wot_2.5m.parquet` | ~22 GB |
| Queries | 1,000 | `wot_retrieval_queries.parquet` | ~39 MB |

### Download Links

> **Note:** Replace the placeholder URLs below with your actual Google Drive share links.

| File | Download |
|------|----------|
| `wot_chunks_with_embeddings_100pct.parquet` | [Google Drive](https://drive.google.com/YOUR_LINK_HERE) |
| `wot_retrieval_queries.parquet` | [Google Drive](https://drive.google.com/YOUR_LINK_HERE) |
| `gen_wot_2.5m.parquet` (optional) | [Google Drive](https://drive.google.com/YOUR_LINK_HERE) |

The dataset config file (`data/wot_dataset.yaml`) describes column mappings used by the benchmark scripts:

```yaml
name: wheel_of_time
embedding_column: embedding
text_column: text
filter_columns: [book_name, chapter_title]
query_embedding_column: query_embedding
query_text_column: query_text
```

---

## 3. Benchmark Execution Cluster (AKS)

The benchmarks run as Kubernetes Jobs on a dedicated AKS cluster, separate from the CNPG PostgreSQL cluster. Benchmark pods connect to the CNPG PostgreSQL instance via its LoadBalancer external IP.

### Create the AKS Cluster

```bash
RESOURCE_GROUP="milvus-rg"
LOCATION="northeurope"
AKS_CLUSTER_NAME="benchmark-execution-aks"

az aks create \
    --resource-group $RESOURCE_GROUP \
    --name $AKS_CLUSTER_NAME \
    --location $LOCATION \
    --node-count 1 \
    --node-vm-size Standard_D8s_v3 \
    --generate-ssh-keys

az aks get-credentials --resource-group $RESOURCE_GROUP --name $AKS_CLUSTER_NAME
```

### Create Azure Container Registry (ACR)

```bash
ACR_NAME="benchmarkregistry$(openssl rand -hex 4)"

az acr create \
    --resource-group $RESOURCE_GROUP \
    --name $ACR_NAME \
    --sku Basic

az acr update -n $ACR_NAME --admin-enabled true

ACR_USERNAME=$(az acr credential show --name $ACR_NAME --query "username" -o tsv)
ACR_PASSWORD=$(az acr credential show --name $ACR_NAME --query "passwords[0].value" -o tsv)

kubectl create secret docker-registry acr-secret \
    --docker-server=$ACR_NAME.azurecr.io \
    --docker-username=$ACR_USERNAME \
    --docker-password=$ACR_PASSWORD

kubectl patch serviceaccount default -p '{"imagePullSecrets": [{"name": "acr-secret"}]}'
```

### Verify Context

Always switch to the benchmark execution cluster before running Helm commands:

```bash
kubectl config use-context benchmark-execution-aks
kubectl config current-context
```

---

## 4. Storage: PVC and Dataset Upload

Benchmark pods mount an Azure Files share via a PersistentVolumeClaim (PVC) to access the parquet dataset files.

### Create Storage Account and File Share

```bash
STORAGE_ACCOUNT_NAME="benchdata$(openssl rand -hex 4)"
FILE_SHARE_NAME="benchmark-data"

az storage account create \
    --resource-group $RESOURCE_GROUP \
    --name $STORAGE_ACCOUNT_NAME \
    --location $LOCATION \
    --sku Standard_LRS \
    --kind StorageV2

CONNECTION_STRING=$(az storage account show-connection-string \
    --resource-group $RESOURCE_GROUP \
    --name $STORAGE_ACCOUNT_NAME \
    --output tsv)

az storage share-rm create \
    --resource-group $RESOURCE_GROUP \
    --storage-account $STORAGE_ACCOUNT_NAME \
    --name $FILE_SHARE_NAME \
    --quota 100
```

### Create Azure Storage Secret

```bash
STORAGE_KEY=$(az storage account keys list \
    --resource-group $RESOURCE_GROUP \
    --account-name $STORAGE_ACCOUNT_NAME \
    --query "[0].value" -o tsv)

kubectl create secret generic azure-secret \
    --from-literal=azurestorageaccountname=$STORAGE_ACCOUNT_NAME \
    --from-literal=azurestorageaccountkey=$STORAGE_KEY
```

### Apply PV and PVC

Edit `kube/pvc.yaml` to set your resource group, storage account name, and share name in the `volumeHandle` and `volumeAttributes`, then apply:

```bash
kubectl apply -f kube/pvc.yaml
```

### Upload Dataset Files

```bash
az storage file upload \
    --account-name $STORAGE_ACCOUNT_NAME \
    --share-name $FILE_SHARE_NAME \
    --source wot_chunks_with_embeddings_100pct.parquet \
    --path wot_chunks_with_embeddings_100pct.parquet

az storage file upload \
    --account-name $STORAGE_ACCOUNT_NAME \
    --share-name $FILE_SHARE_NAME \
    --source wot_retrieval_queries.parquet \
    --path wot_retrieval_queries.parquet

az storage file upload \
    --account-name $STORAGE_ACCOUNT_NAME \
    --share-name $FILE_SHARE_NAME \
    --source data/wot_dataset.yaml \
    --path wot_dataset.yaml
```

For the 2.5M dataset (optional, ~22GB):

```bash
az storage file upload \
    --account-name $STORAGE_ACCOUNT_NAME \
    --share-name $FILE_SHARE_NAME \
    --source gen_wot_2.5m.parquet \
    --path gen_wot_2.5m.parquet
```

---

## 5. Kubernetes Secrets

Two secrets are needed on the **benchmark-execution-aks** cluster. Secrets contain only credentials; database names are configured via environment variables in the Helm value files.

### Results Database Secret (`pg-results-secrets`)

Stores credentials for the PostgreSQL database where benchmark metrics (QPS, latency, etc.) are saved.

```bash
kubectl create secret generic pg-results-secrets \
    --from-literal=PG_HOST="<your-postgres-host>.postgres.database.azure.com" \
    --from-literal=PG_PORT="5432" \
    --from-literal=PG_USER="<your-username>" \
    --from-literal=PG_PASSWORD="<your-password>" \
    --dry-run=client -o yaml | kubectl apply -f -
```

### CNPG pgvector Target Secret (`cnpg-pgvector-secrets`)

Stores credentials for the CNPG PostgreSQL cluster being benchmarked (the LoadBalancer IP and the benchmark user password).

```bash
kubectl create secret generic cnpg-pgvector-secrets \
    --from-literal=PG_HOST="<cnpg-loadbalancer-external-ip>" \
    --from-literal=PG_PASSWORD="<benchmark-user-password>" \
    --dry-run=client -o yaml | kubectl apply -f -
```

### Network Access

If your results database is an Azure Database for PostgreSQL with firewall rules, you must whitelist the AKS cluster's outbound IP:

```bash
kubectl run public-ip-check --image=curlimages/curl --restart=Never -- curl -s https://api.ipify.org
kubectl logs -f public-ip-check
kubectl delete pod public-ip-check
```

Add the returned IP to your PostgreSQL server's firewall rules in the Azure Portal (Server > Networking > Firewall rules).

---

## 6. Results Database Setup

Benchmark results are stored in a dedicated PostgreSQL database (`benchmark_results`), separate from the vector data.

### Create the Database

```bash
export $(grep -v '^#' .env.azure | xargs)
psql "host=$PG_HOST port=5432 dbname=postgres user=$PG_USER password=$PG_PASSWORD sslmode=require"
```

```sql
CREATE DATABASE benchmark_results;
CREATE DATABASE vector_data;
\l
```

### Create Summary Tables

Connect to the results database and create the tables (or let the benchmark scripts auto-create them):

```bash
psql "host=$PG_HOST port=5432 dbname=benchmark_results user=$PG_USER password=$PG_PASSWORD sslmode=require"
```

```sql
CREATE TABLE IF NOT EXISTS benchmark_insert_summary (
    id serial PRIMARY KEY,
    run_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
    database_name varchar(100),
    test_location varchar(100),
    dataset_size int,
    database_config text,
    total_records int,
    batch_size int,
    num_batches int,
    total_time_seconds float,
    throughput_vectors_per_sec float,
    avg_batch_time_seconds float,
    median_batch_time_seconds float,
    min_batch_time_seconds float,
    max_batch_time_seconds float
);

CREATE TABLE IF NOT EXISTS benchmark_index_summary (
    id serial PRIMARY KEY,
    run_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
    database_name varchar(100),
    test_location varchar(100),
    dataset_size int,
    database_config text,
    index_type varchar(50),
    index_parameters jsonb,
    table_row_count int,
    total_build_time_seconds float,
    notes text
);

CREATE TABLE IF NOT EXISTS benchmark_retrieval_summary (
    id serial PRIMARY KEY,
    run_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
    database_name varchar(100),
    test_location varchar(100),
    dataset_size int,
    database_config text,
    test_type varchar(50),
    index_type varchar(50),
    top_k int,
    concurrency_level int,
    total_queries int,
    total_duration_seconds float,
    qps float,
    avg_latency_seconds float,
    p50_latency_seconds float,
    p95_latency_seconds float,
    p99_latency_seconds float
);
```

---

## 7. Running Benchmarks

All benchmarks are executed as Kubernetes Jobs using Helm. Ensure you are on the correct context:

```bash
kubectl config use-context benchmark-execution-aks
```

### Common Variables

```bash
export ACR_NAME="benchmarkregistry504646de"
export IMAGE_NAME="benchmark-engine"
export TAG="v5.2.8-wot-8rep"
export NAMESPACE="default"
```

### CNPG pgvector - WoT 100K Vectors

**Prerequisites:**
- CNPG cluster running with LoadBalancer exposed (see [CNPG_PGVECTOR_SETUP.md](./CNPG_PGVECTOR_SETUP.md))
- `cnpg-pgvector-secrets` created on `benchmark-execution-aks` with `PG_HOST` (LoadBalancer IP) and `PG_PASSWORD`
- `pg-results-secrets` exists for results database

> **Architecture:** CNPG runs on a dedicated AKS cluster (`aks-cnpg-pgvector`, Standard_D16s_v3 16vCPU/64GB). Benchmark jobs connect via the LoadBalancer external IP over TLS.

**Insert**
```bash
helm uninstall cnpg-pg-insert-wot -n $NAMESPACE --ignore-not-found && \
helm install cnpg-pg-insert-wot ./kube/charts/benchmark-engine \
    -n $NAMESPACE \
    -f kube/charts/benchmark-engine/examples/insert-cnpg-pgvector.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG
```

**Index Creation**
```bash
helm uninstall cnpg-pg-index-wot -n $NAMESPACE --ignore-not-found && \
helm install cnpg-pg-index-wot ./kube/charts/benchmark-engine \
    -n $NAMESPACE \
    -f kube/charts/benchmark-engine/examples/index-cnpg-pgvector.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG
```

**Retrieval Benchmark (asyncpg)**
```bash
helm uninstall cnpg-pg-retrieval-asyncpg-wot -n $NAMESPACE --ignore-not-found && \
helm install cnpg-pg-retrieval-asyncpg-wot ./kube/charts/benchmark-engine \
    -n $NAMESPACE \
    -f kube/charts/benchmark-engine/examples/retrieval-cnpg-pgvector-asyncpg.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG
```

### CNPG pgvector - WoT 2.5M Vectors

**Prerequisites:**
- 100K benchmark completed first (to validate connectivity)
- `cnpg-pgvector-secrets` and `pg-results-secrets` exist on `benchmark-execution-aks`
- 2.5M dataset generated and uploaded to Azure Files (see Section 4)

> Uses chunked loading (`INSERT_CHUNK_SIZE: "100000"`) to stream data from disk in 100K-row chunks, keeping memory under 8GB.

**Insert (COPY Binary -- recommended for 2.5M)**
```bash
helm uninstall cnpg-pg-insert-copy-wot-2m5 -n $NAMESPACE --ignore-not-found && \
helm install cnpg-pg-insert-copy-wot-2m5 ./kube/charts/benchmark-engine \
    -n $NAMESPACE \
    -f kube/charts/benchmark-engine/examples/insert-cnpg-pgvector-copy.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG
```

**Insert (Standard INSERT -- slower, for fair cross-DB comparison)**
```bash
helm uninstall cnpg-pg-insert-wot-2m5 -n $NAMESPACE --ignore-not-found && \
helm install cnpg-pg-insert-wot-2m5 ./kube/charts/benchmark-engine \
    -n $NAMESPACE \
    -f kube/charts/benchmark-engine/examples/insert-cnpg-pgvector.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG
```

**Index Creation**
```bash
helm uninstall cnpg-pg-index-wot-2m5 -n $NAMESPACE --ignore-not-found && \
helm install cnpg-pg-index-wot-2m5 ./kube/charts/benchmark-engine \
    -n $NAMESPACE \
    -f kube/charts/benchmark-engine/examples/index-cnpg-pgvector.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG
```

**Retrieval Benchmark (asyncpg)**
```bash
helm uninstall cnpg-pg-retrieval-asyncpg-wot-2m5 -n $NAMESPACE --ignore-not-found && \
helm install cnpg-pg-retrieval-asyncpg-wot-2m5 ./kube/charts/benchmark-engine \
    -n $NAMESPACE \
    -f kube/charts/benchmark-engine/examples/retrieval-cnpg-pgvector-asyncpg.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG
```

### Monitor Progress

```bash
kubectl get jobs -n $NAMESPACE -w
kubectl get pods -n $NAMESPACE -l app=benchmark-engine
kubectl logs -f <pod-name> -n $NAMESPACE
```

### Cleanup

```bash
helm uninstall <release-name> -n $NAMESPACE
```

---

## 8. Viewing Results

### Query Results via psql

```bash
export $(grep -v '^#' .env.azure | xargs)
PGSSLMODE=require PGDATABASE=benchmark_results psql
```

**Retrieval results:**
```sql
SELECT database_name, test_type, top_k, concurrency_level,
       ROUND(avg_latency_seconds::numeric * 1000, 2) as avg_latency_ms,
       ROUND(p99_latency_seconds::numeric * 1000, 2) as p99_latency_ms,
       ROUND(qps::numeric, 2) as qps
FROM benchmark_retrieval_summary
WHERE database_name LIKE '%CNPG%'
ORDER BY id DESC
LIMIT 20;
```

**Insert results:**
```sql
SELECT database_name, dataset_size, total_records,
       ROUND(total_time_seconds::numeric, 2) as total_time_s,
       ROUND(throughput_vectors_per_sec::numeric, 2) as vectors_per_sec
FROM benchmark_insert_summary
WHERE database_name LIKE '%CNPG%'
ORDER BY id DESC;
```

**Index creation results:**
```sql
SELECT database_name, index_type, index_parameters,
       table_row_count,
       ROUND(total_build_time_seconds::numeric, 2) as build_time_s
FROM benchmark_index_summary
WHERE database_name LIKE '%CNPG%'
ORDER BY id DESC;
```

### Plot Results

Use the plotting script to generate publication-quality charts:

```bash
# Requires matplotlib, pandas, psycopg2-binary
export $(grep -v '^#' .env.azure | xargs)
uv run python scripts/plot_article_benchmarks.py
```

This produces charts in `doc/plots/` showing QPS and latency across all search scenarios, top_k values, and concurrency levels.

---

## Quick Reference: CNPG Benchmark Files

| Action | Values File | Notes |
|--------|-------------|-------|
| Insert (standard) | `examples/insert-cnpg-pgvector.yaml` | Row-by-row INSERT |
| Insert (COPY binary) | `examples/insert-cnpg-pgvector-copy.yaml` | 10-50x faster, recommended for 2.5M |
| Index creation | `examples/index-cnpg-pgvector.yaml` | HNSW + B-tree + GIN indexes |
| Retrieval (asyncpg) | `examples/retrieval-cnpg-pgvector-asyncpg.yaml` | Vector, Filtered, Hybrid search |

---

## Troubleshooting

| Issue | Symptom | Fix |
|:------|:--------|:----|
| **DB Connection Timeout** | Logs show `Connection timed out` | Check Azure PostgreSQL Firewall rules. Whitelist the AKS outbound IP (see Section 5). |
| **OOMKilled** | Pod status is `OOMKilled` | Increase memory limits in the example YAML file (e.g., `memory: 8Gi`). |
| **Image Pull Error** | `ErrImagePull` or `ImagePullBackOff` | Verify `ACR_NAME` variable. Ensure `acr-secret` exists and the service account is patched. |
| **Results Not Saved** | Benchmark runs but no results appear | Check `RESULTS_PG_DATABASE` is set. Verify `pg-results-secrets` contains valid credentials. |
| **Query File Not Found** | `FileNotFoundError` for parquet | Ensure file is uploaded to Azure Files and PVC is mounted correctly. |
