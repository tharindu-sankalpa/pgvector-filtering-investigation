# CloudNativePG with pgVector - Self-Hosted Setup Guide

This guide walks through setting up a self-hosted PostgreSQL cluster with pgVector on a **dedicated AKS cluster** using CloudNativePG (CNPG) operator. This allows benchmarking pgVector on self-hosted infrastructure to compare with Azure Managed PostgreSQL results.

## Goal

Validate whether the pgVector limitations discovered on Azure Managed PostgreSQL also exist on self-hosted CNPG:
- **Hybrid search collapse**: 2,096 QPS at 100K → 83 QPS at 2.5M (96% drop)
- **Filtered search cliff**: 1,804 QPS at top_k=10 → 14.5 QPS at top_k≥50 (99% drop)
- **CPU underutilization**: Only 30-40% CPU under max load

## Prerequisites

- Azure CLI (`az`) installed and logged in
- Azure Container Registry (ACR) access
- `kubectl` CLI installed
- `helm` 3.x installed

## Step 0: Create Dedicated AKS Cluster

We create a dedicated AKS cluster in **North Europe** using **Standard_D16s_v3** nodes (16 vCPU, 64GB RAM) which matches the Azure Managed PostgreSQL D16ds_v5 configuration for fair comparison.

### Check Available Quota

```bash
# Verify DSv3 family quota in North Europe (should show available vCPUs)
az vm list-usage --location northeurope --output table | grep "DSv3"
# Expected: Standard DSv3 Family vCPUs - need at least 16 available
```

### Create Resource Group and AKS Cluster

```bash
# Set variables
export RESOURCE_GROUP="milvus-rg"
export CLUSTER_NAME="aks-cnpg-pgvector"
export LOCATION="northeurope"
export NODE_VM_SIZE="Standard_D16s_v3"  # 16 vCPU, 64GB RAM
export ACR_NAME="benchmarkregistry504646de"

# Create AKS cluster in existing resource group
az aks create \
    --resource-group $RESOURCE_GROUP \
    --name $CLUSTER_NAME \
    --location $LOCATION \
    --node-count 1 \
    --node-vm-size $NODE_VM_SIZE \
    --enable-managed-identity \
    --generate-ssh-keys \
    --network-plugin azure \
    --network-policy azure \
    --enable-addons monitoring \
    --attach-acr $ACR_NAME

# Get credentials for kubectl
az aks get-credentials --resource-group $RESOURCE_GROUP --name $CLUSTER_NAME --overwrite-existing

# Verify connection
kubectl get nodes
# Should show: aks-nodepool1-xxxxx Ready <roles> <age> <version>

# Verify node specs
kubectl describe node | grep -E "cpu:|memory:|kubernetes.io/hostname"
```

## Architecture

The CNPG cluster hosts only the PostgreSQL database. Benchmark jobs run from the existing `benchmark-execution-aks` cluster, connecting via the exposed LoadBalancer IP.

```
┌─────────────────────────────────────────────────────────────────────────┐
│           CNPG AKS Cluster (aks-cnpg-pgvector)                          │
│           Location: North Europe | Node: Standard_D16s_v3               │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                     cnpg-system namespace                          │  │
│  │  ┌──────────────────────────────────────────────────────────────┐ │  │
│  │  │ CNPG Operator (manages PostgreSQL lifecycle)                  │ │  │
│  │  └──────────────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                              │                                           │
│  ┌───────────────────────────▼───────────────────────────────────────┐  │
│  │                   cnpg-pgvector namespace                          │  │
│  │  ┌──────────────────────────────────────────────────────────────┐ │  │
│  │  │ PostgreSQL Cluster (ml-pgvector-benchmark)                    │ │  │
│  │  │ - Primary Pod: 8 vCPU / 32GB (limit 16 vCPU / 64GB)          │ │  │
│  │  │ - pgVector extension enabled                                  │ │  │
│  │  │ - 100GB Azure Premium SSD                                     │ │  │
│  │  └──────────────────────────────────────────────────────────────┘ │  │
│  │                              │                                     │  │
│  │  ┌──────────────────────────▼──────────────────────────────────┐  │  │
│  │  │ LoadBalancer Service (External IP)                           │  │  │
│  │  │ - Exposes PostgreSQL on port 5432                            │  │  │
│  │  └──────────────────────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ TCP/5432
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│           Benchmark Execution AKS Cluster (benchmark-execution-aks)     │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │              benchmark-execution namespace                         │  │
│  │  ┌──────────────────────────────────────────────────────────────┐ │  │
│  │  │ Benchmark Jobs (Insert → Index → Retrieval)                   │ │  │
│  │  │ - Connects to CNPG via LoadBalancer IP                        │ │  │
│  │  │ - Results stored in Azure PostgreSQL (benchmark_results)      │ │  │
│  │  └──────────────────────────────────────────────────────────────┘ │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Step 1: Build PostgreSQL + pgVector Docker Image

```bash
# Set variables
export ACR_NAME="benchmarkregistry504646de"
export PG_IMAGE_TAG="18.1-pgvector"

# Build the image
cd /home/tharindu/repos/exp-ai-search-migration-evaluation
docker build -t $ACR_NAME.azurecr.io/postgresql-pgvector:$PG_IMAGE_TAG \
    -f infrastructure/cnpg-pgvector/Dockerfile .

# Push to ACR
az acr login --name $ACR_NAME
docker push $ACR_NAME.azurecr.io/postgresql-pgvector:$PG_IMAGE_TAG
```

## Step 2: Install CNPG Operator

```bash
# Ensure you're connected to the new AKS cluster (from Step 0)
kubectl config current-context
# Should show: aks-cnpg-pgvector

# Add CNPG Helm repo
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo update

# Create namespace for CNPG operator
kubectl create namespace cnpg-system

# Install CNPG operator
helm install cnpg-operator cnpg/cloudnative-pg \
    --namespace cnpg-system \
    --set monitoring.podMonitorEnabled=false

# Verify operator is running
kubectl get pods -n cnpg-system
# Should show: cnpg-cloudnative-pg-xxx Running
```

## Step 3: Deploy PostgreSQL Cluster with pgVector

```bash
# Create namespace for the PostgreSQL cluster
kubectl create namespace cnpg-pgvector

# Update the image name in cluster.yaml if needed
# Then deploy the cluster
kubectl apply -f infrastructure/cnpg-pgvector/cluster.yaml

# Watch cluster come up (takes 2-5 minutes)
kubectl get cluster -n cnpg-pgvector -w

# Check pod status
kubectl get pods -n cnpg-pgvector
# Should show: ml-pgvector-benchmark-1 Running

# Check cluster status
kubectl describe cluster ml-pgvector-benchmark -n cnpg-pgvector
```

## Step 4: Expose PostgreSQL via LoadBalancer

Since benchmark jobs run from a separate AKS cluster (`benchmark-execution-aks`), we need to expose PostgreSQL via a LoadBalancer with an external IP.

```bash
# Create LoadBalancer service to expose PostgreSQL externally
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: ml-pgvector-benchmark-lb
  namespace: cnpg-pgvector
spec:
  type: LoadBalancer
  ports:
    - port: 5432
      targetPort: 5432
      protocol: TCP
  selector:
    cnpg.io/cluster: ml-pgvector-benchmark
    role: primary
EOF

# Wait for external IP assignment (takes 1-2 minutes)
kubectl get svc ml-pgvector-benchmark-lb -n cnpg-pgvector -w
# Wait until EXTERNAL-IP changes from <pending> to an IP address

# Get the external IP
export CNPG_EXTERNAL_IP=$(kubectl get svc ml-pgvector-benchmark-lb -n cnpg-pgvector \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
echo "PostgreSQL External IP: $CNPG_EXTERNAL_IP"
```

## Step 5: Get Connection Credentials

```bash
# Get the superuser password (auto-generated by CNPG)
export CNPG_SUPERUSER_PASSWORD=$(kubectl get secret ml-pgvector-benchmark-superuser -n cnpg-pgvector \
    -o jsonpath='{.data.password}' | base64 -d)
echo "Superuser password: $CNPG_SUPERUSER_PASSWORD"

# Get the app user password
export CNPG_APP_PASSWORD=$(kubectl get secret ml-pgvector-benchmark-app -n cnpg-pgvector \
    -o jsonpath='{.data.password}' | base64 -d)
echo "App user password: $CNPG_APP_PASSWORD"

# Connection details for benchmarking:
echo "
================================================
CNPG PostgreSQL Connection Details
================================================
Host:     $CNPG_EXTERNAL_IP
Port:     5432
Database: benchmark_vectors
User:     benchmark_user (or postgres for superuser)
Password: $CNPG_APP_PASSWORD
================================================
"
```

## Step 6: Verify pgVector Extension

```bash
# Connect via the external IP (using credentials from Step 5)
PGPASSWORD=$CNPG_SUPERUSER_PASSWORD psql -h $CNPG_EXTERNAL_IP -U postgres -d benchmark_vectors

# In psql, verify pgVector
\dx
# Should show: vector | 0.x.x | public | vector data type and ivfflat and hnsw access methods

# Test vector operations
CREATE TABLE test_vectors (id serial PRIMARY KEY, embedding vector(1536));
INSERT INTO test_vectors (embedding) VALUES ('[1,2,3,...]'::vector);
SELECT * FROM test_vectors ORDER BY embedding <=> '[1,2,3,...]' LIMIT 5;

# Clean up test
DROP TABLE test_vectors;
\q
```

## Step 7: Run Benchmarks

Benchmarks run from the existing **benchmark-execution-aks** cluster, connecting to CNPG via the LoadBalancer external IP.

### Switch to Benchmark Execution Cluster

```bash
# Switch kubectl context to benchmark execution cluster
kubectl config use-context benchmark-execution-aks

# Verify context
kubectl config current-context
# Should show: benchmark-execution-aks
```

### Update Helm Values with CNPG Connection

Update the benchmark Helm values files with the CNPG external IP and password:

```bash
# Get the values from Step 4 and 5 (run on CNPG cluster first if not set)
echo "CNPG_EXTERNAL_IP: $CNPG_EXTERNAL_IP"
echo "CNPG_APP_PASSWORD: $CNPG_APP_PASSWORD"
```

Example values file for CNPG benchmarks:

```yaml
# kube/charts/benchmark-engine/examples/retrieval-cnpg-pgvector.yaml
benchmark:
  script: "03_retrieval_asyncpg.py"
  scriptPath: "/app/src/pgvector"
  databaseName: "PostgreSQL CNPG (Self-Hosted)"

config:
  database:
    pg_host: "<CNPG_EXTERNAL_IP>"  # Replace with actual IP from Step 4
    pg_port: "5432"
    pg_database: "benchmark_vectors"
    pg_user: "benchmark_user"
    pg_password: "<CNPG_APP_PASSWORD>"  # Replace with password from Step 5
  
  dataset:
    name: "wot_chunks_2_5m"
    path: "/data/wot_chunks_2_5m.parquet"
  
  retrieval:
    test_types: ["vector", "filtered", "hybrid"]
    concurrency_levels: [1, 10, 50, 100]
    top_k_values: [1, 5, 10, 20, 50, 100]
    queries_per_test: 1000
```

### Run Benchmark Jobs

```bash
# Ensure you're on the benchmark-execution-aks cluster
kubectl config use-context benchmark-execution-aks

# Set benchmark image variables
export ACR_NAME="benchmarkregistry504646de"
export IMAGE_NAME="benchmark-engine"
export TAG="v5.2.6-wot-8rep"  # Get current tag from doc/execution-guide.md

# 1. Run Insert Benchmark (load data)
helm uninstall insert-cnpg-pgvector --ignore-not-found
helm install insert-cnpg-pgvector ./kube/charts/benchmark-engine \
    -f kube/charts/benchmark-engine/examples/insert-cnpg-pgvector.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG

# 2. Run Index Creation (after insert completes)
helm uninstall index-cnpg-pgvector --ignore-not-found
helm install index-cnpg-pgvector ./kube/charts/benchmark-engine \
    -f kube/charts/benchmark-engine/examples/index-cnpg-pgvector.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG

# 3. Run Retrieval Benchmark (after index completes)
helm uninstall retrieval-cnpg-pgvector --ignore-not-found
helm install retrieval-cnpg-pgvector ./kube/charts/benchmark-engine \
    -f kube/charts/benchmark-engine/examples/retrieval-cnpg-pgvector-asyncpg.yaml \
    --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
    --set image.tag=$TAG
```

### Option B: Direct Script Execution (for quick local testing)

```bash
# Set environment variables with external IP
export PG_HOST="$CNPG_EXTERNAL_IP"
export PG_PORT="5432"
export PG_DATABASE="benchmark_vectors"
export PG_USER="benchmark_user"
export PG_PASSWORD="$CNPG_APP_PASSWORD"

# Run insert benchmark first (to load data)
uv run python src/pgvector/01_insert_benchmark.py

# Run index creation
uv run python src/pgvector/02_create_indexes.py

# Run retrieval benchmark
uv run python src/pgvector/03_retrieval_asyncpg.py
```

## Step 8: Compare Results

After running benchmarks, compare CNPG results with Azure Managed PostgreSQL:

| Metric | Azure Managed PG | CNPG Self-Hosted | Difference |
|--------|------------------|------------------|------------|
| Vector Search QPS (2.5M) | 1,353 | TBD | |
| Filtered Search QPS (top_k=10) | 1,804 | TBD | |
| Filtered Search QPS (top_k=50) | 14.5 | TBD | |
| Hybrid Search QPS (2.5M) | 83 | TBD | |
| CPU Utilization | 30-40% | TBD | |
| HNSW Index Build Time | 2.8 hrs | TBD | |

## Scaling Options

### Add Read Replicas
```bash
# Edit cluster to increase instances
kubectl edit cluster ml-pgvector-benchmark -n cnpg-pgvector
# Change: instances: 1 → instances: 3

# This creates 1 primary + 2 replicas
# Use read-only service for read scaling:
# ml-pgvector-benchmark-ro.cnpg-pgvector.svc.cluster.local
```

### Increase Resources
```bash
# Edit cluster resources
kubectl edit cluster ml-pgvector-benchmark -n cnpg-pgvector
# Increase CPU/memory limits
```

### Enable PgBouncer Connection Pooling
```yaml
# Add to cluster.yaml spec:
pooler:
  instances: 2
  type: rw
  pgbouncer:
    poolMode: transaction
    parameters:
      max_client_conn: "1000"
      default_pool_size: "25"
```

## Cleanup

### Option A: Delete PostgreSQL Cluster Only (keep AKS for future use)

```bash
# Switch to CNPG cluster
kubectl config use-context aks-cnpg-pgvector

# Delete PostgreSQL cluster
kubectl delete cluster ml-pgvector-benchmark -n cnpg-pgvector

# Delete LoadBalancer service
kubectl delete svc ml-pgvector-benchmark-lb -n cnpg-pgvector

# Delete namespace
kubectl delete namespace cnpg-pgvector

# Uninstall CNPG operator
helm uninstall cnpg-operator -n cnpg-system
kubectl delete namespace cnpg-system
```

### Option B: Delete Entire AKS Cluster (full cleanup - recommended after benchmarking)

```bash
# Set variables (same as Step 0)
export RESOURCE_GROUP="milvus-rg"
export CLUSTER_NAME="aks-cnpg-pgvector"

# Delete the AKS cluster (keeps other resources in milvus-rg)
az aks delete --resource-group $RESOURCE_GROUP --name $CLUSTER_NAME --yes --no-wait

# Remove kubectl context
kubectl config delete-context $CLUSTER_NAME

# Switch back to benchmark execution cluster
kubectl config use-context benchmark-execution-aks
```

## Troubleshooting

### Pod not starting
```bash
kubectl describe pod ml-pgvector-benchmark-1 -n cnpg-pgvector
kubectl logs ml-pgvector-benchmark-1 -n cnpg-pgvector
```

### Image pull errors
```bash
# Ensure ACR credentials are configured
kubectl create secret docker-registry acr-secret \
    --docker-server=$ACR_NAME.azurecr.io \
    --docker-username=$ACR_USERNAME \
    --docker-password=$ACR_PASSWORD \
    -n cnpg-pgvector

# Add imagePullSecrets to cluster.yaml
```

### Connection refused
```bash
# Check service exists
kubectl get svc -n cnpg-pgvector

# Check endpoints
kubectl get endpoints ml-pgvector-benchmark-rw -n cnpg-pgvector
```

## References

- [CloudNativePG Documentation](https://cloudnative-pg.io/documentation/)
- [pgVector GitHub](https://github.com/pgvector/pgvector)
- [CNPG Cluster Reference](https://cloudnative-pg.io/documentation/current/cloudnative-pg.v1/)
- [Nexus CNPG Architecture](https://ifsdev.atlassian.net/wiki/x/JwAwiw)
