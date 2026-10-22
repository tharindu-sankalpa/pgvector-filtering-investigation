# CloudNativePG and pgvector on Amazon EKS

This guide builds one EKS cluster in Mumbai, two CNPG database clusters, and the supporting infrastructure for the benchmark. Automation creates the resources; each stage has explicit verification commands. Use [execution-guide.md](execution-guide.md) for datasets and benchmark Jobs, [CNPG_MONITORING_SETUP.md](CNPG_MONITORING_SETUP.md) for metrics, and [DATASET.md](DATASET.md) for schema details.

EC2 workers, the EKS control plane, EBS volumes, S3 objects, ECR storage and public IPv4 addresses incur charges. Three fixed on-demand workers stay running until deleted. Review the cost and teardown section before creating resources.

## 0. Tools and variables

Install AWS CLI v2, eksctl, kubectl, Helm, Docker with Buildx, Python 3.11+, and uv. Log in to AWS CLI. The scripts never require static AWS keys in Git. Use the existing ECR registry in your account, with dedicated repositories `pgvector-investigation/postgresql` and `pgvector-investigation/benchmark-engine`.

```bash
export AWS_DEFAULT_REGION=ap-south-1
export EKS_CLUSTER=pgvector-investigation
export KUBECONFIG="$PWD/.local/kubeconfig"
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export ECR_REGISTRY="$ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com"
export DATASET_BUCKET="pgvector-investigation-$ACCOUNT_ID-$AWS_DEFAULT_REGION"
export TAG=eks-20261022.2  # .1 ran both measured studies; .2 adds ANALYZE to 02_create_indexes.py
mkdir -p .local
chmod 700 .local
uv sync
```

`.local/` contains private kubeconfig, rendered manifests, logs and account identifiers. It is ignored. Do not copy its contents into a public article. Every published command uses placeholders or derives the account ID at runtime.

Stable software selected on 22 October 2026: PostgreSQL **18.6**, pgvector **0.8.7**, CNPG **1.30.1**. PostgreSQL 19 is a release candidate, so it is excluded. [PostgreSQL versions](https://www.postgresql.org/support/versioning/), [pgvector changelog](https://github.com/pgvector/pgvector/blob/v0.8.7/CHANGELOG.md), [CNPG release](https://github.com/cloudnative-pg/cloudnative-pg/releases/tag/v1.30.1).

## 1. Review cluster shape and create EKS

The [eksctl configuration](../infrastructure/aws/eks-cluster.yaml) selects Kubernetes 1.36, managed EC2 node groups, one worker AZ, and no NAT gateway. The control-plane subnet layout covers two AZs. Public worker subnets provide outbound registry/package access; no database or monitoring LoadBalancer is created. SSH access is not enabled. The API has private access as well as authenticated public access; restrict its public CIDRs to your administrative IPs if appropriate, keeping private access enabled so nodes can reach it.

| Node group | Instance | Count | Placement |
| --- | --- | --- | --- |
| `vector-db` | m6i.4xlarge, 16 vCPU / 64 GiB | 1 | Vector database only; tainted |
| `benchmarks` | m6i.2xlarge, 8 vCPU / 32 GiB | 1 | Jobs and dataset staging; tainted |
| `services` | m6i.xlarge, 4 vCPU / 16 GiB | 1 | CNPG operator, results database, monitoring |

```bash
aws eks describe-cluster-versions --region "$AWS_DEFAULT_REGION"
eksctl create cluster --config-file infrastructure/aws/eks-cluster.yaml \
  --kubeconfig "$KUBECONFIG" --write-kubeconfig --set-kubeconfig-context=false
aws eks update-kubeconfig --name "$EKS_CLUSTER" --region "$AWS_DEFAULT_REGION" \
  --kubeconfig "$KUBECONFIG" --alias "$EKS_CLUSTER"
kubectl get nodes -L workload,node.kubernetes.io/instance-type,topology.kubernetes.io/zone
```

The version must be offered by EKS in this region and supported by the installed eksctl. Capture the EKS console while the control plane is creating, then again when the control plane and all three node groups are Active. Do not begin application deployment until nodes are Ready.

## 2. Build and push both images

Create the two dedicated ECR repositories if they do not already exist. Do not reuse unrelated repositories or overwrite their tags.

```bash
aws ecr create-repository --repository-name pgvector-investigation/postgresql \
  --image-scanning-configuration scanOnPush=true
aws ecr create-repository --repository-name pgvector-investigation/benchmark-engine \
  --image-scanning-configuration scanOnPush=true
bash scripts/aws/build-images.sh
```

The database Dockerfile builds pgvector from tag `v0.8.7` and checks source commit `f37c13f68b57d2c3472b2214fbcff699d6d34876`. `OPTFLAGS=""` avoids compiling only for the build machine CPU. Both images target **linux/amd64**, matching the m6i workers even when built on an Apple Silicon laptop. The final database image excludes the compiler toolchain. The benchmark image uses `uv.lock`; run `uv lock` when intentionally changing dependencies. The Python pgvector client version is independent of the database extension version.

```bash
aws ecr describe-images --repository-name pgvector-investigation/postgresql \
  --image-ids imageTag=18.6-pgvector0.8.7 --query 'imageDetails[].{digest:imageDigest,tags:imageTags}'
aws ecr describe-images --repository-name pgvector-investigation/benchmark-engine \
  --image-ids imageTag="$TAG" --query 'imageDetails[].{digest:imageDigest,tags:imageTags}'
```

Record the immutable digests in run evidence. For a frozen run, resolve tag references to digests before deploying. Capture the ECR image details after both pushes and inspect scan findings.

## 3. Private S3 bucket, EBS driver and scoped service accounts

```bash
uv run python scripts/aws/deploy.py storage
uv run python scripts/aws/deploy.py addons
kubectl get storageclass
kubectl -n kube-system get pods -l app=ebs-csi-controller
kubectl -n benchmarks get serviceaccount benchmark-data-reader dataset-uploader
```

The dataset bucket blocks public access, enables SSE-S3 encryption and versioning, and aborts abandoned multipart uploads after one day. IRSA binds the dataset reader to `s3:GetObject` under `datasets/`. The temporary uploader additionally has only the bucket permissions needed to write and verify those objects. The EBS CSI controller uses a separate AWS-managed policy and service account. [EBS CSI requirements](https://docs.aws.amazon.com/eks/latest/userguide/ebs-csi.html).

Storage classes use encrypted gp3 volumes, `WaitForFirstConsumer` and `Retain`. The latter protects data during accidental PVC deletion but means deleting a cluster does not remove all billable volumes.

## 4. Deploy the operator, both databases and monitoring

```bash
uv run python scripts/aws/deploy.py workloads
uv run python scripts/aws/deploy.py secrets
kubectl get cluster -A
kubectl get pods,pvc -n cnpg-pgvector
kubectl get pods,pvc -n benchmark-results
kubectl get pods,pvc -n monitoring
kubectl get svc -A
```

The helper selects a chart whose operator `appVersion` is exactly 1.30.1, resolves the monitoring chart version, and records both versions privately. It installs one operator and creates two CNPG Cluster resources:

| Purpose | Namespace / Cluster | Database / owner | Storage |
| --- | --- | --- | --- |
| Vectors | `cnpg-pgvector` / `ml-pgvector-benchmark` | `benchmark_vectors` / `benchmark_user` | 200 GiB gp3, 6,000 IOPS / 250 MiB/s |
| Results | `benchmark-results` / `benchmark-results` | `benchmark_results` / `results_user` | 20 GiB gp3 |

CNPG creates application passwords automatically. The helper copies only the required connection keys into `benchmarks` Secrets. The results secret uses `RESULTS_PG_*`, so it cannot overwrite the vector connection's `PG_*`. Never commit generated Secrets or include password output in screenshots.

The vector Cluster uses `postInitApplicationSQL` to enable `vector` in the application database, rather than only in a maintenance database. All database services are internal ClusterIP services. Verify there are **no LoadBalancer services**.

```bash
kubectl -n cnpg-pgvector exec ml-pgvector-benchmark-1 -- \
  psql -U postgres -d benchmark_vectors -c 'SELECT version();' \
  -c "SELECT extname, extversion FROM pg_extension WHERE extname='vector';"
kubectl -n benchmark-results exec benchmark-results-1 -- \
  psql -U postgres -d benchmark_results -c 'SELECT current_database();'
kubectl get pods -A -o wide
```

Capture runtime version output, actual pod resource allocation, storage parameters and placement. Both CNPG clusters have a **single instance**. This setup cannot demonstrate replica failover or cross-AZ HA. See [monitoring guide](CNPG_MONITORING_SETUP.md) before running workloads.

## 5. Challenges and experimental controls

- **Resource contention:** namespace boundaries do not isolate CPU, memory or disks. Dedicated, tainted node groups keep the timed workload off the monitoring/results node; DaemonSets and shared control-plane effects remain.
- **EBS topology:** volumes are AZ-local and normally ReadWriteOnce. The dataset cache is a single-node cache, not a multi-node shared file system. Run Jobs sequentially and keep their node placement consistent. WaitForFirstConsumer binds volumes to the actual worker AZ.
- **EBS performance:** record volume size, IOPS, throughput, EC2 EBS bandwidth and queue depth. Otherwise, differences in storage can dominate query comparisons.
- **Cache state:** loading the dataset and building an index warms caches. Explicitly distinguish first-read, warm-cache and repeated-run results.
- **Changing software:** exact thresholds and query plans can change. Record PostgreSQL, pgvector, compiler/image digest and session GUCs; do not assume a LIMIT threshold measured elsewhere.
- **Table statistics:** plan choice for filtered vector search depends on whether `ANALYZE` ran after the expression indexes were created. Record the statistics state (`pg_stats` for each expression index, and `last_analyze` / `last_autoanalyze` from `pg_stat_user_tables`) with every measurement. See [execution-guide.md](execution-guide.md#6a-reproduce-both-database-states-with-and-without-filter-statistics).
- **Data transfer:** signed HTTPS sources expire. The worker streams in 32 MiB parts with four concurrent reads, verifies lengths, computes SHA-256, independently reads S3 back, and publishes a checksum manifest only after verification. Renew source credentials if the transfer cannot finish within their validity period.
- **Results durability:** the results database has retained EBS but one instance. Take a logical export to S3 before teardown. A PVC is not a backup.
- **Benchmark runtime:** the full 3 × 8 × 6 matrix is 144 scenarios before repetitions. Use a smoke run first. Query timeouts can make failed scenarios finish faster; record failures and returned rows beside QPS.

## Setup issues and fixes

**An empty context in a new project kubeconfig can look like a missing Kubernetes server.** The initial cluster-create command deliberately avoids changing a context, but a brand-new file then has no selected context. The observed error was `failed to download openapi: Get "http://localhost:8080/openapi/v2?timeout=32s": ... connection refused`. The fix is the explicit `aws eks update-kubeconfig ... --kubeconfig "$KUBECONFIG" --alias "$EKS_CLUSTER"` command in step 1. Do not disable OpenAPI validation to hide that connection problem.

**eksctl's IAM service-account command consumes KUBECONFIG from the environment.** The installed command rejected `--kubeconfig` with `unknown flag: --kubeconfig`. The deployment helper sets the environment variable to the project file, while kubectl and Helm receive their supported explicit flags.

**Dashboard queries need to match the cluster topology and exported units.** Node panels must select the vector node rather than average all three workers. PostgreSQL exports shared_buffers in 8 KiB blocks but work_mem in KiB; the dashboard now converts each to bytes before Grafana formats it. Verified runtime values are 4 GiB and 256 MiB respectively. Scan counters also use the actual `wot_chunks_2_5m` table name. See the monitoring guide for details.

## 6. Costs and teardown

Get the current Mumbai on-demand EC2, EKS, EBS gp3/IOPS/throughput, S3 and public IPv4 rates before starting; do not use an outdated hard-coded estimate. The configured compute cost is one m6i.4xlarge + one m6i.2xlarge + one m6i.xlarge + one EKS control plane per running hour. Add about 590 GiB configured EBS across roots/data/cache/monitoring, provisioned gp3 extras, S3/ECR usage and three public worker IPv4 addresses. Registry pulls, source egress and retries may add transfer charges. [AWS Pricing Calculator](https://calculator.aws/).

Before deletion: export results, raw plans and logs; verify backups; note the dataset bucket, retained volumes and repository names. The following commands are **manual teardown**, not an automatic post-run action:

```bash
# Deletes the named benchmark cluster and its workers; preserve evidence first.
eksctl delete cluster --name pgvector-investigation --region ap-south-1 --wait
```

Then review retained EBS volumes/snapshots, the dataset bucket's current and noncurrent versions, ECR images and dedicated IRSA policies/roles. Delete only resources belonging to this investigation that you no longer need. S3 versioning means deleting current objects does not erase stored versions. Keep the ECR registry's unrelated repositories intact.
