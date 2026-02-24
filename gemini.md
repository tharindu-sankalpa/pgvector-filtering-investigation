## Repository Context & Author Intent

This repository supports a technical deep-dive investigation into **PostgreSQL + PGVector filter search performance degradation**.

The primary objectives of this work are:

1. **Showcase real-world technical expertise**
2. Contribute meaningful insights to the open-source community
3. Demonstrate investigative depth and engineering rigor
4. Attract recruiter and community attention through high-quality technical writing
5. Provide reproducible artifacts for practitioners

This repository is tightly coupled with a Medium technical article.

---

## Core Content Strategy

The `README.md` file serves two purposes:

1. It is the **canonical version of the Medium article**
2. It acts as a structured, high-level technical narrative

The README **must remain clean, engaging, and story-driven**.

It should:

- Focus on explanation, insight, and reasoning
- Highlight investigation methodology
- Explain findings clearly
- Avoid overwhelming readers with setup scripts and long command blocks
- Be structured like a polished technical article

---

## Documentation Philosophy

This repository follows a **Layered Documentation Model**:

### Layer 1 - README (Article Layer)

- Conceptual explanations
- Architecture diagrams
- Performance findings
- Root cause analysis
- Benchmark summaries
- Key code snippets (minimal, illustrative only)
- Links to detailed implementations

### Layer 2 - `/docs` Directory (Implementation Layer)

- Full AKS cluster setup
- CloudNativePG installation steps
- Kubernetes manifests
- Helm configurations
- Benchmark scripts
- Full reproducible environment setup
- Deep technical commands and scripts

The README should reference `/docs` when needed using phrasing such as:

> For full AKS cluster setup and CloudNativePG installation, see `/docs/aks-setup.md`.

---

## Writing Guidelines for AI Assistance

When helping with README or article content:

- Keep tone professional but engaging
- Make explanations crisp and high-signal
- Avoid unnecessary verbosity
- Prioritize clarity over jargon
- Preserve technical depth
- Avoid marketing language
- Avoid generic statements
- Avoid fluff

When including code:

- Keep snippets short and illustrative
- Move large scripts to `/docs`
- Link instead of embedding full configurations

---

## Technical Positioning

This work should emphasize:

- Performance investigation methodology
- Query planner behavior
- Index selection impact
- Filter + vector search interaction
- Benchmark comparisons
- Real production-grade thinking

It should read like:

- A senior engineer's deep-dive analysis
- Not a tutorial for beginners
- Not surface-level documentation

---

## Reproducibility Requirement

Anyone should be able to replicate the investigation by:

1. Reading the README for conceptual understanding
2. Following `/docs` for environment recreation
3. Running provided benchmark scripts

Reproducibility is critical.

---

## Target Audience

- Backend engineers
- Database engineers
- Platform engineers
- AI infrastructure engineers
- Recruiters evaluating deep technical capability
- Open-source contributors

---

## AI Output Expectations

When generating content:

- Maintain consistency with investigation theme
- Ensure technical correctness
- Favor structured sections with headers
- Suggest improvements when clarity is weak
- Do not oversimplify technical concepts
- Keep README Medium-ready

---

## Global Code Generation Rules (Mandatory)

From this point onward, **all generated code, scripts, and Python programs must strictly follow the rules below**. These rules apply repository-wide and override any default assumptions.

I am **no longer using Jupyter Notebooks**. All development is done in **production-grade Python scripts (`.py`)**.

The objective is to generate **debug-ready, production-quality code** that is **highly readable and transparent**. The code must provide full execution visibility via logs and be immediately understandable to a human reader through simplicity and extensive commentary.

---

## Kubernetes & Benchmark Operations (CRITICAL - Read First)

### Infrastructure — Three AKS Clusters + ACR

This project uses separate Kubernetes clusters. **You MUST switch to the correct context before any kubectl/helm operations:**

| Cluster | Purpose | Context Name |
|---------|---------|--------------|
| **Benchmark Execution** | Running benchmark jobs, viewing job logs | `benchmark-execution-aks` |
| **CNPG PostgreSQL** | Self-hosted CloudNativePG PostgreSQL + pgVector cluster, monitoring | `aks-cnpg-pgvector` |
| **Azure Managed PostgreSQL** | Azure PostgreSQL Flexible Server storing benchmark results | _(accessed via `.env.azure` credentials, not kubectl)_ |

```bash
# For benchmark job operations (helm install benchmark, view logs, check job status)
kubectl config use-context benchmark-execution-aks

# For CNPG PostgreSQL operations (pod status, psql access, monitoring, cluster config)
kubectl config use-context aks-cnpg-pgvector
```

**Azure Container Registry (ACR):**

| Resource | Value |
|----------|-------|
| Registry | `benchmarkregistry504646de.azurecr.io` |
| Benchmark image | `benchmark-engine` |
| Custom PG image | `postgresql-pgvector:18.1-pgvector` |
| Pull secret | `acr-secret` (Kubernetes secret) |

**CRITICAL:** Never assume the current context is correct. Always explicitly switch before operations.

### Docker Image Tagging Policy — NO `latest` TAG

There is **NO image tagged as `latest`**. Every code change requires:

1. **Increment the tag** in `doc/execution-guide.md` (e.g., `v5.2.6-wot-8rep` → `v5.2.7-wot-8rep`)
2. **Build with the new tag:**
   ```bash
   export ACR_NAME="benchmarkregistry504646de"
   export IMAGE_NAME="benchmark-engine"
   export TAG="v5.2.7-wot-8rep"  # INCREMENT THIS
   docker build -t $ACR_NAME.azurecr.io/$IMAGE_NAME:$TAG -f infrastructure/docker/Dockerfile .
   ```
3. **Push to ACR:**
   ```bash
   docker push $ACR_NAME.azurecr.io/$IMAGE_NAME:$TAG
   ```
4. **Update `doc/execution-guide.md`** with the new tag value

### Code Changes Require Docker Rebuild

Any changes to these files require a Docker image rebuild:
- `src/pgvector/*.py` — PostgreSQL benchmark scripts
- `src/shared/*.py` — Shared utilities
- `pyproject.toml` — Dependencies

**Workflow for code changes:**
1. Make code changes
2. Test locally if possible: `uv run python scripts/your_script.py`
3. Update TAG in `doc/execution-guide.md`
4. Build Docker image with new tag
5. Push to ACR
6. Deploy benchmark with new tag

### Benchmark Execution Workflow

Always follow `doc/execution-guide.md` for running benchmarks. Quick reference:

```bash
# 1. Switch to benchmark cluster
kubectl config use-context benchmark-execution-aks

# 2. Set required environment variables
export ACR_NAME="benchmarkregistry504646de"
export IMAGE_NAME="benchmark-engine"
export TAG="v5.2.6-wot-8rep"  # GET CURRENT TAG FROM doc/execution-guide.md

# 3. Uninstall previous and install new benchmark
helm uninstall <benchmark-name> --ignore-not-found
helm install <benchmark-name> ./kube/charts/benchmark-engine \
  -f kube/charts/benchmark-engine/examples/<config>.yaml \
  --set image.repository=$ACR_NAME.azurecr.io/$IMAGE_NAME \
  --set image.tag=$TAG
```

### Key Documentation References

- `doc/execution-guide.md` — Complete benchmark execution guide, current image tag
- `doc/CNPG_PGVECTOR_SETUP.md` — CNPG PostgreSQL cluster setup guide
- `doc/CNPG_MONITORING_SETUP.md` — CNPG Prometheus/Grafana monitoring stack and metrics reference
- `infrastructure/cnpg-pgvector/cluster.yaml` — CNPG PostgreSQL cluster definition (resources, PG config, monitoring)

---

## Mandatory Logging Standard — `structlog` (Non-Negotiable)

### No `print()` Statements

- Absolutely **no `print()`** usage.
- **All output must be logged** using `structlog`.

### Configuration Strategy

Configure `structlog` based on the context of the request:

**Scenario A: Standalone Script**
If generating a single, self-contained script, **include this exact block at the top**:

```python
import structlog
import logging
import sys

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(colors=True),
    ],
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)
logger = structlog.get_logger()
```

**Scenario B: Project / Multiple Files**
If generating part of a larger project, make sure configuration is done in a `utils/logger.py` file.

- If the `utils/logger.py` file does not exist, create it and include the configuration block.
- If the `utils/logger.py` file exists, import the logger from it.

### Event-Based Logging Only

Logs must be **structured, event-based, and machine-readable**.

- **Bad:** `logger.info(f"Processing item {i}")`
- **Good:** `logger.info("processing_item", index=i, status="active")`

### Data Profiling Instead of Printing

When loading, transforming, or inspecting data (e.g., DataFrames, tensors, arrays, lists, JSON, and dictionaries):

- **Do NOT print the data.**
- **Log a profile instead**, including:
  - Shape / dimensions
  - Column names / Types
  - Memory usage
  - Distinct / null counts

This allows full state verification **via logs only**.

---

## Architecture, Simplicity & Anti-Over-Engineering

### Radical Readability & Simplicity

- **Avoid Over-Engineering:** Do not use complex design patterns (e.g., Decorators, Factories, Metaclasses) unless strictly necessary for functionality.
- **Explicit > Implicit:** Logic must be immediately visible. Do not hide behavior behind obscure abstractions.
- **Linear Flow:** Prefer linear, procedural logic within functions over deeply nested structures or recursive complexity.
- **Goal:** A developer should be able to read the code once and understand exactly what it does without jumping between multiple files or classes unnecessarily.

### Modular, Debug-Friendly Design

- Break logic into **small, single-purpose functions**.
- **Type Hinting is Mandatory:** All functions must have Python type hints (e.g., `def process_data(df: pd.DataFrame) -> dict:`).
- Code must be friendly to **Step-Through Debugging** (avoid one-liners that do too much).

### Contextual Logging

At the start of every major function or workflow, bind context:

```python
log = logger.bind(task="data_cleaning", file_id=file_path)
```

All subsequent logs in that function **must use the bound `log` variable**.

### Exception Handling (Never Silent)

- **Never swallow exceptions.**
- Always log failures using: `log.exception("event_failed")`.
- This automatically captures the stack trace and bound context.

---

## Code Documentation, Comments & Output Standards

### High-Resolution Docstrings

- Every module, function, and class must have a **Google-Style Docstring**.
- **Tone:** Professional, yet highly explanatory.
- **Content:** Do not just describe _what_ the function does, but _how_ it fits into the broader workflow.
- **Args/Returns/Raises:** Must be exhaustively detailed.

### Dense, Line-by-Line Commentary

- **Code must be self-narrating.**
- **Algorithmic/Complex Logic:** Provide **line-by-line comments** explaining the functionality.
- **Business Logic:** Comments should explain the "Why" behind the code, ensuring the intent is clear to future maintainers.
- **Visual Scannability:** Use whitespace and comment blocks to visually separate logical steps within a function.

### Output Requirements

- Always return a **complete, runnable `.py` script**.
- **Dependencies:** Ensure all dependencies are listed in `pyproject.toml`. You may list `uv add` commands in comments for visibility, but **`pyproject.toml` is the source of truth**.
- The execution flow **must be traceable entirely through terminal logs**.

---

## Environment & Dependency Management

### Python Version & Virtual Environment

- **Version Management:** This project uses **pyenv** to manage Python versions.
- **Dependency Management:** This project uses **uv** for virtual environment and dependency management.

### Automated Dependency Housekeeping

- **Check `pyproject.toml`:** Every time you generate code, **verify that all imported libraries are present** in `pyproject.toml`.
- **Add Missing Dependencies:** If a library is missing, **you must add it** to `pyproject.toml` (or run `uv add <package>`).
- **Remove Unused Dependencies:** If you remove functionality or refactor code such that a dependency is no longer needed, **remove it from `pyproject.toml`** to keep the project clean.
- **Proactive Maintenance:** Perform these checks **automatically** during code generation. Do not wait for the user to ask.

---

## PostgreSQL Database Access

The benchmark system uses **two separate PostgreSQL deployments**:

| Deployment | Purpose | Location |
|------------|---------|----------|
| **Azure Managed PostgreSQL** | Results DB (benchmark metrics), Legacy DB | Azure Flexible Server |
| **CNPG PostgreSQL** | Vector DB (pgvector embeddings) | Self-hosted on `aks-cnpg-pgvector` |

Connection credentials are stored in `.env.azure`. Load once before any `psql` or benchmark runs:

```bash
export $(grep -v '^#' .env.azure | xargs)
```

---

### Azure Managed PostgreSQL (Results DB)

Benchmark metrics are stored in an Azure PostgreSQL Flexible Server. Use for querying results, summaries, and legacy data.

**CLI connection (psql):**

```bash
# Connect to Results Database (benchmark metrics only)
PGSSLMODE=require PGHOST=$PG_HOST PGUSER=$PG_USER PGPASSWORD=$PG_PASSWORD \
  PGDATABASE=benchmark_results psql

# Connect to Legacy/Vector Database (old combined database on Azure)
PGSSLMODE=require PGHOST=$PG_HOST PGUSER=$PG_USER PGPASSWORD=$PG_PASSWORD \
  PGDATABASE=vector_benchmark psql
```

**Key tables (Results DB):**

| Table | Description |
|-------|-------------|
| `benchmark_retrieval_summary` | Aggregated metrics per test scenario (avg, p50, p95, p99, qps) |
| `benchmark_insert_summary` | Insert/ingestion benchmark summaries |
| `benchmark_index_summary` | Index creation timing metrics |

**Common queries:**

```sql
-- List all databases tested
SELECT DISTINCT database_name FROM benchmark_retrieval_summary;

-- View latest results
SELECT database_name, test_type, top_k, concurrency_level,
       ROUND(avg_latency_seconds * 1000, 2) as avg_latency_ms,
       ROUND(p99_latency_seconds * 1000, 2) as p99_latency_ms,
       ROUND(qps, 2) as qps
FROM benchmark_retrieval_summary
ORDER BY id DESC
LIMIT 20;

-- Delete results for a specific database
DELETE FROM benchmark_retrieval_summary
WHERE database_name = '<database_name_to_delete>';
```

---

### CNPG PostgreSQL (Vector DB — Self-Hosted)

The CloudNativePG cluster on `aks-cnpg-pgvector` hosts the pgvector data. Use for running benchmarks, diagnosing queries, or inspecting embeddings.

**Get credentials from Kubernetes (if not in .env.azure):**

```bash
kubectl config use-context aks-cnpg-pgvector

# App user (benchmark_user) — used for benchmarks
kubectl get secret ml-pgvector-benchmark-app -n cnpg-pgvector \
  -o jsonpath='{.data.user}' | base64 -d
kubectl get secret ml-pgvector-benchmark-app -n cnpg-pgvector \
  -o jsonpath='{.data.password}' | base64 -d

# LoadBalancer external IP (public host for psql from your machine)
kubectl get svc ml-pgvector-benchmark-lb -n cnpg-pgvector -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

**CLI connection (psql) — using .env.azure CNPG vars:**

```bash
# Connect to CNPG vector database (benchmark_vectors)
PGSSLMODE=require PGHOST=$CNPG_PG_HOST PGPORT=$CNPG_PG_PORT \
  PGUSER=$CNPG_PG_USER PGPASSWORD=$CNPG_PG_PASSWORD \
  PGDATABASE=$CNPG_PG_DATABASE psql

# Or explicitly (if CNPG_* not loaded):
PGSSLMODE=require psql -h 51.104.162.145 -p 5432 -U benchmark_user -d benchmark_vectors
```

**Run pgvector benchmarks locally against CNPG:**

```bash
export PG_HOST=$CNPG_PG_HOST PG_USER=$CNPG_PG_USER \
  PGPASSWORD=$CNPG_PG_PASSWORD VECTOR_PG_DATABASE=$CNPG_PG_DATABASE

uv run python src/pgvector/03_retrieval_asyncpg.py  # example
```

---

### Environment Variables (in .env.azure)

**Azure Managed PostgreSQL (Results / Legacy):**

```
PGHOST=pgvector-benchmark-server.postgres.database.azure.com
PGUSER=mlsvc_pgvector_admin
PGPASSWORD=<password>
PGDATABASE=vector_benchmark
PGSSLMODE=require
```

**CNPG PostgreSQL (Vector DB):**

```
CNPG_PG_HOST=51.104.162.145        # LoadBalancer IP from ml-pgvector-benchmark-lb
CNPG_PG_PORT=5432
CNPG_PG_USER=benchmark_user
CNPG_PG_PASSWORD=<from ml-pgvector-benchmark-app secret>
CNPG_PG_DATABASE=benchmark_vectors
```

**K8s overrides (values.yaml):** `RESULTS_PG_DATABASE=benchmark_results`, `VECTOR_PG_DATABASE=benchmark_vectors`

---

## Enforcement Expectation

If a request violates any rule above:

1. **Refactor the solution to comply** immediately.
2. **Do not ask for permission.**
3. These rules are mandatory by default.
