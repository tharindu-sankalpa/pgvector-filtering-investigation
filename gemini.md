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

### Layer 2 - `doc/` Directory (Implementation Layer)

- Full EKS cluster setup
- CloudNativePG installation steps
- Kubernetes manifests
- Helm configurations
- Benchmark scripts
- Full reproducible environment setup
- Deep technical commands and scripts

The README should reference `doc/` when needed using phrasing such as:

> For full EKS cluster setup and CloudNativePG installation, see `doc/CNPG_PGVECTOR_SETUP.md`.

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
- Move large scripts to `doc/`
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
2. Following `doc/` for environment recreation
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

### Infrastructure — one EKS cluster

Use `ap-south-1`, cluster `pgvector-investigation`, and an explicit `.local/kubeconfig`. Do not change unrelated contexts. Namespace mapping: `cnpg-system` operator; `cnpg-pgvector` vector database; `benchmark-results` results database; `benchmarks` execution; `monitoring` metrics. Read `doc/CNPG_PGVECTOR_SETUP.md` and `doc/execution-guide.md` before operating workloads. All database access is through internal services or local port-forwards. Images are tagged and stored in ECR. Rebuild the benchmark image after code changes and record its digest. Never use the mutable `latest` tag for a measured run.

### Article preservation

README.md is canonical and is the article: a big-picture summary first, then concepts, then the evidence. Preserve all concepts, theory, query-plan explanations, and the workaround evidence (summary in the README, full detail in `doc/workarounds.md`). Every number comes from the recorded EKS runs in `doc/RUN_EVIDENCE.md` and `doc/results/`; do not invent or relabel measurements. The central finding: the filtered-search cliff comes from missing statistics for the `metadata->>'book_name'` expression index (no `ANALYZE` after `CREATE INDEX`), and `ANALYZE` removes it. Regenerate `doc/article.md` with `python scripts/sync_article.py` after editing the README. Public documentation describes EKS only. Dataset transfer credentials and source URLs belong in ignored private state.

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

Use the commands in `doc/execution-guide.md`. Local DBeaver endpoints are `127.0.0.1:15432` for vectors and `127.0.0.1:15433` for results. CNPG generates application credential Secrets. Copy only the required keys into `benchmarks`, and never print or commit passwords. Local optional overrides use `.env.aws` (shape in `example.env.aws`).

## Enforcement Expectation

If a request violates any rule above:

1. **Refactor the solution to comply** immediately.
2. **Do not ask for permission.**
3. These rules are mandatory by default.
