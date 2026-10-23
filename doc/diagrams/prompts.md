# Diagram prompts (Nano Banana Pro)

Prompts for every image-model figure in the article and README, and for the LinkedIn carousel. These use the `diagram` skill's visual-only approach: only quoted text is printed, everything else is private direction.

**Project preference: no standalone titles inside diagram images.** Start directly with the diagram content; do not reserve an empty title band. Component names, boundary labels and section labels remain. The surrounding article or README supplies the figure heading. This explicitly overrides the skill's default requirement for a large title.

Output formats:

| Set | Files | Aspect ratio | Why |
|---|---|---|---|
| Article and README prompts in this file | Sixteen image-model `doc/diagrams/*.png` figures below (the distribution chart is scripted) | **16:9 landscape**, 2752 × 1536 (the generator's 2K landscape output; the accepted figures use it) | Gives technical labels and connection routes enough width, with a consistent appearance across the set. The prompts say 3:2 as a fallback; the generator's 2K setting wins. Convert JPG output to PNG under the target filename. |
| LinkedIn multi-image post | Two `doc/linkedin/diagrams/li-*.png` diagrams below, placed on pages 2 and 7 of the ten pages that Claude Design builds from [linkedin-carousel.md](../linkedin-carousel.md) | **3:4 portrait**, 1792 × 2400 (the accepted files) | Larger type and fewer details for phone viewing; matches the 1080 × 1440 pages. The two prompts still say 4:5 / 1600 × 2000; set 3:4 in the generator. |

Set the aspect ratio in the generator's settings, not only in the prompt, and check the exported dimensions.

## Shared visual direction

Use the accepted architecture image as the style reference: compact cards, small colored icon tabs, pale fills, thin boundaries, clear left-aligned labels and deliberate connection routes. Every prompt below is self-contained and can be pasted into the generator on its own. The architecture prompt is retained as approved.

- No standalone title, subtitle or empty title band. Local section and component labels provide context.
- Use one consistent outline-icon family. A small colored icon tab is appropriate; a large decorative icon is not.
- Keep type readable by shortening captions and using explicit line breaks. Do not shrink text to fit a crowded card.
- Use color for meaning, and distinguish arrows by style as well as color. Reserve red for the observed expensive path or a missing result, not for every alternative plan.
- Keep connector labels off card borders. Count every directed arrow, including arrows inside panels; structural tree edges have no arrowheads.
- Use quiet factual notes instead of large yellow banners. Include only the compact legend entries relevant to that figure.
- Treat page grids, candidate dots and tree shapes as schematics unless the prompt explicitly specifies a measured count. Measured values must retain their scenario and setting labels.

## Shared palette

One palette for both sets, so a component looks the same in the article and the carousel.

| Role | Accent | Fill |
|---|---|---|
| Benchmark workloads, vector-search steps | Indigo #4F46E5 | #EEF2FF |
| PostgreSQL databases | Green #15803D | #F0FDF4 |
| Observability | Teal #0F766E | #F0FDFA |
| AWS storage and registry (S3, ECR, EBS) | Blue #2563EB | #EFF6FF |
| Kubernetes boundaries, operator, controllers | Slate-blue #475569 | near-white #F8FAFC |
| Observed expensive path, missing results | Red #B91C1C | #FFF1F2 |
| Caution, decision | Amber #B45309 | #FFFBEB |
| Files, neutral steps, factual notes | Slate #475569 | #F8FAFC |

| Line meaning | Style |
|---|---|
| SQL, result writes, primary process sequence | Medium solid indigo #4F46E5 |
| Reads, image pulls, metric scrapes | Thinner dashed steel-blue #0369A1 |
| Build, deploy, neutral comparison links | Thin solid slate #64748B |
| CNPG management | Thin dotted slate #64748B |

## Inventory

| Diagram | Action |
|---|---|
| `architecture.png` | **Generated 22 Oct, accepted, embedded.** Known minor issue: the "write results" arrow starts at the vector database instead of Benchmark Jobs. |
| `benchmark-execution-flow.png` | **Generated 22 Oct, accepted, embedded.** |
| `dataset-creation.png` | **Generated 22 Oct, accepted, embedded.** Known minor issue: line widths are printed beside two arrows ("4 px", "3 px"). |
| `Two-Compounding-Problems.png` | **Generated 22 Oct, accepted, embedded.** |
| `planner-row-estimates.png` | **Generated 22 Oct, accepted, embedded.** Concept diagram for the "Table statistics" section. |
| `b-tree-index-and-bitmap-heap.png` | **Generated 22 Oct, accepted, embedded.** |
| `book-filter-distribution.png` | **Generated 23 Oct from the numbered fallback prompt, accepted, installed.** Known issues: the unrecorded bars are schematic, the 209,226 bar ends near 170k on its axis, and the 168,426 label sits on Book 14 rather than Book 13. `scripts/plot_book_distribution.py` produces an exact version when the database is available. |
| `search-patterns.png` | **Generated 23 Oct, accepted, installed.** Known minor issue: `metadata->>"book_name"` printed with double quotes instead of single. |
| `ef_construction.png`, `ef_search.png` | **Generated 23 Oct, accepted, installed.** |
| `m-Parameter.png` | **Generated 23 Oct (second attempt), accepted, installed.** |
| `HNSW-Post-Filtering.png` | **Generated 23 Oct, accepted, installed.** Known minor issues: the expansion is drawn as three joined red arrows, and five matches are shown instead of four. |
| `iterative_scan.png` | **Generated 23 Oct, accepted, installed.** |
| `gin-index.png` | **Generated 23 Oct, accepted, installed.** Known minor issue: the "(76,198 IDs)" tag sits slightly below the dragon row. |
| `planner-decision.png` | **Generated 23 Oct, accepted, installed.** Known minor issue: the legend prints "HNSW pat" (truncated). |
| `limit-41-plan-steps.png` | **Generated 23 Oct, accepted, installed.** Known minor issues: "4 px" printed in three arrow labels and "2%" under the timing bar. |
| `text-search-cte-steps.png` | **Generated 23 Oct, accepted, installed.** |
| `hnsw-graph.html` | Keep. Interactive figure; `doc/plots/hnsw-graph.png` is its static export, not image-model work. |
| `doc/plots/*.png` | Not image-model work. Regenerate with `scripts/plot_article_benchmarks.py` from the EKS results. |
| `doc/screenshots/**` | Not image-model work. Capture from the live system per [SCREENSHOT_PLAN.md](../SCREENSHOT_PLAN.md). |
| `doc/linkedin/diagrams/li-01-architecture.jpg` .. `li-10-production-checklist.jpg` | **Generated 23 Oct (3:4 portrait).** Ten-image LinkedIn post walkthrough replacing the PDF carousel workflow. |
| `doc/linkedin/diagrams/li-architecture.png`, `li-planner-switch.png` | Earlier two-diagram portrait references. |

---

## architecture.png

**What it shows:** the configured benchmark deployment: one EKS cluster, three managed node groups with one worker each, two single-instance CNPG databases, S3 staging to an EBS cache, benchmark clients, and database observability. Namespace names identify workloads; they are not physical subdivisions of a node. This is a deployment overview, not a complete network or Kubernetes control-plane diagram.

**Review of the supplied image:** the major components, machine sizes, database versions and storage allocations match the repository. The main corrections are:

- Show the separate staging Job between S3 and the cache. In the supplied image the S3 arrow stops at the node header, leaving the staging path ambiguous.
- Replace nested namespace enclosures with small namespace tags. Show one worker per node group and the shared worker availability zone. State that both databases have one instance.
- Spell out resource requests and limits instead of ranges. The study Job's allocation does not describe the smaller staging Job.
- Replace the taints-only claim with selectors and tolerations for placement, plus taints on the benchmark and vector workers. The services group is not tainted in the manifest.
- Distinguish results written over SQL from Prometheus scrapes and CNPG management. Include the Grafana-to-Prometheus query connection. Treat operator arrows as logical management relationships through Kubernetes, not direct database SQL connections.
- Scope ECR to the two custom images. The results database uses GHCR and the staging Job uses public ECR; other image sources are intentionally outside this overview.

**Visual direction:** remove the headline, heavy nesting, bottom-edge shadows, oversized icons and empty card interiors. Use a wider canvas, aligned text, restrained accent colors and dedicated connector space. A single compact legend replaces the duplicate worker-node key. The specific project palette below takes precedence over the skill's generic palette; PostgreSQL with pgvector keeps the same cylinder symbol as the results database, and the Parquet cache uses a file-stack symbol.

**Evidence:** [node groups and placement](../../infrastructure/aws/eks-cluster.yaml), [staging Job](../../infrastructure/aws/dataset-stage.yaml), [dataset PVC](../../infrastructure/aws/dataset-pvc.yaml), [study Job](../../infrastructure/aws/full-study-job.yaml), [vector database](../../infrastructure/cnpg-pgvector/cluster.yaml), [results database](../../infrastructure/aws/results-cluster.yaml), [storage classes](../../infrastructure/aws/storage.yaml), [monitoring settings](../../infrastructure/cnpg-pgvector/monitoring/values-prometheus-grafana.yaml), [PodMonitor](../../infrastructure/cnpg-pgvector/monitoring/cnpg-podmonitor.yaml), [deployment helper](../../scripts/aws/deploy.py), [study orchestrator](../../scripts/aws/full_study.py), [retrieval client](../../src/pgvector/03_retrieval_asyncpg.py), [results writer](../../src/shared/results_db.py), and [recorded run environment](../RUN_EVIDENCE.md). This review uses repository configuration and recorded evidence, not a fresh live-cluster inspection.

**Prompt:**

```text
Create a polished technical deployment diagram on a pure white canvas. Output a 3:2 landscape raster image at 3072 × 2048 or higher. Begin directly with the diagram content. No standalone title, headline, subtitle or blank title band. All instructions are private art direction; print only the quoted labels specified below, once each unless an explicit repetition is requested.

VISUAL STYLE — PRIVATE:
Use the visual restraint of a carefully typeset engineering whitepaper. Flat orthographic shapes, crisp edges, white space and precise alignment. No gradients, shadows, thick bottom edges, extrusion, perspective, glossy surfaces or ornamental backgrounds. Modest rounded corners, identical across component cards. Avoid oversized dashboard tiles.

Use an Inter-like sans-serif. Text is left-aligned within cards: component names semibold dark slate #0F172A; metadata regular slate #475569. At the target resolution, component names are about 40 px, node headers 34 px, metadata and arrow labels 30 px, and footer text at least 28 px. Namespace tags use a restrained monospace at the metadata size. Wrap text onto the specified lines; preserve readable type rather than squeezing long captions onto one line.

Use near-white cards with a narrow colored left accent, not large saturated fills. Benchmark Jobs: indigo #4F46E5 with #EEF2FF tint. PostgreSQL databases: green #15803D with #F0FDF4 tint. Monitoring: teal #0F766E with #F0FDFA tint. S3 and ECR: blue #2563EB with #EFF6FF tint. Workstation, operator and file cache: slate #475569 with #F8FAFC tint. Container outlines are lighter than component outlines. Names, icons and placement must remain understandable without color.

One small outline icon at the upper-left of each card, beside its name, with a consistent 40 px size and stroke. Bucket for S3, laptop for Workstation, package for ECR, download tray for the staging Job, stacked flat files for the cache, play symbol for Benchmark Jobs, matching cylinders for both PostgreSQL databases, gear for the operator, chart for Prometheus and gauge for Grafana. Keep icon space compact; never center a large icon above an empty card. No brand logos or additional decorative symbols.

SPATIAL LAYOUT — PRIVATE:
Use about 4% outer margins. Across the upper 15% of the canvas place three compact external cards: S3 at left, Workstation at center, ECR at right. Leave usable space between them for connector labels.

Below, one thin slate-blue rounded EKS boundary occupies roughly 70% of the canvas height. Its small identifying label sits inside the upper-left edge; it is a boundary caption, not a page heading. Beneath it, three aligned vertical node-group panels share a top and bottom baseline. Allocate about 28% of the inner width to the left panel, 32% to the center and 30% to the right, with the remaining 10% split into two generous routing gutters. Panels have a barely tinted #F8FAFC fill, thin solid #CBD5E1 borders and compact headers. Panel sizes are layout choices, not quantitative capacity bars.

In the left panel, stack the staging Job, file cache and Benchmark Jobs in that order. In the center, place the vector database card level with the Benchmark Jobs; leave routing space above and below it. In the right panel, place the operator at the top, results database in the middle and monitoring at the bottom. Monitoring contains two compact full-width cards, Prometheus above Grafana, with a gap for their connector. Cards fit their text with consistent padding; do not stretch the vector database or benchmark cards to fill the panels.

Use small namespace tags directly above the associated cards. Do not draw a third set of enclosing namespace boxes. Inside the cluster, leave a clear horizontal route below the vector database card for the results connection and a separate route for the monitoring connection. Keep the outer footer for two small factual notes at left and a compact legend at right.

EXACT TEXT LABELS — PRIVATE:
External left card: "Amazon S3", then "Parquet datasets" and "SHA256SUMS".
External center card: "Workstation", then "kubectl · Helm · Docker Buildx".
External right card: "Amazon ECR", then "Custom workload images" and "postgresql · benchmark-engine".

Cluster boundary caption, two modest lines: "Amazon EKS · pgvector-investigation", then "ap-south-1 · Kubernetes 1.36". At the upper-right inside the boundary, a small caption: "Workers: ap-south-1a".

Left node header: "benchmarks", then "1 worker · m6i.2xlarge" and "8 vCPU · 32 GiB". Below the header, one namespace tag: "namespace: benchmarks".
First card: "Dataset staging Job", then "S3 download · SHA-256 check".
Second card: "Dataset cache", then "80 GiB EBS gp3 PVC".
Third card: "Benchmark Jobs", then four metadata lines: "COPY · index · ANALYZE · retrieval", "Study / retrieval pod", "Request: 4 CPU · 8 GiB", "Limit: 6 CPU · 24 GiB".

Center node header: "vector-db", then "1 worker · m6i.4xlarge" and "16 vCPU · 64 GiB".
Above its database card, namespace tag: "namespace: cnpg-pgvector".
Database card: "Vector database", then six metadata lines: "PostgreSQL 18.6 · pgvector 0.8.7", "1 instance · benchmark_vectors", "Request: 12 CPU · 48 GiB", "Limit: 14 CPU · 56 GiB", "200 GiB EBS gp3", "6,000 IOPS · 250 MiB/s".

Right node header: "services", then "1 worker · m6i.xlarge" and "4 vCPU · 16 GiB".
Above the first card: "namespace: cnpg-system". Card name: "CNPG operator", then "1.30.1".
Above the second card: "namespace: benchmark-results". Card name: "Results database", then "PostgreSQL 18.6 · 1 instance", "benchmark_results", "20 GiB EBS gp3".
Above the two monitoring cards, one shared tag: "namespace: monitoring".
Upper monitoring card: "Prometheus", then "20 GiB EBS · retention: 7d / 10GB".
Lower monitoring card: "Grafana", then "10 GiB EBS".

VISUAL CONNECTIONS — PRIVATE:
Draw exactly twelve directed connectors, with one small filled arrowhead at each destination. Main SQL connections are solid indigo #4F46E5, about 4 px. File transfers, image distribution and monitoring queries are dashed steel-blue #0369A1, about 3 px. Build and deploy connections are solid slate #64748B, about 2 px. Logical CNPG management relationships are dotted slate #64748B, about 2 px.

Use horizontal and vertical segments with neat bends. Keep every connector outside card interiors, text and namespace tags. Labels sit beside a straight segment on a small white backing with comfortable clearance. Never erase a large piece of a panel border for a label. Distinct routes must not share a trunk. If a crossing is unavoidable, use a small white interruption in the lighter line, without a junction dot. Prefer direct paths; avoid looping around the entire figure.

1. Thin solid slate arrow rightward from Workstation to Amazon ECR, label "push images".
2. Thin solid slate arrow downward from Workstation to the EKS boundary's top edge, label "deploy resources".
3. Dashed blue arrow downward from Amazon ECR to the EKS boundary's top edge, label "workload images". Its endpoint is on the outer boundary, separated from the services panel; it does not land on the operator or results database.
4. Dashed blue arrow from Amazon S3 downward to Dataset staging Job, label "download files".
5. Dashed blue arrow from Dataset staging Job downward to Dataset cache, label "verify + stage".
6. Dashed blue arrow from Dataset cache downward to Benchmark Jobs, label "read Parquet".
7. Solid indigo arrow from Benchmark Jobs rightward to Vector database, label "SQL :5432". Give this primary horizontal connection the clearest path.
8. Solid indigo arrow from Benchmark Jobs to Results database, through the route below the vector database card, label "write results". It must pass outside the vector database card and must not appear to originate there.
9. Dotted slate arrow from CNPG operator leftward to the upper edge of Vector database, label "CNPG management".
10. Dotted slate arrow from CNPG operator downward to Results database, repeat label "CNPG management".
11. Dashed blue arrow from Prometheus leftward to the lower edge of Vector database, in a separate route from the results line, label "scrape :9187".
12. Dashed blue arrow from Grafana upward to Prometheus, label "query metrics".

CALLOUTS — PRIVATE:
Use two quiet left-aligned notes below the cluster, without colored banners or warning icons: "Single-instance databases · benchmark deployment", then "Internal SQL services · namespace tags identify workloads". Above the bottom edge inside the cluster, one small caption: "Selectors + tolerations place workloads; taints reserve benchmark and vector workers".

LEGEND — PRIVATE:
At the bottom-right, outside the cluster, draw one compact white legend with a fine light-grey outline, aligned in two columns. No legend heading. Left column, four small accent swatches and labels: indigo "Benchmark workloads", green "PostgreSQL", teal "Monitoring", blue "AWS services". Right column, four line samples and labels: solid indigo "SQL / results", dashed blue "Files / images / monitoring", thin solid slate "Build / deploy", dotted slate "CNPG management". Each entry appears exactly once. Do not add worker-node or namespace swatches.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Three external cards, one EKS boundary, three one-worker node-group panels, eight internal component cards, five namespace tags and twelve arrows. Namespace tags are labels, not containment boundaries. Resource requests and limits are distinct lines. The staging Job is separate from Benchmark Jobs. Both databases have one instance. No arrow between the databases. The ECR arrow ends on the cluster boundary. All arrow labels remain readable and touch no borders or card text. No duplicated legend entries. No standalone title and no unused title space. No code, rendering syntax, invented labels, extra services or extra arrows. Overall appearance: precise, calm, publication-quality technical architecture. Previous output drew "write results" starting from the Vector database: it must start on the Benchmark Jobs card.
```

---

## benchmark-execution-flow.png

**What it shows:** the staged-data prerequisite, the main full-study phases, and the retrieval scenario matrix. The full-study orchestrator also checks the row count and runs an initial `ANALYZE` after COPY; the critical expression-statistics refresh is after index creation. The diagram separates results stored in PostgreSQL from completeness and failure diagnostics retained in logs. The matrix schedules 629,712 queries; that number is not a claim that every query succeeds.

**Evidence:** [study orchestration](../../scripts/aws/full_study.py), [COPY loader](../../src/pgvector/01_insert_benchmark_copy.py), [index creation](../../src/pgvector/02_create_indexes.py), [retrieval and timeouts](../../src/pgvector/03_retrieval_asyncpg.py), [results persistence](../../src/shared/results_db.py), and [run evidence](../RUN_EVIDENCE.md). The current index entrypoint includes `ANALYZE`; the recorded runs used an earlier image with that step in the orchestrator.

**Prompt:**

```text
Create a polished technical workflow diagram on a pure white canvas, 3:2 landscape at 3072 × 2048 or higher. Begin with the diagram itself. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below. Each prompt label appears once unless repetition is explicitly requested.

VISUAL STYLE — PRIVATE:
Match a precise engineering whitepaper: flat cards, modest consistent corners, fine outlines, pale fills and small colored icon tabs. No shadows, gradients, thick bottom edges, perspective or decorative texture. Use Inter-like sans-serif, semibold #0F172A names and regular #475569 captions. Names about 40 px, section labels 36 px, captions and connector labels at least 30 px. Settings use readable monospace. Text is left-aligned with consistent padding. Cards fit their content; no oversized empty tiles.
Use indigo #4F46E5 on #EEF2FF for benchmark steps, blue #2563EB on #EFF6FF for staging, amber #B45309 on #FFFBEB for the post-index analysis step, green #15803D on #F0FDF4 for the results database, and slate #475569 on #F8FAFC for logs and grouping. Icons share a 40 px outline style: download tray, stacked rows, index tree, magnifier, thermometer, play symbol, cylinder and document, respectively.

SPATIAL LAYOUT — PRIVATE:
Leave 4% margins and a quiet footer. The left 36% is a preparation column; the right 58% is a scenario panel; the intervening 6% is clear connector space. Use only one thin #CBD5E1 grouping outline per column. Local section labels sit inside the upper-left corners.
Left: six compact numbered cards stacked vertically with equal gaps. Right: three horizontal search-type rows at the top, then a concurrency-chip row, then a top_k-chip row. Below these, one execution card aligned with the sixth card on the left. At the bottom of the right panel, two output cards side by side, with room above them for connectors. This panel has no nested enclosing boxes.

EXACT TEXT LABELS — PRIVATE:
Left section label: "Preparation and execution".
Card 1, blue: "1 · Stage files", with "Separate Job · before timing" and "S3 to EBS · SHA-256 verified".
Card 2, indigo: "2 · Load rows", with "Binary COPY · 5,000-row batches", "100,000-row Parquet chunks", "Row check + initial ANALYZE".
Card 3, indigo: "3 · Build indexes", with "HNSW · m=16 · ef_construction=64", "B-tree · book_name", "GIN · full-text search".
Card 4, amber: "4 · Refresh statistics", with "ANALYZE after index creation" and "Expression statistics available".
Card 5, indigo: "5 · Warm up", with "48 vector queries" and "top_k: 1 · 10 · 50 · 100".
Card 6, indigo: "6 · Run retrieval", with "asyncpg pool + semaphore".
Right section label: "Scenario matrix · nested order".
First row label: "1 · Search type". Beneath it, three separate full-width compact rows:
"Vector", with "ef_search = max(64, top_k)";
"Filtered", with "ef_search = max(64, top_k)" and "iterative_scan = strict_order";
"Hybrid", with "ef_search = max(64, 2 × top_k)" and "Vector + text ranks · RRF constant 60".
The first settings label intentionally appears in both Vector and Filtered rows.
Next row label: "2 · Concurrency". Eight identical chips: "1", "2", "4", "8", "16", "32", "50", "100".
Next row label: "3 · top_k". Six identical chips: "1", "5", "10", "20", "50", "100". Shared numeric chip labels intentionally repeat across these two rows.
Execution card: "4,373 queries per scenario", with "3 × 8 × 6 = 144 scenarios" and "629,712 scheduled queries".
Left output card, green: "Results database", with "Latency percentiles · QPS", "Successful query counts".
Right output card, slate: "Job logs", with "Returned-row counts · clipping", "Failures · timeout diagnostics".

VISUAL CONNECTIONS — PRIVATE:
Exactly eight directed connectors, each with one small filled destination head and a short label. Use solid indigo 4 px lines for sequence and results; use a dashed slate 3 px line for logs. All routes are horizontal or vertical with neat bends, clear of text and chip rows. Labels have small white backings and never overlap borders.
1. Stage files down to Load rows, label "files ready".
2. Load rows down to Build indexes, label "rows loaded".
3. Build indexes down to Refresh statistics, label "indexes ready".
4. Refresh statistics down to Warm up, label "statistics ready".
5. Warm up down to Run retrieval, label "warmup complete".
6. Run retrieval rightward to the execution card, label "each scenario".
7. Execution card down to Results database, label "write summaries".
8. Execution card down to Job logs, dashed slate, label "record diagnostics".
No arrows among the chip rows or search-type rows. Their numbered row labels express nesting.

CALLOUTS — PRIVATE:
At bottom-left, two quiet caption lines: "Command timeout: 120 s", "Task budget: 160 s including queue wait". These are configuration values, not measured phase durations.

LEGEND — PRIVATE:
At bottom-right, a compact white box with a fine grey outline and four swatches in two columns: indigo "Benchmark steps", amber "Statistics refresh", green "Stored results", slate "Diagnostic logs". No legend heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Six numbered preparation cards, three search-type rows, eight concurrency chips, six top_k chips, one execution card, two output cards, eight arrows. Post-index ANALYZE precedes warmup. Scheduled queries are not labelled successful queries. Logs and results storage are separate outputs. All numbers and formulas are exact. No standalone title, oversized icons, extra nesting, invented timing results or rendering syntax.
```

---

## dataset-creation.png

**What it shows:** corpus preparation and synthetic expansion, the separately supplied query file, S3 storage, verified EBS staging, and the vector-table schema. It does not imply that the query dataset came from the synthetic expansion or that all 2.5 million vectors are independently embedded source chunks.

**Evidence:** [dataset provenance and schemas](../DATASET.md), [file verification](../RUN_EVIDENCE.md), [staging Job](../../infrastructure/aws/dataset-stage.yaml), and [COPY table definition](../../src/pgvector/01_insert_benchmark_copy.py).

**Prompt:**

```text
Create a polished technical data-provenance diagram on pure white, 3:2 landscape at 3072 × 2048 or higher. No standalone title, subtitle or empty title band. Begin with the content. Instructions are private; print only the quoted labels, once each unless explicitly repeated.

VISUAL STYLE — PRIVATE:
Flat compact cards, thin outlines, modest uniform corners, subtle tints, small colored icon tabs and precise alignment. No gradients, shadows, extrusion, thick bottom edges or decoration. Inter-like sans-serif: semibold #0F172A names around 40 px, regular #475569 captions at least 30 px, local section labels 36 px. Use readable monospace for filenames and schema fields. Left-align text and give every card equal internal padding.
Slate #475569 on #F8FAFC identifies sources and files; indigo #4F46E5 on #EEF2FF identifies embeddings and expansion; blue #2563EB on #EFF6FF identifies S3; green #15803D on #F0FDF4 identifies the PostgreSQL table. Use a consistent 40 px outline-icon family: book, text lines, small vector grid, stacked copies, query document, bucket, file stack and database cylinder.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins, three columns about 30% wide, two 5% routing gutters and a footer. Local section labels are small and aligned at the top; no enclosing column boxes.
Left column: four compact cards stacked in order, with generous connector gaps. Center column: the query-file card at top, S3 in the middle, and staging/cache below. Align the left expansion card horizontally with S3, so their connection is short and direct. Right column: a vertically centered schema card, plus a quiet factual note beneath. Its lower edge is near the staging/cache card, leaving a short bent connection between them. The schema card contains four aligned rows with horizontal hairline separators. Let card height follow content rather than stretching all three columns to equal height.

EXACT TEXT LABELS — PRIVATE:
Column labels: "Corpus preparation", "Files and staging", "PostgreSQL table".
Left card 1: "Source corpus", with "The Wheel of Time · EPUB text" and "16 book filter values".
Left card 2: "100,105 text chunks", with "text · book_name" and "chapter_number · chapter_title".
Left card 3: "100,105 real embeddings", with "text-embedding-ada-002" and "1,536 dimensions".
Left card 4: "2,500,000 benchmark rows", with "Duplicated chunks · varied metadata" and "Synthetic vector expansion".
Center top card: "4,373 query records", with "Query embeddings · keywords", "Filter field + value". Do not add an incoming arrow to this separately supplied dataset.
Center middle card: "Amazon S3", with five caption lines: "gen_wot_2.5m.parquet", "30.94 GB · 2,500,000 rows", "wot_retrieval_queries.parquet", "4,373 queries", "SHA256SUMS".
Center lower card: "Verified dataset cache", with "Staging Job · SHA-256 check", "80 GiB EBS gp3 PVC", "Vector file + query file".
Right schema card: "wot_chunks_2_5m", followed by four aligned monospace field-and-type rows: "id" and "bigserial PK"; "content" and "text"; "metadata" and "jsonb"; "embedding" and "vector(1536)". The schema row labels intentionally reuse words from the chunk metadata where applicable.
Below the schema, two quiet lines: "Query file: retrieval client input", "Only vector rows enter this table".

VISUAL CONNECTIONS — PRIVATE:
Exactly seven arrows, one small filled destination head each. Corpus transformations and table loading use medium solid indigo lines; file storage and staging use thinner dashed blue #0369A1 lines. Orthogonal routes stay in the gaps. Labels are at least 30 px, beside straight segments, on small white backings clear of card borders.
1. Source corpus down to text chunks, label "chunk text".
2. Text chunks down to real embeddings, label "embed chunks".
3. Real embeddings down to benchmark rows, label "expand dataset".
4. Benchmark rows rightward to Amazon S3, dashed blue, label "write vector Parquet".
5. Query records down to Amazon S3, dashed blue, label "write query Parquet".
6. Amazon S3 down to Verified dataset cache, dashed blue, label "download + verify".
7. Verified dataset cache rightward to the schema card, solid indigo, label "COPY vector rows".
Do not draw a query-records-to-table arrow.

CALLOUTS — PRIVATE:
At bottom-left, two quiet factual lines: "Smallest book: 63,945 rows · 2.6%", "Largest book: 209,226 rows · 8.4%". No yellow banner.

LEGEND — PRIVATE:
Bottom-right, a small outlined white box with four swatches in two columns: slate "Source / files", indigo "Vector preparation", blue "Object storage", green "Database table". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Eight cards, seven arrows and four schema rows. Query records are a separate input to S3. The cache contains both files; only the vector file's rows feed COPY. Synthetic expansion is explicit. No invented expansion multiplier, query-generation model, corpus word count or claim that all 2.5 million vectors are original embeddings. No standalone title, large empty cards or rendering syntax. Previous output printed line widths beside the arrows ("4 px", "3 px"): no sizes, units or styling words appear anywhere in the image.
```

---

## Two-Compounding-Problems.png

**What it shows:** plan selection and result-count completeness as separate concerns. The left panel uses the recorded `LIMIT 41` plan change; the right uses the recorded `LIMIT 40` HNSW result of 18 rows with iterative scanning off. A good row estimate does not universally mandate HNSW, and enabling iterative scans does not guarantee exact recall or an unlimited search.

**Evidence:** [no-statistics plans](../results/eks-20261022-trap-no-stats.txt), [post-ANALYZE plans](../results/eks-20261022-fixed-stats.txt), and [pgvector iterative-scan behavior and limits](https://github.com/pgvector/pgvector#iterative-index-scans).

**Prompt:**

```text
Create a polished two-panel technical comparison on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the panels. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below. Explicit repetitions are allowed only where requested.

VISUAL STYLE — PRIVATE:
Precise engineering-whitepaper styling: compact cards, small colored icon tabs, pale fills, thin borders, modest matching corners, no shadows, gradients, thick bottom edges or decorative backgrounds. Use Inter-like sans-serif with semibold #0F172A names about 40 px, local panel labels 36 px, regular #475569 metadata and arrow labels at least 30 px. Use monospace for settings. Left-align text and align corresponding cards across the two panels where practical.
Indigo #4F46E5 on #EEF2FF identifies HNSW; amber #B45309 on #FFFBEB identifies estimates; red #B91C1C on #FFF1F2 identifies the expensive observed plan or missing rows; green #15803D on #F0FDF4 identifies corrective steps. Neutral elements are slate on #F8FAFC. Small outline icons: query document, estimate chart, cost scales, network graph, page stack, filter funnel and check. All icons share a 40 px size and stroke.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins, two equal-width columns and a generous center gutter. Each column has one faint outline with a small local heading. No extra nested frames.
Left column: query card, estimate card and compact cost-comparison card stacked vertically; beneath, two equally sized outcome cards side by side. A shallow green remedy card sits below both outcomes.
Right column: HNSW card, a compact candidate-grid-and-filter illustration, then a result card. Its green remedy card aligns with the left remedy card. Leave room beneath both panels for two short notes and a small legend.
The candidate illustration has a five-by-eight grid of forty small circles. Exactly eighteen are solid indigo with a tiny white check; twenty-two are grey with a small cross. Put the funnel below the grid. This grid is an observed candidate example, not a proportional illustration of the whole table.

EXACT TEXT LABELS — PRIVATE:
Left panel heading: "1 · Plan selection"; secondary label: "Before execution".
Query card: "Filtered nearest neighbors", with "00. New Spring · LIMIT 41" and "ef_search = 40 · iterative_scan = off".
Estimate card: "Estimated matching rows", with "Without statistics: 12,500", "After ANALYZE: 62,172", "Actual: 63,945".
Cost-comparison card: "Planner cost comparison".
Left outcome card, red: "Bitmap scan + sort", with "Observed without statistics".
Right outcome card, indigo: "HNSW index scan", with "Observed after ANALYZE".
Left green remedy card: "ANALYZE after index creation", with "Refresh expression statistics".
Right panel heading: "2 · Result completeness"; secondary label: "During an HNSW scan".
HNSW card: "Initial HNSW search", with "00. New Spring · LIMIT 40" and "ef_search = 40 · iterative_scan = off". The settings line intentionally repeats from the left query card.
Candidate illustration caption: "Observed: 18 matches · 22 rejected". Beside the funnel: "book_name filter".
Result card, red: "18 rows returned", with "40 requested · 22 missing".
Right green remedy card: "Enable iterative scans", with "Continue within scan and memory limits".

VISUAL CONNECTIONS — PRIVATE:
Exactly six directed connectors, each with one small filled head and a readable label. Use 3 px solid slate for planning links, 4 px solid red or indigo for plan outcomes, and 4 px solid indigo in the scan column. Keep arrows in the gaps, clear of the grid and text; labels have small white backings.
1. Left query card down to Estimated matching rows, slate, label "estimate selectivity".
2. Estimated matching rows down to Planner cost comparison, slate, label "compare costs".
3. Planner cost comparison down-left to Bitmap scan + sort, red, label "without statistics".
4. Planner cost comparison down-right to HNSW index scan, indigo, label "after ANALYZE".
5. Initial HNSW search down to the candidate illustration, indigo, label "candidate rows".
6. Funnel down to the result card, indigo, label "matching rows".
No arrows between panels or from the remedy cards. The forty grid circles have no connectors of their own.

CALLOUTS — PRIVATE:
Below the panels at left, two quiet lines: "Iterative scanning helps only when HNSW is selected.", "Returning k rows does not establish nearest-neighbor recall.".

LEGEND — PRIVATE:
Bottom-right, a small white outlined box: amber "Estimates", indigo "HNSW", red "Observed problem", green "Corrective step", in two columns. No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Two panels, six arrows, two green remedy cards. The plan example requests 41; the completeness example requests 40. The grid contains exactly forty circles, eighteen matches and twenty-two rejected rows. Do not equate ef_search with an exact universal count of candidates or graph visits. Do not imply ANALYZE always chooses HNSW, iterative scans always return k, or full row counts prove recall. No latency figures, title, large yellow banner or rendering syntax.
```

---

## planner-row-estimates.png

**What it shows:** why analyzing the table before creating a new expression index does not supply that index's expression statistics. It compares queries issued before a subsequent analysis with queries issued after it. This describes the missing-statistics condition, not the current fixed full-study sequence. Manual `ANALYZE` or a later automatic analyze can collect the new statistics; the sampled estimate shown is 62,172, not the exact count of 63,945.

**Evidence:** [index creation and statistics check](../../src/pgvector/02_create_indexes.py), [recorded comparison](../RUN_EVIDENCE.md), [sampled estimate](../results/eks-20261022-fixed-stats.txt), and [PostgreSQL's notes on new expression indexes](https://www.postgresql.org/docs/18/sql-createindex.html#SQL-CREATEINDEX-NOTES).

**Prompt:**

```text
Create a polished technical sequence with a two-way comparison beneath it, on pure white, 3:2 landscape at 3072 × 2048 or higher. No standalone title, subtitle or empty title band. Begin directly with the diagram content. Instructions are private; print only the quoted labels below, with repetitions only where explicitly requested.

VISUAL STYLE — PRIVATE:
Flat, carefully aligned engineering-whitepaper cards with fine borders, pale fills, modest identical corners and compact colored icon tabs. No shadows, gradients, thick bottom edges, perspective or texture. Use Inter-like sans-serif: semibold #0F172A names about 40 px, regular #475569 captions and arrow labels at least 30 px. Use monospace for SQL and field names. All text is left-aligned. Use equal padding and matching numeric emphasis in the two lower panels; avoid oversized infographic numerals.
Preparation cards are indigo #4F46E5 on #EEF2FF or neutral slate on #F8FAFC. Missing statistics use amber #B45309 on #FFFBEB. Collected statistics use green #15803D on #F0FDF4. Small 40 px outline icons: stacked rows, statistics chart, index tree, question mark and sampled-data chart. Do not use a check mark to imply that a sampled estimate is exact.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. The upper third contains three equally sized cards in one horizontal row, with wide connector gaps. The lower half contains two equal side-by-side comparison panels, each with one thin outline, aligned headings, numeric rows and caption baselines. Leave a generous horizontal lane between the upper and lower parts for two branch connectors. Keep a small footer below the comparison panels.
Within each lower panel, show a shallow action strip, a statistics-status line, an emphasized estimate and a quieter actual-count line. Use subtle horizontal rules rather than additional boxes around each value.

EXACT TEXT LABELS — PRIVATE:
Upper card 1: "1 · Bulk load", with "2,500,000 rows".
Upper card 2: "2 · Analyze before index creation", with "Base-table statistics available", "New indexed expression not yet present".
Upper card 3: "3 · Create expression index", with "B-tree on metadata->>'book_name'", "Expression statistics not collected here".
Left lower panel label: "Query before another ANALYZE".
Left action strip: "No post-index analysis yet".
Left status: "Expression statistics: absent".
Left emphasized estimate: "Estimated: 12,500 rows".
Left explanation: "2,500,000 × 0.005 default estimate".
Left actual-count line: "Actual: 63,945 rows".
Right lower panel label: "Query after ANALYZE".
Right action strip, monospace: "ANALYZE wot_chunks_2_5m;".
Right status: "Expression statistics: sampled".
Right emphasized estimate: "Estimated: 62,172 rows".
Right explanation: "Measured sample from this run".
Right actual-count line: repeat "Actual: 63,945 rows".
Above the pair of panels, a small shared context caption: "Same filter: 00. New Spring". It is local to the comparison, not a page title.

VISUAL CONNECTIONS — PRIVATE:
Exactly four arrows, each with a single filled destination head. Use solid slate #64748B at 3 px for the upper sequence, amber at 4 px for the left branch and green at 4 px for the right branch. Labels sit in white clear space beside straight segments.
1. Bulk load rightward to Analyze before index creation, slate, label "analyze table".
2. Analyze before index creation rightward to Create expression index, slate, label "create index".
3. Create expression index to the top edge of the left lower panel, amber, label "query before refresh". Route through the lane above both lower panels.
4. Create expression index to the top edge of the right lower panel, green, label "run ANALYZE". Use a separate route from the amber branch.
No timeline baseline with an extra arrowhead, and no connector between the lower panels.

CALLOUTS — PRIVATE:
At bottom-left, two quiet lines: "New expression-index statistics need a later ANALYZE.", "Manual or automatic analysis can collect them.".

LEGEND — PRIVATE:
Bottom-right, a small white box with a fine grey outline and three swatches: indigo "Preparation", amber "Missing statistics", green "Sampled statistics". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Three upper cards, two comparison panels, four arrows. Both actual counts are 63,945; the estimates are 12,500 and 62,172. The comparison does not claim all expression statistics require an index, that autoanalyze always runs at a fixed point, or that sampled statistics are exact. No guessed histogram, inconsistent sample percentages, latency figures, standalone title or rendering syntax.
```

---

## b-tree-index-and-bitmap-heap.png

**What it shows:** the measured `LIMIT 41` bitmap plan for `00. New Spring` without expression statistics: B-tree lookup, exact TID bitmap and heap access, then distance evaluation and top-N sorting. The second execution took 358.129 ms, with about 4 ms in the bitmap index scan and 50,143 exact heap blocks. Buffer hits are accesses, not a count of unique pages. The diagram does not invent a per-phase timing breakdown.

**Evidence:** [recorded plan, section 3b](../results/eks-20261022-trap-no-stats.txt), [PostgreSQL bitmap-scan description](https://www.postgresql.org/docs/18/indexes-bitmap-scans.html), and [exact versus lossy TID bitmap representation](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/nodes/tidbitmap.c). The exact bitmap tracks tuple offsets within pages; “one bit per table page” describes lossy storage and is inappropriate for this observed plan.

**Prompt:**

```text
Create a polished technical query-execution diagram on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin with the content. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels, once each except explicit repetitions.

VISUAL STYLE — PRIVATE:
Use restrained engineering-whitepaper styling: flat compact cards, small colored icon tabs, fine outlines, pale fills and matching modest corners. No shadows, gradients, extrusion, thick bottom edges or background ornament. Inter-like sans-serif: semibold #0F172A component names around 40 px; regular #475569 metadata and arrow labels at least 30 px. SQL uses readable monospace. Left-align text, keep even padding and align the three phase headers.
Amber #B45309 on #FFFBEB marks the estimate; green #15803D on #F0FDF4 marks the B-tree; blue #2563EB on #EFF6FF marks bitmap and page access; red #B91C1C on #FFF1F2 marks distance work and sorting in this observed expensive plan. Small matching outline icons: query document, estimate chart, tree, page grid, CPU, ordered list and result rows.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. A compact query card and estimate card sit side by side across the upper quarter. Under them, three equal-width phase panels occupy the middle and lower canvas, separated by broad connector gutters. Each has one thin outline, a small numbered heading and generous internal spacing.
Phase 1 contains a small tree schematic above a row-address caption. Phase 2 contains an exact-bitmap schematic above a sparse row of page tiles. Phase 3 contains two compact internal blocks, distance evaluation above top-N sorting, and a small result strip below. Leave the footer for the measured total and a small legend.
The tree uses one root, two branches and four leaf rectangles, connected by fine lines with no arrowheads; one path is green. These structural edges carry no text.
The exact-bitmap schematic contains three small page-entry rectangles, each with a row of tiny offset squares inside; selected offsets are blue. Below, twelve page tiles have a few blue selected tiles and other pale-grey tiles. These tile counts are illustrative, not a scale model of the table. Avoid a single continuous one-bit-per-page bitmap.

EXACT TEXT LABELS — PRIVATE:
Query card, four monospace lines: "WHERE metadata->>'book_name'", "  = '00. New Spring'", "ORDER BY embedding <=> $1", "LIMIT 41".
Estimate card: "Planner estimate", with "12,500 matching rows" and "No expression statistics".
Phase 1 heading: "1 · B-tree lookup".
Beside the tree, small caption: "Index scan: about 4 ms".
Below the tree: "63,945 matching row locations".
Phase 2 heading: "2 · Bitmap heap scan".
Bitmap schematic caption: "Exact bitmap: pages + tuple offsets".
Page-tile caption: "Selected heap pages · schematic".
Below the tiles, two lines: "50,143 exact heap blocks visited", "Heap access in physical page order".
Phase 3 heading: "3 · Distance and top-N sort".
First internal block: "Evaluate distance", with "63,945 rows · 1,536 dimensions".
Second internal block: "Top-N heapsort", with "Keep the nearest 41".
Result strip: "41 rows returned".

VISUAL CONNECTIONS — PRIVATE:
Exactly six directed arrows, all with one filled destination head and a short label. Main connectors are solid slate #64748B, 3 px; arrows within phase 3 are solid red, 3 px. Use orthogonal segments in gutters and gaps; labels sit on small white backings, clear of borders. Keep the route from the estimate to phase 1 in the gap below the upper row.
1. Query card rightward to Planner estimate, slate, label "estimate rows".
2. Planner estimate to the upper edge of phase 1, slate, label "chosen bitmap plan".
3. Phase 1 rightward to phase 2, slate, label "row locations".
4. Phase 2 rightward to Evaluate distance, slate, label "matching rows".
5. Evaluate distance down to Top-N heapsort, red, label "distance values".
6. Top-N heapsort down to the result strip, red, label "nearest rows".
No extra arrows inside the tree, bitmap schematic or page tiles.

CALLOUTS — PRIVATE:
Bottom-left, two quiet lines: "Observed total: 358 ms · warm-cache second run", "Estimated 12,500 matches; actual 63,945". Total time is not presented as the duration of any single phase.

LEGEND — PRIVATE:
Bottom-right, one compact white outlined box with four entries in two columns: amber "Estimate", green "Index lookup", blue "Heap access", red "Distance / sort". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Two upper cards, three phase panels, six directed arrows. Exact bitmap entries contain tuple offsets. The page-tile illustration is explicitly schematic. Only the total and bitmap-index timings are shown. Do not label buffer hits as unique pages, show one bit per page for this exact bitmap, claim every row requires a filter recheck, or imply a cold-cache measurement. No standalone title, yellow banner, extra arrowheads or rendering syntax.
```

---

## book-filter-distribution.png

**Status: generated by script, not by the image model.** The image generator rejected the prompt that listed all sixteen book titles with a policy failure (the two titles already used in accepted prompts pass; the full list of recognisable copyrighted titles does not). A bar chart also needs exact proportions, which the image model cannot promise. Use [plot_book_distribution.py](../../scripts/plot_book_distribution.py):

```bash
export $(grep -v '^#' .env.aws | xargs)
uv run python scripts/plot_book_distribution.py            # queries the vector database
uv run python scripts/plot_book_distribution.py --csv doc/results/book-row-counts.csv   # or from a saved CSV (book_name,rows)
```

It writes `doc/diagrams/book-filter-distribution.png` at 3:2, in the shared palette: slate bars, red for the smallest book, indigo for the largest, counts printed in muted ink, no title. Save the query output as a CSV under `doc/results/` so the figure can be regenerated without the cluster.

**What it shows:** the row count of each of the 16 `book_name` values in the 2.5 million row table in book order, so a reader sees why `00. New Spring` (63,945 rows, 2.6%) is the most selective filter and `06. Lord of Chaos` (209,226 rows, 8.4%) the least. Only three counts are recorded in this repository (the third is `13. Towers of Midnight`, 168,426); the current image invents the rest and duplicates "00. New Spring (prequel)" as a 16th bar, which contradicts the text.

**Evidence:** [recorded counts for three books](../results/eks-20261022-fixed-stats.txt) (section 1b), [DATASET.md](../DATASET.md), and the per-book count query in [perf_audit_pgvector_2.5m_wot.sql](../../scripts/perf_audit_pgvector_2.5m_wot.sql).

**Fallback image-model prompt** (only if the script route is unavailable). It avoids the policy trigger by printing the two annotated titles only and numbering the other bars; fill the bar lengths from the query output first. Bars without a printed count stay schematic.

```text
Create a polished horizontal bar chart on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the chart. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: flat bars, thin #CBD5E1 axis lines, hairline vertical gridlines only, no shadows, gradients, 3D or decorative background. Inter-like sans-serif: row labels regular #0F172A at least 32 px, axis ticks #475569 at least 28 px, value annotations semibold at least 30 px. Left-align the row labels in one column.
Bars are slate #94A3B8 except the smallest, which is red #B91C1C, and the largest, which is indigo #4F46E5.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. A label column on the left about one fifth of the width, then the bar area. Exactly sixteen bars with equal heights and gaps, top to bottom in the order listed. The x axis runs along the bottom from 0 to 250k with ticks at 0, 50k, 100k, 150k, 200k and 250k. Leave a narrow footer for one note and a compact legend.
Bar lengths follow the row counts supplied with this prompt; the first bar is the shortest and the seventh is the longest.

EXACT TEXT LABELS — PRIVATE:
Row labels, top to bottom: "00. New Spring", "Book 01", "Book 02", "Book 03", "Book 04", "Book 05", "06. Lord of Chaos", "Book 07", "Book 08", "Book 09", "Book 10", "Book 11", "Book 12", "Book 13", "Book 14", "Book 15".
X axis caption: "Rows per book_name value".
Annotation after the first bar: "63,945 rows · 2.6% · most selective".
Annotation after the seventh bar: "209,226 rows · 8.4% · least selective".
Annotation after the fourteenth bar: "168,426 rows".
Print a count after any other bar only if the supplied table gives one.

VISUAL CONNECTIONS — PRIVATE:
No arrows, leader lines or connectors. Annotations sit directly after their bar.

CALLOUTS — PRIVATE:
Footer-left, one quiet line: "16 distinct values · 2,500,000 rows total · uneven by design".

LEGEND — PRIVATE:
Bottom-right, a small white outlined box with three swatches: red "Smallest book", indigo "Largest book", slate "Other books". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Exactly sixteen bars and sixteen distinct labels; only two full titles are printed. The red bar is the shortest and the indigo bar the longest. No invented counts. No standalone title, banner, arrows or rendering syntax.
```

---

## search-patterns.png

**What it shows:** one row of the 4,373-row query dataset and how the three benchmark search patterns consume it unchanged: vector search uses the embedding alone, filtered search adds the `book_name` filter value, hybrid search adds the keyword list for full-text scoring with Reciprocal Rank Fusion. The example values are the ones in the current image.

**Evidence:** [query dataset schema](../DATASET.md), [retrieval benchmark](../../src/pgvector/03_retrieval_asyncpg.py), and the README section "The Benchmark".

**Prompt:**

```text
Create a polished technical flow diagram on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the cards. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: flat compact cards, small colored icon tabs, fine outlines, pale fills, modest uniform corners, no shadows, gradients or decorative background. Inter-like sans-serif: semibold #0F172A card names about 40 px, regular #475569 descriptions at least 30 px, arrow labels at least 30 px. Field names and values use readable monospace. Left-align text with even padding.
The query-row card is slate on #F8FAFC with a slate icon tab. The three pattern cards use indigo #4F46E5 on #EEF2FF for vector search, amber #B45309 on #FFFBEB for filtered search, and green #15803D on #F0FDF4 for hybrid search. Small matching outline icons: table row, network graph, filter funnel, merged streams. All icons share a 40 px size and stroke.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. One tall query-row card occupies the left third of the canvas, vertically centered. Three equal pattern cards are stacked in the right half with equal gaps, their combined height matching the query-row card. A generous central gutter carries the three arrows and their labels. Leave a narrow footer for one note.

EXACT TEXT LABELS — PRIVATE:
Query-row card name: "One query row". Below it, four monospace field groups separated by small gaps:
"query_text:" then "\"What is the Dragon Reborn's true name?\"";
"query_embedding:" then "[1,536-dim vector]";
"keywords:" then "[\"Dragon\", \"Reborn\", \"true\", \"name\"]";
"filter_value:" then "\"01. The Eye of the World\"".
Top pattern card name: "Vector search", with two lines "query_embedding only" and "Nearest neighbors by cosine distance. No filter.".
Middle pattern card name: "Filtered search", with two lines "query_embedding + filter_value" and "Nearest neighbors within one book via metadata->>'book_name'.". The quotation marks around book_name are straight single quotes, as in SQL; never double quotes.
Bottom pattern card name: "Hybrid search", with two lines "query_embedding + keywords" and "Vector and full-text ranks fused with RRF. No filter.".

VISUAL CONNECTIONS — PRIVATE:
Exactly three directed arrows, each with one small filled destination head and a short label on a white backing in the gutter. Each arrow leaves the right edge of the query-row card at the height of its destination card and enters the left edge of that card. Use 4 px solid indigo for the top arrow, 4 px solid amber for the middle arrow and 4 px solid green for the bottom arrow.
1. To Vector search, label "embedding".
2. To Filtered search, label "embedding + filter".
3. To Hybrid search, label "embedding + keywords".
No other connectors.

CALLOUTS — PRIVATE:
Footer, one quiet line: "4,373 query rows · each row used unmodified in all three patterns".

LEGEND — PRIVATE:
None; the arrow labels and card names are sufficient.

FINAL CHECK — PRIVATE, DO NOT PRINT:
One query-row card, three pattern cards, three arrows. Field values are printed exactly, with quotation marks where given; book_name is wrapped in straight single quotes. No standalone title, no legend, no extra icons, no rendering syntax.
```

---

## m-Parameter.png

**What it shows:** the HNSW `m` parameter as the maximum number of edges a node keeps to its neighbors within a layer, comparing a low setting with the benchmark default of 16. Each greedy search hop evaluates the current node's neighbors, so `m` shapes graph connectivity, build time, memory and per-hop search cost. The two panels are neutral alternatives; neither is a failure.

**Evidence:** [pgvector HNSW options](https://github.com/pgvector/pgvector#hnsw), the README section "HNSW Index", and the index build settings in [02_create_indexes.py](../../src/pgvector/02_create_indexes.py).

**Prompt:**

```text
Create a polished two-panel technical comparison on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the panels. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: thin panel outlines, pale fills, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A panel labels about 40 px, regular #475569 captions at least 30 px. Settings use readable monospace.
Graph nodes are circles with a thin #475569 outline and a #F8FAFC fill; the center node in each panel is solid indigo #4F46E5. Edges are 3 px solid slate #64748B with no arrowheads. Both panels use the same neutral treatment; do not color one panel green and the other orange.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins and two equal-width panels with a generous center gutter and a hairline divider. Each panel has a small local heading at the top, a centered star-shaped graph schematic in the middle, and a two-line caption near the bottom. Leave a narrow footer for one note.
Left panel: one indigo center node with exactly four neighbor nodes spaced evenly around it, each joined to the center by one straight edge. No edges between neighbors, and no ring, circle or arc drawn through or around the neighbor nodes; the only lines are the four spokes.
Right panel: one indigo center node with exactly sixteen neighbor nodes spaced evenly on one ring around it, each joined to the center by one edge. No edges between neighbors. Keep the ring large enough that edges remain distinct.

EXACT TEXT LABELS — PRIVATE:
Left heading: "Low m", with monospace "m = 4" beneath.
Right heading: "Benchmark default", with monospace "m = 16" beneath.
Left caption, two lines: "Each hop evaluates 4 neighbors.", "Cheaper hops · sparser graph · more dead ends".
Right caption, two lines: "Each hop evaluates 16 neighbors.", "Costlier hops · denser graph · fewer dead ends".

VISUAL CONNECTIONS — PRIVATE:
The graph edges are the only lines. Exactly four edges in the left panel and sixteen in the right. No directed arrows anywhere.

CALLOUTS — PRIVATE:
Footer, one quiet line: "m is set at build time · affects build time, index memory and per-hop search cost".

LEGEND — PRIVATE:
None.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Two panels, four and sixteen neighbors, twenty straight spokes in total, no arrowheads, no ring or circle through the neighbors in either panel. Neither panel is marked as good or bad. No claim about recall percentages or layer-specific edge counts. No standalone title, no icons, no rendering syntax.
```

---

## ef_construction.png

**What it shows:** `ef_construction` as the size of the candidate list searched when a new vector is inserted into the HNSW graph, before the best `m` neighbors are chosen as edges. A larger list evaluates more candidates and produces better edges at the cost of build time, and has no effect on query latency. The benchmark used the default of 64; the 2.5 million row build took 96 minutes.

**Evidence:** [pgvector HNSW options](https://github.com/pgvector/pgvector#hnsw), and the README sections "The Benchmark" (recorded 96-minute build) and "HNSW Index".

**Prompt:**

```text
Create a polished two-panel technical comparison on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the panels. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: thin panel outlines, pale fills, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A panel labels about 40 px, regular #475569 captions at least 30 px. Settings use readable monospace.
Existing graph nodes are small circles with a thin #94A3B8 outline and #F8FAFC fill. The node being inserted is solid amber #B45309 with a thin dark outline. Candidate nodes evaluated during insertion are filled #CBD5E1. Chosen neighbors are solid indigo #4F46E5. The candidate region is a dashed 3 px #0369A1 circle with a very pale #EFF6FF fill. Chosen edges are 4 px solid indigo with no arrowheads.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins and two equal-width panels with a generous center gutter and a hairline divider. Each panel has a small local heading, a point-cloud schematic of about forty evenly scattered nodes in the middle, and a two-line caption near the bottom. Leave a narrow footer for one note.
Left panel: the amber node sits near the center. A small dashed circle around it encloses about six candidate nodes. Four of them are indigo and joined to the amber node by indigo edges. Nodes outside the circle are unchanged.
Right panel: the same layout with a dashed circle about two and a half times the diameter, enclosing about twenty candidate nodes. Four of them, spread more evenly around the amber node, are indigo and joined by indigo edges. The number of chosen edges is the same in both panels.
These counts are schematic; the panels illustrate a wider candidate search, not a measured number of visited nodes.

EXACT TEXT LABELS — PRIVATE:
Left heading: "Small candidate pool", with monospace "ef_construction = 64" beneath.
Right heading: "Large candidate pool", with monospace "ef_construction = 256" beneath.
Inside each dashed circle, near its edge, a small label: left "candidates searched", right the same words.
Left caption, two lines: "Fewer candidates compared before choosing m edges.", "Faster build · edges may miss closer neighbors".
Right caption, two lines: "More candidates compared before choosing m edges.", "Slower build · edges closer to the true neighbors".

VISUAL CONNECTIONS — PRIVATE:
The chosen edges are the only lines: four per panel, eight in total, no arrowheads. No directed arrows anywhere.

CALLOUTS — PRIVATE:
Footer, one quiet line: "Build-time only · no effect on query latency · benchmark used 64, build took 96 minutes".

LEGEND — PRIVATE:
Bottom-right, a small white outlined box with three swatches: amber "New vector", pale slate "Candidate evaluated", indigo "Chosen neighbor". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Two panels, one amber node each, four indigo edges each, one dashed circle each with the right circle clearly larger. No arrowheads. Do not print a recall percentage or claim a specific number of candidates is visited. No standalone title, large banner or rendering syntax.
```

---

## ef_search.png

**What it shows:** `ef_search` as the size of the candidate queue used while exploring layer 0 at query time. A small queue stops exploring sooner and is faster; a large queue explores more of the neighborhood for better recall at higher latency. It is a session setting and must be at least `top_k`. Visited-node counts in the drawing are schematic.

**Evidence:** [pgvector query options](https://github.com/pgvector/pgvector#query-options), the README section "HNSW Index", and `SET hnsw.ef_search` usage in [03_retrieval_asyncpg.py](../../src/pgvector/03_retrieval_asyncpg.py).

**Prompt:**

```text
Create a polished two-panel technical comparison on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the panels. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: thin panel outlines, pale fills, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A panel labels about 40 px, regular #475569 captions at least 30 px. Settings use readable monospace.
Graph nodes are small circles with a thin #94A3B8 outline and #F8FAFC fill, joined by hairline #CBD5E1 edges with no arrowheads to form an irregular mesh. The query position is a small red #B91C1C cross, not a node. Visited nodes are solid indigo #4F46E5. The visited path is drawn as 3 px solid indigo segments with small filled heads between consecutive visited nodes. The visited region has a very pale #EEF2FF tint. Each panel includes a small queue widget: a white outlined rectangle with a thin indigo bar whose fill fraction indicates how much of the neighborhood was queued; the widget is schematic.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins and two equal-width panels with a generous center gutter and a hairline divider. Each panel has a small local heading at the top, the same layer-0 mesh of about sixty nodes in the middle with the query cross at the same position, a queue widget in the upper-right corner of the mesh, and a two-line caption near the bottom. Leave a narrow footer for one note.
Left panel: a short visited path of about six indigo nodes approaching the query cross, with a small tinted region.
Right panel: a longer visited path of about twenty indigo nodes winding around the query cross, with a tinted region roughly four times the area. Keep paths clear of the queue widget.

EXACT TEXT LABELS — PRIVATE:
Left heading: "Narrow search", with monospace "ef_search = 40" beneath.
Right heading: "Wide search", with monospace "ef_search = 400" beneath.
In the top-left of each mesh, a small label "Layer 0".
Beside the red cross in the left panel only: "query vector".
Queue widget label, left: "candidate queue · 40"; right: "candidate queue · 400".
Left caption, two lines: "Queue fills quickly; exploration stops early.", "Lower latency · lower recall".
Right caption, two lines: "Larger queue keeps exploring the neighborhood.", "Higher latency · higher recall".

VISUAL CONNECTIONS — PRIVATE:
The only directed marks are the small heads between consecutive visited nodes along each path. Mesh edges have no heads. No arrows cross between panels or point at the queue widgets.

CALLOUTS — PRIVATE:
Footer, one quiet line: "Query-time setting · SET hnsw.ef_search = N · must be at least top_k".

LEGEND — PRIVATE:
Bottom-right, a small white outlined box with three entries: indigo dot "Visited node", red cross "Query vector", pale indigo "Explored region". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Two panels, same mesh, same query position, visibly different path lengths and tinted areas. Visited counts are schematic and not printed as numbers. Do not claim a recall percentage or an exact multiple of nodes visited. No standalone title, large banner or rendering syntax.
```

---

## HNSW-Post-Filtering.png

**What it shows:** why a `WHERE` clause on an HNSW scan forces over-exploration. The index returns candidates in distance order regardless of book; the filter discards those from other books; the scan must expand outward to collect enough matches. The lower band gives the worst-case uniform-distribution estimate from the README: `00. New Spring` is 2.6% of rows, so about 38 candidates per match and about 1,920 candidates for `LIMIT 50`, while a single non-iterative pass with the default `ef_search = 40` collects at most 40 candidates. The estimate assumes the book's chunks are spread evenly through the space; the README notes that in practice a query about one book usually lands near that book's chunks, so the real count varies. The current image's line "The index never had a chance" is replaced with a factual statement.

**Evidence:** README section "HNSW Index", the critical detail for filtered search; [pgvector filtering notes](https://github.com/pgvector/pgvector#filtering); the recorded `LIMIT 40` HNSW result of 18 rows in [the no-statistics plans](../results/eks-20261022-trap-no-stats.txt).

**Prompt:**

```text
Create a polished technical mechanism diagram on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the content. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each except explicit repetitions.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: thin outlines, pale fills, compact cards, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A section labels about 40 px, regular #475569 captions and arrow labels at least 30 px, card values semibold at least 34 px. Settings use readable monospace.
Candidate nodes are small circles. Nodes that pass the book filter are solid indigo #4F46E5 with a tiny white check. Nodes that fail it are #CBD5E1 with a small grey cross. The query is a solid amber #B45309 circle at the center. Distance rings are thin dashed #94A3B8 circles. The expansion arrow is 4 px solid red #B91C1C. Cards: amber on #FFFBEB for the selectivity figure, red on #FFF1F2 for the required-candidates figure, slate on #F8FAFC for the single-pass figure.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. The left 60% of the canvas holds the mechanism: the amber query circle at the center of three concentric dashed rings. About 40 candidate nodes sit on and between the rings, denser near the center. Exactly four of them are indigo matches, placed one on the inner ring, one on the middle ring and two on the outer ring; all others are grey rejects. A single straight red arrow points radially outward, starting just outside the inner ring and ending just outside the outer ring, in the lower-right sector where no node sits on its path. It has one head at the outer end and no other heads, branches or curves. Two short slate leader lines with small labels point at one grey node and one indigo node.
The right 40% holds a vertical stack of three cards, aligned to the rings' vertical extent, and a thin horizontal proportion bar above them. The proportion bar is a long grey #E2E8F0 rectangle with an indigo segment occupying 2.6% of its length at the left end. Leave a footer for one note and a compact legend.

EXACT TEXT LABELS — PRIVATE:
Above the rings, small local label: "HNSW candidates in distance order".
Leader label at a grey node: "other book · discarded by filter".
Leader label at an indigo node: "00. New Spring · kept".
Beside the red arrow, label: "scan expands to replace discarded rows".
Above the proportion bar: "00. New Spring share of the table".
Below the proportion bar: "63,945 of 2,500,000 rows · 2.6%".
Top card (amber): "Candidates per match", with "1 ÷ 0.026 ≈ 38" and "if the book were spread evenly".
Middle card (red): "Candidates for LIMIT 50", with "50 × 38 ≈ 1,920" and "worst-case estimate".
Bottom card (slate): "Single pass at default", with monospace "ef_search = 40" and "collects at most 40 candidates".

VISUAL CONNECTIONS — PRIVATE:
Exactly one directed arrow: the straight red expansion arrow with one filled head at its outer end. No curved or branching red lines. The two leader lines are thin slate with no heads. The dashed rings and the proportion bar are not connectors. No arrows between the mechanism and the cards.

CALLOUTS — PRIVATE:
Footer, one quiet line: "Queries about a book usually land near its chunks, so the real candidate count varies; the ratio is the principle.".

LEGEND — PRIVATE:
Bottom-right, a small white outlined box with three entries: amber dot "Query", indigo dot "Passes filter", grey dot "Discarded". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
One query, three dashed rings, exactly four indigo nodes among about forty (count them), one straight red arrow with one head, three cards, one proportion bar at 2.6%. The arithmetic reads 1 ÷ 0.026 ≈ 38 and 50 × 38 ≈ 1,920. The numbers are labelled as an estimate, not a measurement. Do not print "never had a chance" or any claim about latency. No standalone title, large banner or rendering syntax.
```

---

## iterative_scan.png

**What it shows:** the three values of `hnsw.iterative_scan` in pgvector 0.8.0 and later. `off` performs one traversal of up to `ef_search` candidates and may return fewer rows than `LIMIT` after filtering. `strict_order` resumes the scan in further batches and emits rows only in exact distance order. `relaxed_order` resumes too but may emit adjacent rows slightly out of distance order, which is faster. Resumption stops at `hnsw.max_scan_tuples` (default 20,000). Iterative scanning only matters when the planner has chosen the HNSW index. The current image has a duplicated word ("results results"), a large yellow banner and a title, all removed here.

**Evidence:** [pgvector iterative index scans](https://github.com/pgvector/pgvector#iterative-index-scans), README section "Iterative index scans", and the benchmark's `strict_order` setting for filtered queries in [03_retrieval_asyncpg.py](../../src/pgvector/03_retrieval_asyncpg.py).

**Prompt:**

```text
Create a polished three-column technical comparison on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the columns. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each except explicit repetitions.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: compact cards, thin outlines, pale fills, modest uniform corners, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A column labels about 40 px, regular #475569 captions at least 30 px. Settings and SQL use readable monospace on #F8FAFC chips.
Graph schematics: small nodes with thin #94A3B8 outlines joined by hairline #CBD5E1 edges with no heads; the query is a solid amber #B45309 circle; visited nodes are solid in the column's accent color; batch boundaries are dashed circles in the accent color. Column accents: slate #475569 on #F8FAFC for off, indigo #4F46E5 on #EEF2FF for strict_order, green #15803D on #F0FDF4 for relaxed_order. The shortfall value in the first column is red #B91C1C text.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins and three equal columns separated by hairline dividers and generous gutters. Each column, top to bottom: a monospace mode chip, a graph schematic of about twenty nodes around a central amber query, a two-line schematic caption, a behavior card with three short lines, and a monospace SET chip. Corresponding elements align across the three columns. Leave a footer for one note.
Column 1 schematic: one dashed slate circle around the query enclosing about six slate visited nodes. Nothing outside it is visited.
Column 2 schematic: two concentric dashed indigo circles; about six indigo visited nodes inside the inner circle and about eight more between the circles, with one small indigo arrow from the inner circle to the outer labelled as below.
Column 3 schematic: the same two concentric circles in green with the same visited counts, plus one tiny double-headed green arrow between two adjacent visited nodes in the outer band.

EXACT TEXT LABELS — PRIVATE:
Mode chips: "hnsw.iterative_scan = off", "hnsw.iterative_scan = strict_order", "hnsw.iterative_scan = relaxed_order".
Column 1 caption: "Single traversal.", "Stops after ef_search candidates.".
Column 2 caption: "Resumes in further batches.", "Rows emitted in exact distance order.".
Column 3 caption: "Resumes in further batches.", "Adjacent rows may swap in distance order.".
Column 2 arrow label: "resume".
Column 3 double-arrow label: "may swap".
Behavior card 1: "Default.", "Filtered queries can return fewer rows than LIMIT." with "fewer rows" in red, "No resumption.".
Behavior card 2: "Exact ordering guaranteed.", "Extra work to confirm no closer unseen row.", "Used by this benchmark for filtered queries.".
Behavior card 3: "Fastest iterative mode.", "Approximate order; fine for RAG retrieval.", "Best recall-to-latency ratio for filtered queries.".
SET chips: "SET hnsw.iterative_scan = off;", "SET hnsw.iterative_scan = strict_order;", "SET hnsw.iterative_scan = relaxed_order;".

VISUAL CONNECTIONS — PRIVATE:
Exactly two small arrows: the indigo "resume" arrow in column 2 with one filled head, and the green double-headed "may swap" arrow in column 3. Graph edges have no heads. No arrows between columns or to the cards.

CALLOUTS — PRIVATE:
Footer, one quiet line: "Resumption stops at hnsw.max_scan_tuples (default 20,000) · applies only when the planner has chosen the HNSW index".

LEGEND — PRIVATE:
None; the column colors are explained by their chips.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Three columns, three mode chips, three SET chips, one dashed circle in column 1 and two in each of columns 2 and 3. Exactly two small arrows in total. No duplicated words. Do not claim iterative scanning returns exactly LIMIT rows, guarantees exact recall, or changes the planner's plan choice. No standalone title, yellow banner or rendering syntax.
```

---

## gin-index.png

**What it shows:** how the GIN index on `to_tsvector('english', content)` is built and what it stores. Step 1 tokenizes each row into lexemes (stopwords removed, words stemmed). Step 2 inverts the mapping into a sorted lexeme list with a posting list of row IDs per lexeme. The index answers "which rows contain dragon" from the posting list (76,198 row IDs) but stores no term frequency, so `ts_rank()` must fetch and re-score every matching row. The three sample sentences and their lexemes are the ones quoted in the README text and must stay identical; posting lists are printed in ascending row-ID order, which is how GIN stores them. The current image's claim that this makes hybrid search "3–4× slower than vector search" is dropped; the measured comparison lives in the hybrid investigation section.

**Evidence:** README section "GIN Index and Full-Text Search", [PostgreSQL GIN internals](https://www.postgresql.org/docs/18/gin-implementation.html), [text search ranking](https://www.postgresql.org/docs/18/textsearch-controls.html#TEXTSEARCH-RANKING), and the recorded 76,198-row `dragon` match in [hybrid component plans](../results/eks-20261022-hybrid-components.txt).

**Prompt:**

```text
Create a polished technical data-structure diagram on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the content. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each except explicit repetitions.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: compact cards, thin outlines, pale fills, modest uniform corners, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A section labels about 40 px, regular #475569 captions at least 30 px. Lexemes, row IDs and SQL use readable monospace at least 30 px.
Row-content cards are slate on #F8FAFC. Lexeme cards are green #15803D on #F0FDF4. The inverted-index table has hairline #CBD5E1 rules; the "dragon" row is highlighted with a #FFF1F2 fill and red #B91C1C text. The two capability cards at the bottom are green on #F0FDF4 for what the index answers and red on #FFF1F2 for what it cannot. Small matching outline icons: text document, tokens, sorted list, check, cross.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. Top band: one monospace SQL chip spanning the width. Below it, the left 45% of the canvas holds Step 1: three row-content cards stacked on the left half of that area, each paired with a lexeme card to its right, and a small caption beneath. The right 55% holds Step 2: a two-column table with eight lexeme rows, a small heading, and a short note card to its right about the highlighted row. Across the bottom, two equal capability cards side by side. Leave a narrow footer for one note.

EXACT TEXT LABELS — PRIVATE:
SQL chip: "CREATE INDEX ON wot_chunks_2_5m USING gin (to_tsvector('english', content));".
Step 1 heading: "1 · Tokenize at build time".
Row card 1: "Row 4021", with "Rand drew on the One Power, the dragon symbol blazing…". Lexeme card 1: "rand · draw · power · dragon · symbol · blaze".
Row card 2: "Row 4022", with "The dragon banner unfurled above the city gates…". Lexeme card 2: "dragon · banner · unfurl · above · citi · gate".
Row card 3: "Row 9901", with "Moiraine spoke of the dragon reborn, the prophecies said…". Lexeme card 3: "moirain · spoke · dragon · reborn · prophecy".
Step 1 caption: "Stopwords removed · words stemmed to lexemes".
Step 2 heading: "2 · Invert: lexeme → row IDs".
Table column headings: "Lexeme (sorted)" and "Posting list (row IDs, ascending)".
Table rows, in order:
"banner" | "4022, 7103, 12881, 34201, …"
"blaze" | "891, 4021, 23445, 56012, …"
"citi" | "4022, 5511, 8834, 19021, …"
"dragon" | "312, 788, 1204, 4021, 4022, 9901, … (76,198 IDs)"
"gate" | "4022, 6612, 14332, …"
"moirain" | "2341, 9901, 18823, 45001, …"
"power" | "783, 2211, 4021, 9034, …"
"reborn" | "9901, 14523, 29834, 56712, …"
Note card beside the table: "76,198 rows contain dragon", with "3% of 2.5M rows · found from the posting list".
Left capability card: "Answers from the index", with "Which rows contain a lexeme?" and "@@ lookup → posting list of row IDs".
Right capability card: "Not stored in the index", with "How often or how prominently it appears" and "ts_rank() must fetch and re-score every matching row".

VISUAL CONNECTIONS — PRIVATE:
Exactly four directed arrows, each with one small filled head and a short label on a white backing. Three thin 3 px slate arrows run from each row card to its lexeme card, the first labelled "to_tsvector" and the other two unlabelled repeats. One 4 px solid green arrow runs from the Step 1 area rightward to the table, labelled "build inverted index". No arrows to or from the capability cards or the note card.

CALLOUTS — PRIVATE:
Footer, one quiet line: "The posting list says which rows match; ranking them needs the row text.".

LEGEND — PRIVATE:
None.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Three row cards, three lexeme cards, one eight-row table with "dragon" highlighted, one note card, two capability cards, four arrows. Lexeme strings match exactly. Posting lists are in ascending order. No speed multiple, no latency claim, no standalone title, yellow banner or rendering syntax.
```

---

## planner-decision.png

**What it shows:** the decision the planner makes before executing a filtered vector search. The estimated number of rows matching the filter feeds two cost estimates: the HNSW index scan with a post-filter, whose cost pgvector scales up as fewer matches are expected (more of the graph must be scanned to fill the `LIMIT`), and the B-tree bitmap scan with a brute-force distance sort, whose cost grows with the expected matches. The recorded totals for `00. New Spring` without statistics are 13,637.69 for HNSW at `LIMIT 40` and 13,557.94 for the bitmap plan at `LIMIT 41`. Replaces the "Query Planner Decision" Mermaid flowchart in the README and article; the Mermaid's "faster when few rows are expected to match" wording was inverted and is corrected here.

**Evidence:** README section "The Query Planner and Cost Model", [recorded LIMIT 40 and 41 plans](../results/eks-20261022-trap-no-stats.txt), [pgvector cost estimation](https://github.com/pgvector/pgvector/blob/master/src/hnswscan.c), and [PostgreSQL planner cost constants](https://www.postgresql.org/docs/18/runtime-config-query.html#RUNTIME-CONFIG-QUERY-CONSTANTS).

**Prompt:**

```text
Create a polished technical decision diagram on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the content. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each except explicit repetitions.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: compact cards, small colored icon tabs, thin outlines, pale fills, modest uniform corners, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A card names about 40 px, regular #475569 metadata and arrow labels at least 30 px. SQL and settings use readable monospace.
Slate on #F8FAFC for the query card; amber #B45309 on #FFFBEB for the estimate and the decision; indigo #4F46E5 on #EEF2FF for the HNSW strategy; green #15803D on #F0FDF4 for the B-tree strategy card; red #B91C1C on #FFF1F2 for the observed expensive outcome; indigo on #EEF2FF for the fast outcome. Small matching outline icons: query document, estimate chart, network graph, tree, scales, result rows.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. Top row: the query card at left and the estimate card at right, aligned. Middle row: two equal strategy cards side by side, each containing a name, a one-line description and a cost-rule line. Below them, one centered decision card shaped as a compact rounded rectangle, not a diamond. Bottom row: two outcome cards aligned under the strategy cards. Leave a footer for two notes and a compact legend. Keep gutters wide for arrow labels.

EXACT TEXT LABELS — PRIVATE:
Query card: "Filtered vector search", with monospace "WHERE metadata->>'book_name' = $book", "ORDER BY embedding <=> $1 LIMIT N".
Estimate card: "Estimated matching rows", with "From expression statistics" and "Default 0.5% when none exist".
Left strategy card (indigo): "A · HNSW index scan + post-filter", with "Approximate graph search, filter applied to candidates" and "Cost ∝ LIMIT ÷ expected matches: fewer expected matches → more graph to scan".
Right strategy card (green): "B · B-tree bitmap scan + distance sort", with "Fetch every matching row, compute every distance, sort" and "Cost ∝ expected matches".
Decision card (amber): "Compare estimated costs", with "Lowest total cost wins".
Left outcome card (indigo): "HNSW chosen", with "Recorded: 1.5 ms".
Right outcome card (red): "Bitmap plan chosen", with "Recorded: 358 ms".

VISUAL CONNECTIONS — PRIVATE:
Exactly seven directed arrows, each with one small filled head. Slate #64748B 3 px for planning links; indigo 4 px to the HNSW outcome; red 4 px to the bitmap outcome. Labels sit on white backings in the gaps.
1. Query card rightward to Estimated matching rows, slate, label "estimate selectivity".
2. Estimated matching rows down-left to strategy A, slate, label "cost A".
3. Estimated matching rows down-right to strategy B, slate, label "cost B".
4. Strategy A down to the decision card, slate, unlabelled.
5. Strategy B down to the decision card, slate, unlabelled.
6. Decision card down-left to HNSW chosen, indigo, label "estimate close to actual".
7. Decision card down-right to Bitmap plan chosen, red, label "estimate far too low".

CALLOUTS — PRIVATE:
Footer-left, two quiet lines: "Recorded for 00. New Spring without statistics: estimate 12,500, actual 63,945.", "Total cost 13,637.69 for HNSW at LIMIT 40; 13,557.94 for the bitmap plan at LIMIT 41.".

LEGEND — PRIVATE:
Bottom-right, a small white outlined box with four swatches: amber "Estimate / decision", indigo "HNSW path", green "B-tree path", red "Observed expensive plan". No heading. Print every legend word in full; widen the box rather than truncating a label.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Seven arrows, six cards plus one decision card. All four legend labels are complete words. Strategy A's cost rises when fewer matches are expected; strategy B's cost rises when more are expected. Do not print "FAST" or "SLOW" badges. Recorded figures keep their book and LIMIT context. No diamond, no standalone title, no rendering syntax.
```

---

## limit-41-plan-steps.png

**What it shows:** the four plan nodes PostgreSQL executed for `00. New Spring` at `LIMIT 41` without expression statistics, as a left-to-right step sequence with the recorded figures: bitmap index scan on the B-tree (63,945 row IDs, about 4 ms), bitmap heap scan (50,143 exact heap blocks, 305,980 buffer hits including out-of-line embeddings), sort by cosine distance over all 63,945 rows of 1,536 dimensions, then the limit. Total 358 ms warm. Replaces the "Catastrophic Plan (LIMIT 41)" Mermaid flowchart. `b-tree-index-and-bitmap-heap.png` explains the same plan's mechanics in depth; this figure is the compact step view used in the investigation narrative.

**Evidence:** [recorded plan, section 3b](../results/eks-20261022-trap-no-stats.txt) and README section "The Catastrophic Path: LIMIT 41".

**Prompt:**

```text
Create a polished technical step-sequence diagram on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the content. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: compact cards, small colored icon tabs, thin outlines, pale fills, modest uniform corners, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A step names about 40 px, regular #475569 detail lines at least 30 px, small numbered step tags in monospace. SQL uses readable monospace.
Green #15803D on #F0FDF4 for the index scan; blue #2563EB on #EFF6FF for the heap scan; red #B91C1C on #FFF1F2 for the sort; slate on #F8FAFC for the limit and the query card. Small matching outline icons: tree, page grid, CPU, result rows.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. A compact query card spans the upper left. Below it, four equal step cards in one horizontal row with wide gutters, each with a step tag at the top-left, a name, and three detail lines. Beneath the row, one slim timing bar: a thin horizontal rectangle whose left 2% is green and the remainder red, with labels above its two parts. Leave a footer for one note and a compact legend.

EXACT TEXT LABELS — PRIVATE:
Query card, monospace, three lines: "WHERE metadata->>'book_name' = '00. New Spring'", "ORDER BY embedding <=> $1", "LIMIT 41".
Step 1 (green): tag "1", name "Bitmap Index Scan", details "B-tree on metadata->>'book_name'", "63,945 matching row IDs", "about 4 ms".
Step 2 (blue): tag "2", name "Bitmap Heap Scan", details "Fetch all 63,945 rows and their embeddings", "50,143 exact heap blocks", "305,980 buffer hits".
Step 3 (red): tag "3", name "Sort by cosine distance", details "Distance for every one of 63,945 rows", "1,536 dimensions each", "Brute force, no index".
Step 4 (slate): tag "4", name "Limit", details "Keep the nearest 41", "41 rows returned".
Timing bar left label: "index scan · ~4 ms". Timing bar right label: "heap scan + sort · ~350 ms".

VISUAL CONNECTIONS — PRIVATE:
Exactly four directed arrows, each with one small filled head and a short label on a white backing in the gutter. The arrow labels are only the quoted words; never print a line width, pixel value or percentage. A thin solid slate arrow from the query card down to step 1, label "chosen plan". A medium solid slate arrow from step 1 to step 2, label "row IDs". A medium solid slate arrow from step 2 to step 3, label "rows + embeddings". A medium solid red arrow from step 3 to step 4, label "sorted rows". No arrows to the timing bar. The timing bar carries only its two quoted labels; do not print its proportions.

CALLOUTS — PRIVATE:
Footer-left, one quiet line: "Total 358 ms, warm cache, second run · planner expected 12,500 matches".

LEGEND — PRIVATE:
Bottom-right, a small white outlined box with three swatches: green "Index lookup", blue "Heap access", red "Distance + sort". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
One query card, four step cards, four arrows, one timing bar. No pixel values or percentages are printed anywhere. Buffer hits are not described as unique pages. Only the recorded index-scan time and the combined heap-scan-plus-sort time appear; no invented per-step durations. No standalone title, no "catastrophic" wording, no rendering syntax.
```

---

## text-search-cte-steps.png

**What it shows:** the plan PostgreSQL executed for the hybrid query's text-search CTE with keyword `dragon` and `LIMIT 20`: GIN bitmap index scan (76,198 row IDs, about 14 ms), parallel bitmap heap scan across four workers plus the leader (about 15,240 rows and about 11,500 to 12,000 heap blocks per process), `ts_rank()` computed for every fetched row, a per-process sort of its scored rows, then Gather Merge, `ROW_NUMBER` and the limit. Total 596 ms. Replaces the "Text Search CTE Execution" Mermaid flowchart.

**Evidence:** [recorded hybrid component plans](../results/eks-20261022-hybrid-components.txt), README section "What the Text Search Plan Reveals", and [diagnose_hybrid_components.sql](../../scripts/diagnose_hybrid_components.sql).

**Prompt:**

```text
Create a polished technical step-sequence diagram on pure white, 3:2 landscape at 3072 × 2048 or higher. Begin directly with the content. No standalone title, subtitle or empty title band. Instructions are private; print only the quoted labels below, once each except explicit repetitions.

VISUAL STYLE — PRIVATE:
Restrained engineering-whitepaper styling: compact cards, small colored icon tabs, thin outlines, pale fills, modest uniform corners, no shadows, gradients or decoration. Inter-like sans-serif: semibold #0F172A step names about 40 px, regular #475569 detail lines at least 30 px, small numbered step tags in monospace. SQL uses readable monospace.
Green #15803D on #F0FDF4 for the GIN index scan; red #B91C1C on #FFF1F2 for the heap scan and the ranking step, which are the observed expensive steps; blue #2563EB on #EFF6FF for the per-process sort; slate on #F8FAFC for the query card and the merge step. Small matching outline icons: sorted list, page grid, CPU, ordered list, merged streams.

SPATIAL LAYOUT — PRIVATE:
Use 4% margins. A compact query card spans the upper left. Below it, five step cards in one horizontal row with wide gutters, each with a step tag, a name and three detail lines. Steps 2, 3 and 4 sit inside one faint dashed slate boundary with a small label, marking the parallel section; steps 1 and 5 sit outside it. Beneath the row, one slim timing bar: a thin horizontal rectangle whose left 3% is green and the remainder red, with labels above its two parts. Leave a footer for one note and a compact legend.

EXACT TEXT LABELS — PRIVATE:
Query card, monospace, two lines: "WHERE to_tsvector('english', content) @@ plainto_tsquery('english', 'dragon')", "ORDER BY ts_rank(...) DESC LIMIT 20".
Parallel boundary label: "4 workers + leader".
Step 1 (green): tag "1", name "GIN Bitmap Index Scan", details "Posting list for dragon", "76,198 matching row IDs", "about 14 ms".
Step 2 (red): tag "2", name "Parallel Bitmap Heap Scan", details "Fetch all 76,198 rows", "about 15,240 rows per process", "about 12,000 heap blocks per process".
Step 3 (red): tag "3", name "ts_rank() per row", details "Re-tokenize content at query time", "Score every fetched row", "Frequency is not in the index".
Step 4 (blue): tag "4", name "Sort per process", details "Order its scored rows by rank", "quicksort, under 1 MB each".
Step 5 (slate): tag "5", name "Gather Merge + ROW_NUMBER", details "Merge the sorted streams", "Return the top 20".
Timing bar left label: "index scan · ~14 ms". Timing bar right label: "heap scan + ranking + sort · ~580 ms".

VISUAL CONNECTIONS — PRIVATE:
Exactly five directed arrows, each with one small filled head and a short label on a white backing in the gutter. The arrow labels are only the quoted words; never print a line width, pixel value or percentage. A thin solid slate arrow from the query card down to step 1, label "plan". A medium solid slate arrow from step 1 to step 2, label "row IDs". A medium solid red arrow from step 2 to step 3, label "rows"; a medium solid red arrow from step 3 to step 4, label "scored rows"; a medium solid slate arrow from step 4 to step 5, label "sorted streams". No arrows to the timing bar or the boundary. The timing bar carries only its two quoted labels.

CALLOUTS — PRIVATE:
Footer-left, one quiet line: "Total 596 ms, warm cache · the vector CTE of the same hybrid query took about 1 ms".

LEGEND — PRIVATE:
Bottom-right, a small white outlined box with three swatches: green "Index lookup", red "Observed expensive steps", blue "Sort". No heading.

FINAL CHECK — PRIVATE, DO NOT PRINT:
One query card, five step cards, one dashed parallel boundary around steps 2 to 4, five arrows, one timing bar. Per-process figures are labelled as approximate. Only the recorded index-scan time and the combined remainder appear. No standalone title, no rendering syntax.
```

---

## li-architecture.png (LinkedIn, 3:4)

**What it shows:** a compact logical overview of the same deployment, with the staging/cache prerequisite explicit and SQL, management and scraping distinguished. It deliberately omits node boxes, resource allocations and namespaces to preserve readability at phone width. Save to `doc/linkedin/diagrams/li-architecture.png`.

**Evidence:** the manifests, deployment helper and recorded run environment linked in the main architecture section above.

**Prompt:**

```text
Create a compact professional deployment diagram for a phone-viewed technical carousel. Pure white canvas, 4:5 portrait at 1600 × 2000 or higher. No standalone title, headline, subtitle or empty title band. Begin with the component cards. Instructions are private; print only the quoted labels below, allowing only explicit repetitions.

VISUAL STYLE — PRIVATE:
Match the detailed architecture figure with flat compact cards, modest identical corners, fine outlines, pale fills and small colored icon tabs. No shadows, gradients, thick bottom edges, perspective or ornamental texture. Use Inter-like sans-serif: semibold #0F172A names around 52 px, regular #475569 captions and connector labels at least 42 px. Small footer and legend labels are at least 38 px. Left-align text. Wrap only at the specified label breaks; shorten empty card space rather than shrinking typography.
Use indigo #4F46E5 on #EEF2FF for Jobs, green #15803D on #F0FDF4 for both databases, teal #0F766E on #F0FDFA for monitoring, blue #2563EB on #EFF6FF for S3/ECR, and slate #475569 on #F8FAFC for the operator and file cache. Use one consistent 48 px outline-icon family: bucket, package, files, gear, play symbol, matching database cylinders and chart. No brand logos or decorative symbols.

SPATIAL LAYOUT — PRIVATE:
Keep 5% margins. At the top, two equal compact cards side by side: S3 left and ECR right. Beneath them, one thin EKS boundary contains two equal columns separated by a wide routing gutter. A small boundary caption sits at the upper-left inside edge.
Inside the boundary, three rows of paired cards: first row staging/cache left and operator right; second row Benchmark Jobs left and Vector database right; third row Results database left and monitoring right. Give the middle row the clearest horizontal connection. Leave usable space between all rows for labels. No node-group or namespace frames. Keep the bottom area for a small factual note and compact legend.

EXACT TEXT LABELS — PRIVATE:
Top left: "Amazon S3", with "Parquet datasets".
Top right: "Amazon ECR", with "Custom images".
Boundary label: "Amazon EKS · ap-south-1".
First row left: "Stage + cache", with "SHA-256 verified", "EBS-backed files".
First row right: "CNPG operator", with "1.30.1".
Second row left: "Benchmark Jobs", with "COPY · index · ANALYZE", "Vector · filtered · hybrid".
Second row right: "Vector database", with "PostgreSQL 18.6", "pgvector 0.8.7".
Third row left: "Results database", with "Latency · QPS".
Third row right: "Prometheus + Grafana", with "Database metrics". Split the name over two lines if needed at the plus sign, without reducing its type size.

VISUAL CONNECTIONS — PRIVATE:
Exactly eight directed arrows, one small filled head each. Use solid indigo 4 px for SQL and stored results, dashed blue #0369A1 3 px for files/images/scrapes, and dotted slate #64748B 3 px for logical management. All paths use short horizontal and vertical segments outside cards. Connector labels remain large and sit in clear white space. Routes may bend in the central gutter; do not run them across text. If two routes cross, interrupt the lighter line with a small white gap and no junction dot.
1. S3 downward to Stage + cache, dashed blue, label "stage files".
2. ECR downward to the EKS boundary top edge, dashed blue, label "workload images". End on the outer boundary, not on the operator.
3. Stage + cache downward to Benchmark Jobs, dashed blue, label "read Parquet".
4. Benchmark Jobs rightward to Vector database, solid indigo, label "SQL queries".
5. Benchmark Jobs downward to Results database, solid indigo, label "write results".
6. CNPG operator downward to Vector database, dotted slate, label "CNPG management".
7. CNPG operator to Results database through a separate route in the center gutter, dotted slate, repeat label "CNPG management".
8. Prometheus + Grafana upward to Vector database, dashed blue, label "scrape metrics".
No arrow between the databases and no separate Grafana-to-Prometheus arrow inside the combined monitoring card.

CALLOUTS — PRIVATE:
Below the boundary at left, two quiet lines: "Three node groups", "Two databases · one instance each".

LEGEND — PRIVATE:
Below the boundary at right, one compact white box with a fine grey outline and three small swatches: indigo "Jobs", green "Databases", teal "Monitoring". No heading. Arrow labels carry the relationship meanings.

FINAL CHECK — PRIVATE, DO NOT PRINT:
Two external cards, six internal cards, one boundary, eight arrows. Staging/cache is visible. ECR ends at the cluster boundary. Both databases share the same cylinder icon. Management and SQL have different line styles. All labels remain readable at phone width. No node specifications, storage quantities, namespace names, title, extra services or rendering syntax.
```

---

## li-planner-switch.png (LinkedIn, 3:4)

**What it shows:** the same `LIMIT 41` query and session settings before and after expression statistics are available. The diagnostic used `ef_search=40` and iterative scans off. Its plans returned 41 rows via bitmap-and-sort before `ANALYZE`, but only 18 via HNSW afterward. This makes the distinction between plan selection and completeness visible; the earlier “stops after 41 matches” caption did not match this evidence. Save to `doc/linkedin/diagrams/li-planner-switch.png`.

**Evidence:** section 3b in the [no-statistics plan](../results/eks-20261022-trap-no-stats.txt) and [post-ANALYZE plan](../results/eks-20261022-fixed-stats.txt). The full benchmark's strict-order iterative setting is different from this diagnostic.

**Prompt:**

```text
Create a precise before-and-after technical comparison for a phone-viewed carousel. Pure white canvas, 4:5 portrait at 1600 × 2000 or higher. No standalone title, headline, subtitle or empty title band. Begin directly with a query-context card. Instructions are private; print only the quoted labels below, with explicit repetitions where specified.

VISUAL STYLE — PRIVATE:
Match the architecture series: compact flat cards, small colored icon tabs, fine borders, pale fills, modest uniform corners and generous alignment. No shadows, gradients, heavy bottom edges, perspective or ornament. Inter-like sans-serif, semibold #0F172A names around 52 px, regular #475569 captions at least 42 px. Settings use readable monospace. Footer and legend text are at least 38 px. Left-align text. Use matching sizes for equivalent rows in both columns; numbers are emphasis, not huge decorative graphics.
Use slate on #F8FAFC for the shared query card; amber #B45309 on #FFFBEB for both estimate cards; red #B91C1C on #FFF1F2 for the observed bitmap plan; indigo #4F46E5 on #EEF2FF for HNSW. Result cards have neutral slate outlines, with the shortfall value in red. Use small matching outline icons for query document, estimate chart, page stack, network graph and result rows. No success check mark on the HNSW card.

SPATIAL LAYOUT — PRIVATE:
Use 5% margins. A wide compact query-context card sits at the top. Beneath it are two equal columns with a generous middle gap and small local column labels. Each column has three stacked cards: estimate, chosen plan and returned-row count. Corresponding rows and text baselines align exactly. Avoid enclosing each column in another heavy frame.
Reserve the lower area for two short explanatory notes and one compact legend. Card heights follow their text, leaving roomy gaps for vertical arrow labels. Keep the comparison readable at phone width.

EXACT TEXT LABELS — PRIVATE:
Shared query-context card, three lines: "00. New Spring · LIMIT 41", "ef_search = 40", "iterative_scan = off".
Left column label: "Before ANALYZE".
Right column label: "After ANALYZE".
Left estimate card: "Estimated matches", with "12,500 rows" and "Default estimate".
Right estimate card: repeat "Estimated matches", with "62,172 rows" and "Sampled estimate".
Left plan card: "Bitmap scan + sort", with "63,945 matching rows", "Distance for every match".
Right plan card: "HNSW index scan", with "Approximate search", "Metadata post-filter".
Left result card: "41 rows returned", with "41 requested".
Right result card: "18 rows returned", with repeat "41 requested". Only the returned-row value in this card is red; do not make the entire HNSW column a failure color.

VISUAL CONNECTIONS — PRIVATE:
Exactly six directed arrows, one small filled destination head each. Use solid slate #64748B 3 px for the two upper links, red 4 px within the left column and indigo 4 px within the right. Keep every label readable on white space between cards.
1. Shared query card down-left to the left estimate card, slate, label "missing statistics".
2. Shared query card down-right to the right estimate card, slate, label "sampled statistics".
3. Left estimate card down to the left plan card, red, label "chosen plan".
4. Right estimate card down to the right plan card, indigo, repeat label "chosen plan".
5. Left plan card down to the left result card, red, label "returned rows".
6. Right plan card down to the right result card, indigo, repeat label "returned rows".
No arrow between columns. No arrow implies that ANALYZE itself enables iterative scanning.

CALLOUTS — PRIVATE:
Below the cards, two quiet lines: "Same query and settings; statistics changed.", "Iterative scans address the remaining row shortfall.". Use no large colored banner.

LEGEND — PRIVATE:
Bottom-right, a small white outlined box with three swatches: amber "Estimate", red "Bitmap plan", indigo "HNSW plan". No heading. Keep it clear of the explanatory notes.

FINAL CHECK — PRIVATE, DO NOT PRINT:
One shared query card, six comparison cards, six arrows. LIMIT is 41 on both sides. Estimates are 12,500 and 62,172; returned counts are 41 and 18. Iterative scanning is off throughout this recorded diagnostic. HNSW is not labelled exact or guaranteed to return 41. No latency claims, title, large decorative numerals, invented settings or rendering syntax.
```