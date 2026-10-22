# Diagram prompts (Nano Banana Pro)

Prompts for the diagrams that change for the EKS environment. These use the `diagram` skill's visual-only approach: only quoted text is printed, everything else is private direction.

**Project preference: no standalone titles inside diagram images.** Start directly with the diagram content; do not reserve an empty title band. Component names, boundary labels and section labels remain. The surrounding article or README supplies the figure heading. This explicitly overrides the skill's default requirement for a large title.

Output formats:

| Set | Files | Aspect ratio | Why |
|---|---|---|---|
| Article and README prompts in this file | Six `doc/diagrams/*.png` figures below | **3:2 landscape**, at least 3072 × 2048 | Gives technical labels and connection routes enough width, with a consistent appearance across the rewritten set. |
| LinkedIn carousel | `doc/linkedin/diagrams/*.png` | **4:5 portrait**, at least 1600 × 2000 | Larger type and fewer details for phone viewing; fits the carousel's image slot. |

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
| `HNSW-Post-Filtering.png`, `iterative_scan.png`, `ef_search.png`, `ef_construction.png`, `m-Parameter.png`, `book-filter-distribution.png`, `hnsw-graph.html` | Keep. Platform-independent mechanisms and dataset facts. |
| `gin-index.png`, `search-patterns.png` | Keep. Optional later rewrite with invented sample sentences instead of corpus text. |
| `doc/plots/*.png` | Not image-model work. Regenerate with `scripts/plot_article_benchmarks.py` from the EKS results. |
| `doc/screenshots/**` | Not image-model work. Capture from the live system per [SCREENSHOT_PLAN.md](../SCREENSHOT_PLAN.md). |
| `doc/linkedin/diagrams/li-architecture.png` | **Generated 22 Oct, accepted.** Carousel page 2. |
| `doc/linkedin/diagrams/li-planner-switch.png` | **Generated 22 Oct, accepted.** Carousel page 5. |

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

## li-architecture.png (LinkedIn, 4:5)

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

## li-planner-switch.png (LinkedIn, 4:5)

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
