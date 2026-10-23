# LinkedIn carousel: brief and page content

This file is the complete brief for the LinkedIn post images: how to produce them, the style, and the exact text, numbers, diagrams and screenshots on every page. Every fact comes from the README, the manifests and the recorded EKS results in `doc/results/`. The design tool adds nothing.

The images are the first of three layers. They give a ten-page tour of the whole piece of work, from the cluster to the production checklist, that takes five to ten minutes with the post text. The [article](article.md) is the second layer, with the theory, every query plan and every workaround. The repository is the third: setup guide, manifests, benchmark code and raw results. The last page says so.

The story the pages tell, in order: what was built (a production-style pgvector deployment on one EKS cluster), how it was measured (three kinds of search across concurrency and result size, run twice), what it delivered, the three performance traps the benchmark exposed, which of them have fixes, and what to check before shipping your own. The filtered-search cliff is the largest single number in the work and gets three pages, but it is one finding among three, not the headline.

Repository: `github.com/tharindu-sankalpa/pgvector-filtering-investigation`

## How to produce it

1. **Generate the two diagrams** from [diagrams/prompts.md](diagrams/prompts.md) (`li-architecture.png`, `li-planner-switch.png`, 3:4 portrait) and save them under `doc/linkedin/diagrams/`. Both exist at 1792 × 2400. Review each against its prompt's FINAL CHECK before use.
2. **Capture and crop the screenshots** listed in [SCREENSHOT_PLAN.md](SCREENSHOT_PLAN.md), section D, into `doc/screenshots/crops/`, and view every crop. This brief uses `02-eks-node-groups.png` and `d03-trap-limit-41-bitmap.png`.
3. **Give Claude Design this file** plus the two diagrams and the two crops. Say: "Read this file in full and follow it exactly; add nothing that is not in it; place the images unaltered; run its closing checklist and report each item."
4. **Export a PDF** of ten pages, each **1080 × 1440 px (3:4 portrait)**, no bleed. Name it `pgvector-production-traps.pdf`. (`doc/pgvector-filtered-search-trap.pdf` is the export of the earlier seven-page brief and is superseded.)
5. **Convert the PDF to ten PNGs** at exactly 1080 × 1440:

   ```bash
   brew install poppler
   pdftoppm -png -scale-to-x 1080 -scale-to-y 1440 \
     pgvector-production-traps.pdf doc/linkedin/page
   ```

   Check that each `page-N.png` is 1080 × 1440 with `sips -g pixelWidth -g pixelHeight`.
6. **Post** the ten PNGs, in order, as one multi-image post, with the text from [linkedin-post.md](linkedin-post.md). The article and repository links go in the post text, and pages 1 and 10 also write the repository URL out in plain text.

## Overrides of the `linkedin-carousel` skill

This post differs from the skill's default in three ways. Keep these choices in later revisions:

- **Multi-image post, not a document post.** The PDF is only an intermediate file for producing the images.
- **3:4 portrait (1080 × 1440), ten pages.** The skill's 4:5 / A4 and twelve to sixteen pages are for document posts. Ten pages keep one idea per image while giving the system, the method and the results a page each, so the set tells the whole story rather than only the trap.
- **Two diagrams come from Nano Banana Pro, not from the design tool.** They are generated from reviewed prompts at 3:4 and placed unaltered. The design tool draws only cards, tables, stat tiles and text. The cover has no illustration.

## What earlier versions got wrong

- A first set of seven images was generated as whole pages by an image model. None of them stated the finding, there was no evidence from the live system, and type sizes and spacing varied across the set. The design tool now draws every page, and pages 3 and 6 carry real AWS console and DBeaver crops.
- A first brief described the filtered-search cliff as a planner flaw without a fix. The investigation found the cause (missing statistics for the filter expression) and the fix (`ANALYZE`); the pages follow that story.
- The seven-page brief put the `ANALYZE` finding on the cover as the whole story. The work is a production-style deployment and a benchmark across concurrency, result size and three search patterns, which exposed three traps; one `ANALYZE` number on the cover undersold the rest and overstated one finding. The cover now states what was built and measured, and names the three traps and which have fixes.
- Concurrency is stated as what was measured, 1 to 100 concurrent clients. The benchmark did not test thousands of concurrent users and no page claims it.

---

## Brief for the design tool

Design ten pages for a LinkedIn multi-image post. Each page is **1080 × 1440 px, 3:4 portrait**, exported together as one PDF. The pages are read in the LinkedIn app at phone width, so everything is large: body text at least 30 px, captions and table text at least 24 px, code at least 24 px. Each page is one slide of a presentation: one idea, read in about a minute. Use only the text given below; do not add, reword, shorten or invent any sentence, number, file name or SQL line. Place the given diagrams and screenshot crops unaltered. No stock imagery, photographs or other illustrations. If a page does not fit, do not shrink type below the minimums; report which page overflowed.

### Style

- **Canvas** pure white #FFFFFF, so the white-background diagrams sit on the page without a visible box. Text #0F172A, secondary text #475569. A 6 px rule in indigo #4F46E5 across the top of every page. Outer padding 64 px on all sides.
- **Three fixed elements on every page:**
  - A small-caps kicker in indigo #4F46E5 at the top left, letter-spaced, 22 px.
  - A bold headline below it, 60 px, at most two lines (one line on pages 2 and 7), with one phrase in an accent colour.
  - A page number "n/10" at the bottom right in secondary text, 24 px.
- **Typeface:** Inter (or a similar clean sans-serif) for everything except SQL; JetBrains Mono for SQL and setting names.
- **Accent colours** only on small marks, left accent bars, the highlighted headline phrase and table status cells:
  - indigo #4F46E5: HNSW, the fast path, the system
  - red #B91C1C: the trap
  - green #15803D: the fix, "works"
  - amber #B45309: partial, band-aid, open problem

  Fills are very pale tints (#EEF2FF, #FFF1F2, #F0FDF4, #FFFBEB). Never a saturated block.
- **Cards and stat tiles:** white fill, solid 3 px outline #334155, 12 px radius, 6 px coloured left accent bar, number or name in bold. They must read as solid boxes at phone size, not hairline frames.
- **Diagrams** (pages 2 and 7): the PNG from `doc/linkedin/diagrams/` placed unaltered, centred, as large as the space between headline and page number allows, without a frame, without cropping or recolouring, and with no text added on or under it.
- **Screenshots** (pages 3 and 6): the crops from `doc/screenshots/crops/` placed unaltered at full content width (952 px) inside a card with the 3 px outline and 12 px radius. A caption of at most two lines sits beneath each one, in secondary text, 24 px. Screenshot text may be smaller than body text; the caption carries the facts.
- **Code card** (page 8): dark card #1E293B, light text, header line naming what it is, no line numbers.
- **Technology badges** on page 1 only: text badges (name and version in a 3 px outlined pill), no logos.
- Plenty of empty space.

### Page 1 · cover

Kicker: `PGVECTOR · CLOUDNATIVEPG · AMAZON EKS`

Headline: `pgvector in production: three traps found at scale` (highlight "three traps" in red)

Standfirst, secondary text, 32 px: `2.5 million vectors on one EKS cluster, three kinds of search, 1 to 100 concurrent clients, 629,712 queries. What worked, what broke, and what fixed it.`

Four numbered lines, numerals 48 px in indigo, text 30 px:

1. **One EKS cluster runs everything.** CloudNativePG runs PostgreSQL 18.6 with pgvector 0.8.7 on a dedicated 16 vCPU node; benchmark Jobs, a results database and Prometheus run beside it.
2. **144 scenarios, run twice.** Vector, filtered and hybrid search × 8 concurrency levels × 6 result sizes, once as an ordinary load leaves the database and once after `ANALYZE`.
3. **Three traps.** Filtered search fell off a cliff at LIMIT 41 (1.5 ms → 358 ms), filtered results came back incomplete (18 of 40 rows), and hybrid search ran about 3× slower than vector search.
4. **Two have fixes; one remains.** `ANALYZE` after indexing removed the cliff and iterative scan completed the results. Hybrid search stays slow because PostgreSQL's full-text index cannot rank.

A row of four technology badges: `Amazon EKS 1.36` · `CloudNativePG 1.30.1` · `PostgreSQL 18.6` · `pgvector 0.8.7`

Repository line, monospace, 26 px: `github.com/tharindu-sankalpa/pgvector-filtering-investigation`

Footer, indigo italic, 26 px: `A ten-image tour. The article has the full investigation. Swipe →`

### Page 2 · the system

Kicker: `THE SYSTEM`

Headline (one line): `One EKS cluster runs everything` (highlight "One EKS cluster" in indigo)

Diagram: `doc/linkedin/diagrams/li-architecture.png`, unaltered, filling the space below the headline. No caption.

### Page 3 · the AWS setup

Kicker: `THE SETUP · ON AWS`

Headline: `Three node groups, one job each` (highlight "one job each" in indigo)

Three cards, stacked, bold name then two lines at 28 px:

- **Vector database · m6i.4xlarge** (indigo accent) — 16 vCPU, 64 GiB. PostgreSQL 18.6 + pgvector 0.8.7, run by CloudNativePG 1.30.1. 200 GiB EBS gp3 at 6,000 IOPS. `shared_buffers=4GB`, `work_mem=256MB`.
- **Benchmark Jobs · m6i.2xlarge** (indigo accent) — 8 vCPU, 32 GiB. Parquet datasets staged from S3 to EBS and checksummed before the timed run.
- **Operator, results, monitoring · m6i.xlarge** (indigo accent) — 4 vCPU, 16 GiB. CNPG operator, results database, Prometheus and Grafana.

Screenshot card: `doc/screenshots/crops/02-eks-node-groups.png`.
Caption: `Amazon EKS console, cluster pgvector-investigation in ap-south-1: three node groups, one per role.`
`Database traffic stays inside the cluster; administrative access is kubectl port-forward.`

### Page 4 · the method

Kicker: `THE METHOD`

Headline: `Same benchmark, run twice` (highlight "run twice" in indigo)

Four bullets, bold lead then one or two sentences, 28 px:

- **2.5 million vectors.** 1,536-dimension embeddings loaded by binary `COPY` in 19 minutes; the HNSW index (`m = 16`, `ef_construction = 64`) took 96 minutes to build.
- **Three kinds of search.** Vector: nearest neighbours. Filtered: nearest neighbours within one book, a JSONB field with 16 values, each 2.6% to 8.4% of the rows. Hybrid: vector plus keyword search, fused with RRF.
- **144 scenarios.** 8 concurrency levels (1 to 100 clients) × 6 result sizes (top_k 1 to 100), 4,373 real queries each.
- **Two database states.** Once as an ordinary load leaves it (no `ANALYZE` after the indexes), once after `ANALYZE`.

A card, full content width, monospace 26 px, indigo accent:
`3 patterns × 8 concurrency levels × 6 top_k values = 144 scenarios`
`144 scenarios × 4,373 queries = 629,712 queries per run`

### Page 5 · what it delivered

Kicker: `THE RESULT · AFTER THE FIX`

Headline: `What one 16-vCPU node delivered` (highlight "one 16-vCPU node" in indigo)

One line above the tiles, secondary text, 28 px: `32 concurrent clients, top_k 10, after ANALYZE.`

A column of three stat tiles, equal size, indigo accent:

- number `3,166 QPS`, label `Vector search`, sub-label `p95 latency 8.5 ms`
- number `1,700 QPS`, label `Filtered search`, sub-label `p95 latency 26.6 ms`
- number `471 QPS`, label `Hybrid search`, sub-label `p95 latency 42.0 ms`

Two lines under the tiles, secondary text, 28 px:
`At 100 clients, p95 latency was 36.6 ms for vector, 101.1 ms for filtered and 137.7 ms for hybrid search. All 629,712 queries completed.`
`The first run, without ANALYZE, did not look like this.`

### Page 6 · trap 1, the cliff

Kicker: `TRAP 1 · THE CLIFF`

Headline: `Ask for one more row, lose the index` (highlight "lose the index" in red)

A row of two stat tiles, equal width:

- Left tile, indigo accent: number `1.5 ms`, label `LIMIT 40 · HNSW index`, sub-label `same query, same filter`
- Right tile, red accent: number `358 ms`, label `LIMIT 41 · brute force`, sub-label `distance to all 63,945 rows`

One line under the tiles, secondary text: `Without statistics, filtered search at 50 clients and top_k 50 averaged 7,999 ms, and 85% of the top_k 50 and 100 queries timed out.`

Screenshot card: `doc/screenshots/crops/d03-trap-limit-41-bitmap.png`.
Caption: `EXPLAIN ANALYZE in DBeaver at LIMIT 41: the planner expects rows=12500,`
`the Bitmap Heap Scan reads 63,945, then sorts every distance.`

### Page 7 · trap 1, the root cause

Kicker: `TRAP 1 · THE ROOT CAUSE`

Headline (one line): `The planner was guessing` (highlight "guessing" in red)

Diagram: `doc/linkedin/diagrams/li-planner-switch.png`, unaltered, filling the space below the headline. No caption.

### Page 8 · trap 1, the fix

Kicker: `TRAP 1 · THE FIX`

Headline: `Statistics after indexes, not before` (highlight "after indexes" in green)

Code card, header `the fix`:

```sql
-- last step of every bulk load or reindex
ANALYZE wot_chunks_2_5m;
```

A table, full content width, 24 px, header row bold, thin #CBD5E1 row rules, the last column a coloured status pill (red "fails", amber "partial", green "works"):

| What I tried | Uses HNSW? | Status |
|---|---|---|
| `enable_bitmapscan = off` | No | partial |
| `hnsw.ef_search = 200` | No | fails |
| `hnsw.iterative_scan` | No | fails |
| CTE over-fetch + iterative scan | Yes | partial |
| `ANALYZE` | Yes | works |

One line under the table, secondary text: `After ANALYZE, 50 clients at top_k 50 went from 7,999 ms and 954 completed queries to 90 ms and all 4,373, and the planner kept HNSW at every LIMIT up to 200.`

### Page 9 · traps 2 and 3

Kicker: `TRAPS 2 AND 3 · WHAT REMAINS`

Headline: `Two traps survive the fix` (highlight "survive the fix" in amber)

Two cards, stacked, bold title then two or three lines at 28 px, then a status pill on the right of the title:

- **Trap 2 · incomplete filtered results** (green pill "fixed by a setting") — The HNSW index does not know about the filter. With default settings it collected 40 candidates, discarded the 22 from other books and returned 18 rows for LIMIT 40, with no error. Fix: `SET hnsw.iterative_scan = relaxed_order` (or `strict_order`).
- **Trap 3 · hybrid search about 3× slower** (amber pill "no full fix") — The GIN full-text index finds every row containing a keyword but cannot rank them, so `ts_rank()` fetches and scores every match: 76,198 rows for 'dragon', 596 ms for the text half alone. Mitigations only: a stored `tsvector` column or the `rum` extension.

One line under the cards, secondary text, 28 px: `Neither is a bug. PostgreSQL's planner works from estimates and its indexes were built for exact matching.`

### Page 10 · before you ship, and where to read more

Kicker: `BEFORE YOU SHIP · WHERE NEXT`

Headline: `Five checks for your own pgvector` (highlight "Five checks" in green)

Five numbered lines, numerals 40 px in green, text 28 px:

1. **Run `ANALYZE` after every index build and bulk load**, before the table serves traffic.
2. **Check `pg_stats` for every filtered expression index.** An empty result means the planner is guessing.
3. **Compare `rows=` with `actual rows=`** in `EXPLAIN ANALYZE` at your production top_k values.
4. **Enable `hnsw.iterative_scan`** for filtered queries, or they can quietly return fewer rows than requested.
5. **Do not raise `hnsw.ef_search` to fix a plan switch.** Without statistics it moves the cliff to a lower top_k.

Then three rows under the heading `Read more`, each a card with a bold label and plain text, 26 px:

- **This post** — the system, the benchmark and the three traps in ten images.
- **The article** — HNSW, B-tree and GIN internals, every EXPLAIN plan, why the usual workarounds fail. Link in the post text.
- **The repository** — EKS setup guide, CNPG manifests, benchmark Jobs, raw results. `github.com/tharindu-sankalpa/pgvector-filtering-investigation`

---

## Sources for every number

| Number | Source |
|---|---|
| 1.5 ms / 358 ms / 63,945 / rows=12500 | `doc/results/eks-20261022-trap-no-stats.txt`, sections 3a and 3b |
| 7,999 ms and 954 completed at C:50, top_k 50; 85% timed out; 90 ms after ANALYZE | README, "The Problem" and "The Fix" sections; `doc/results/eks-20261022-retrieval-no-stats.csv` and `-with-stats.csv` |
| 3,166 / 1,700 / 471 QPS and p95 8.5 / 26.6 / 42.0 ms at C:32, top_k 10 | `doc/results/eks-20261022-retrieval-with-stats.csv` |
| p95 36.6 / 101.1 / 137.7 ms at C:100, top_k 10 | `doc/results/eks-20261022-retrieval-with-stats.csv` |
| 629,712 queries completed; 144 scenarios; 4,373 queries each | `benchmark_study_runs` evidence for run `20261022T045130Z`; README, "The Benchmark" |
| 19 minutes insert; 96 minutes HNSW build; m = 16, ef_construction = 64 | README, "The Benchmark"; `doc/RUN_EVIDENCE.md` |
| Node sizes, EBS 200 GiB gp3 at 6,000 IOPS, PostgreSQL settings, versions | README, "Infrastructure Architecture"; `infrastructure/aws/eks-cluster.yaml`; `infrastructure/cnpg-pgvector/cluster.yaml` |
| 18 of 40 rows; 22 discarded | `doc/results/eks-20261022-trap-no-stats.txt`, section 4a |
| HNSW at every LIMIT up to 200 | `doc/results/eks-20261022-fixed-stats.txt`, section 2 |
| about 3× slower hybrid; 76,198 rows for 'dragon'; 596 ms text CTE | `benchmark_retrieval_summary`, C:1 top_k 10: 8.45 ms hybrid vs 2.80 ms vector; `doc/results/eks-20261022-hybrid-components.txt` |
| Workaround statuses | README, "Why the Usual Workarounds Don't Help"; full detail in `doc/workarounds.md` |
| Five checks | README, "Recommendations" |

## Checklist before posting

- [ ] Ten PNGs, each exactly 1080 × 1440.
- [ ] Every number on pages 1, 3, 4, 5, 6, 8 and 9 matches the article and the sources table above.
- [ ] Page 2 diagram: eight cards, seven arrows, one boundary, no load balancer.
- [ ] Page 7 diagram: one query card, two estimate cards, two plan cards, four arrows, no latency numbers.
- [ ] Both screenshot crops keep the console or DBeaver header and contain no account ID, public IP, email or password.
- [ ] Kicker, headline and page number "n/10" present on all ten pages; no page overflowed.
- [ ] Concurrency is stated as 1 to 100 clients everywhere; no page claims thousands of users.
- [ ] The repository URL is written out on pages 1 and 10; the post text has both links.
- [ ] No mention of any other cloud provider or of an earlier environment anywhere.
