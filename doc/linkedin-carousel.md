# LinkedIn carousel: brief and page content

This file is the complete brief for the LinkedIn post images: how to produce them, the style, and the exact text, numbers, diagrams and screenshots on every page. Every fact comes from the README, the manifests and the recorded EKS results in `doc/results/`. The design tool adds nothing.

The images are the first of three layers. They give a seven-page tour of the investigation that takes ten to fifteen minutes with the post text. The [article](article.md) is the second layer, with the theory, every query plan and every workaround. The repository is the third: setup guide, manifests, benchmark code and raw results. The last page says so.

Repository: `<REPO_URL>`

## How to produce it

1. **Generate the two diagrams** from [diagrams/prompts.md](diagrams/prompts.md) (`li-architecture.png`, `li-planner-switch.png`, 3:4 portrait) and save them under `doc/linkedin/diagrams/`. Review each against its prompt's FINAL CHECK before use.
2. **Capture and crop the three screenshots** listed in [SCREENSHOT_PLAN.md](SCREENSHOT_PLAN.md), section D, into `doc/screenshots/crops/`, and view every crop.
3. **Give Claude Design this file** plus the two diagrams and the three crops. Say: "Read this file in full and follow it exactly; add nothing that is not in it; place the images unaltered; run its closing checklist and report each item."
4. **Export a PDF** of seven pages, each **1080 × 1440 px (3:4 portrait)**, no bleed. Name it `pgvector-filtered-search-trap.pdf`.
5. **Convert the PDF to seven PNGs** at exactly 1080 × 1440:

   ```bash
   brew install poppler
   pdftoppm -png -scale-to-x 1080 -scale-to-y 1440 \
     pgvector-filtered-search-trap.pdf doc/linkedin/page
   ```

   Check that each `page-N.png` is 1080 × 1440 with `sips -g pixelWidth -g pixelHeight`.
6. **Post** the seven PNGs, in order, as one multi-image post, with the text from [linkedin-post.md](linkedin-post.md). The article and repository links go in the post text, and pages 1 and 7 also write the repository URL out in plain text.

## Overrides of the `linkedin-carousel` skill

This post differs from the skill's default in three ways. Keep these choices in later revisions:

- **Multi-image post, not a document post.** The PDF is only an intermediate file for producing the images.
- **3:4 portrait (1080 × 1440), seven pages.** The skill's 4:5 / A4 and twelve to sixteen pages are for document posts. Seven pages keep the tour to one idea per image.
- **Two diagrams come from Nano Banana Pro, not from the design tool.** They are generated from reviewed prompts at 3:4 and placed unaltered. The design tool draws only cards, tables, stat tiles and text. The cover has no illustration.

## What earlier versions got wrong

- A first set of seven images was generated as whole pages by an image model. None of them stated the finding, there was no evidence from the live system, and type sizes and spacing varied across the set. The design tool now draws every page, the cover states the result with its numbers, and pages 3 and 4 carry real AWS console and DBeaver crops.
- A first brief described the filtered-search cliff as a planner flaw without a fix. The investigation found the cause (missing statistics for the filter expression) and the fix (`ANALYZE`); the pages now follow that story.

---

## Brief for the design tool

Design seven pages for a LinkedIn multi-image post. Each page is **1080 × 1440 px, 3:4 portrait**, exported together as one PDF. The pages are read in the LinkedIn app at phone width, so everything is large: body text at least 30 px, captions and table text at least 24 px, code at least 24 px. Each page is one slide of a presentation: one idea, read in about a minute. Use only the text given below; do not add, reword, shorten or invent any sentence, number, file name or SQL line. Place the given diagrams and screenshot crops unaltered. No stock imagery, photographs or other illustrations. If a page does not fit, do not shrink type below the minimums; report which page overflowed.

### Style

- **Canvas** pure white #FFFFFF, so the white-background diagrams sit on the page without a visible box. Text #0F172A, secondary text #475569. A 6 px rule in indigo #4F46E5 across the top of every page. Outer padding 64 px on all sides.
- **Three fixed elements on every page:**
  - A small-caps kicker in indigo #4F46E5 at the top left, letter-spaced, 22 px.
  - A bold headline below it, 60 px, at most two lines (one line on pages 2 and 5), with one phrase in an accent colour.
  - A page number "n/7" at the bottom right in secondary text, 24 px.
- **Typeface:** Inter (or a similar clean sans-serif) for everything except SQL; JetBrains Mono for SQL and setting names.
- **Accent colours** only on small marks, left accent bars, the highlighted headline phrase and table status cells:
  - indigo #4F46E5: HNSW, the fast path
  - red #B91C1C: the trap
  - green #15803D: the fix, "works"
  - amber #B45309: partial, band-aid
  
  Fills are very pale tints (#EEF2FF, #FFF1F2, #F0FDF4, #FFFBEB). Never a saturated block.
- **Cards and stat tiles:** white fill, solid 3 px outline #334155, 12 px radius, 6 px coloured left accent bar, number or name in bold. They must read as solid boxes at phone size, not hairline frames.
- **Diagrams** (pages 2 and 5): the PNG from `doc/linkedin/diagrams/` placed unaltered, centred, as large as the space between headline and page number allows, without a frame, without cropping or recolouring, and with no text added on or under it.
- **Screenshots** (pages 3 and 4): the crops from `doc/screenshots/crops/` placed unaltered at full content width (952 px) inside a card with the 3 px outline and 12 px radius. A caption of at most two lines sits beneath each one, in secondary text, 24 px. Screenshot text may be smaller than body text; the caption carries the facts.
- **Code card** (page 6): dark card #1E293B, light text, header line naming what it is, no line numbers.
- **Technology badges** on page 1 only: text badges (name and version in a 3 px outlined pill), no logos.
- Plenty of empty space.

### Page 1 · cover

Kicker: `PGVECTOR · CLOUDNATIVEPG · AMAZON EKS`

Headline: `One missing ANALYZE made filtered vector search 233× slower` (highlight "233× slower" in red)

Standfirst, secondary text, 32 px: `PostgreSQL as a production vector database: 2.5 million vectors, three kinds of search, and the traps that only show up at scale.`

Four numbered lines, numerals 48 px in indigo, text 30 px:

1. **One EKS cluster runs everything.** CloudNativePG runs the vector database and a separate results database; benchmark Jobs and Prometheus run beside them.
2. **LIMIT 40 → 41: 1.5 ms → 358 ms.** Same query, same filter. PostgreSQL dropped the vector index and computed the distance to every row of the book.
3. **The planner was guessing.** With no statistics for the filter, it assumed 12,500 matching rows. The real number was 63,945.
4. **`ANALYZE` fixed it.** After it, all 629,712 benchmark queries completed. Two traps remain: incomplete filtered results and slow hybrid search.

A row of four technology badges: `Amazon EKS 1.36` · `CloudNativePG 1.30.1` · `PostgreSQL 18.6` · `pgvector 0.8.7`

Repository line, monospace, 26 px: `<REPO_URL>`

Footer, indigo italic, 26 px: `A seven-image tour. The article has the full investigation. Swipe →`

### Page 2 · the system

Kicker: `THE SETUP`

Headline (one line): `One EKS cluster runs everything` (highlight "One EKS cluster" in indigo)

Diagram: `doc/linkedin/diagrams/li-architecture.png`, unaltered, filling the space below the headline. No caption.

### Page 3 · the method

Kicker: `THE METHOD · ON AWS`

Headline: `Same benchmark, run twice` (highlight "run twice" in indigo)

Three bullets, bold lead then one sentence, 30 px:

- **2.5 million vectors.** 1,536-dimension embeddings and a JSONB book filter with 16 values, each 2.6% to 8.4% of the rows.
- **144 scenarios.** Vector, filtered and hybrid search × 8 concurrency levels × 6 top_k values, 4,373 queries each.
- **Two database states.** Once as an ordinary load leaves it (no `ANALYZE` after the indexes), once after `ANALYZE`.

Screenshot card: `doc/screenshots/crops/02-eks-node-groups.png`.
Caption: `Amazon EKS console, cluster pgvector-investigation in ap-south-1: three node groups, one per role.`
`The vector database has an m6i.4xlarge (16 vCPU, 64 GiB) to itself.`

### Page 4 · the trap

Kicker: `THE TRAP`

Headline: `Ask for one more row, lose the index` (highlight "lose the index" in red)

A row of two stat tiles, equal width:

- Left tile, indigo accent: number `1.5 ms`, label `LIMIT 40 · HNSW index`, sub-label `same query, same filter`
- Right tile, red accent: number `358 ms`, label `LIMIT 41 · brute force`, sub-label `distance to all 63,945 rows`

One line under the tiles, secondary text: `In the benchmark, 1 client at top_k 50 finished 154 of 4,373 filtered queries in the time budget.`

Screenshot card: `doc/screenshots/crops/d03-trap-limit-41-bitmap.png`.
Caption: `EXPLAIN ANALYZE in DBeaver at LIMIT 41: the planner expects rows=12500,`
`the Bitmap Heap Scan reads 63,945, then sorts every distance.`

### Page 5 · why

Kicker: `THE ROOT CAUSE`

Headline (one line): `The planner was guessing` (highlight "guessing" in red)

Diagram: `doc/linkedin/diagrams/li-planner-switch.png`, unaltered, filling the space below the headline. No caption.

### Page 6 · the fix

Kicker: `THE FIX`

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

One line under the table, secondary text: `After ANALYZE the planner kept HNSW at every LIMIT up to 200, and every benchmark query completed.`

### Page 7 · what remains, and where to read more

Kicker: `WHAT REMAINS · WHERE NEXT`

Headline: `Two traps survive the fix` (highlight "survive the fix" in amber)

Three bullets, 28 px:

- **Filtered results can be incomplete.** HNSW returned 18 of 40 rows until iterative scan was enabled. Set `hnsw.iterative_scan`.
- **Hybrid search is about 3× slower.** GIN finds keyword matches but cannot rank them, so every match is fetched and scored.
- **Check your own tables.** An empty `pg_stats` result for an expression index means the planner is guessing.

Then three rows under the heading `Read more`, each a card with a bold label and plain text:

- **This post** — the trap and the fix in seven images.
- **The article** — HNSW, B-tree and GIN internals, every EXPLAIN plan, why the usual workarounds fail. Link in the post text.
- **The repository** — EKS setup guide, CNPG manifests, benchmark Jobs, raw results. `<REPO_URL>`

---

## Sources for every number

| Number | Source |
|---|---|
| 1.5 ms / 358 ms / 233× / 63,945 / rows=12500 | `doc/results/eks-20261022-trap-no-stats.txt`, sections 3a and 3b |
| 154 of 4,373 at C:1, top_k 50 | Job log of the run without statistics, `scenario_complete` event; `doc/results/` export |
| 629,712 queries completed | `benchmark_study_runs` evidence for run `20261022T045130Z` |
| 18 of 40 rows | `doc/results/eks-20261022-trap-no-stats.txt`, section 4a |
| HNSW at every LIMIT up to 200 | `doc/results/eks-20261022-fixed-stats.txt`, section 2 |
| about 3× slower hybrid | `benchmark_retrieval_summary`, C:1 top_k 10: 8.45 ms hybrid vs 2.80 ms vector |
| Workaround statuses | README, "Why the Usual Workarounds Don't Help"; full detail in `doc/workarounds.md` |

## Checklist before posting

- [ ] Seven PNGs, each exactly 1080 × 1440.
- [ ] Every number on pages 1, 4, 6 and 7 matches the article and the sources table above.
- [ ] Page 2 diagram: eight cards, seven arrows, one boundary, no load balancer.
- [ ] Page 5 diagram: one query card, two estimate cards, two plan cards, four arrows, no latency numbers.
- [ ] Both screenshot crops keep the console or DBeaver header and contain no account ID, public IP, email or password.
- [ ] Kicker, headline and page number present on all seven pages; no page overflowed.
- [ ] The repository URL is written out on pages 1 and 7; the post text has both links.
- [ ] No mention of any other cloud provider or of an earlier environment anywhere.
