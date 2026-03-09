"""
CNPG Planner Tipping Point Analysis — Latency vs top_k for All Book Filters

Measures the actual query latency of filtered vector search at every top_k
value to visualise the exact point where the query planner abandons the HNSW
index and switches to the catastrophic B-tree + Bitmap brute-force plan.

The ef_search parameter is configurable so we can test the hypothesis:
    "Does raising ef_search push the tipping point higher, or lower?"

Plot output:
    X-axis: top_k value
    Y-axis: query latency in milliseconds (log scale)
    Lines:  one per book filter, coloured by data percentage

Environment Variables:
    EF_SEARCH:   HNSW ef_search value to set (default: 40, pgvector default)
    BOOK_SUBSET: Optional comma-separated book indices to test (e.g. "0,7,15"
                 for smallest, middle, largest). If empty, tests all 16 books.

Usage:
    export $(grep -v '^#' .env.azure | xargs)

    # Default ef_search=40
    uv run python scripts/cnpg_planner_tipping_point_all_books.py

    # Test with ef_search=100
    EF_SEARCH=100 uv run python scripts/cnpg_planner_tipping_point_all_books.py

    # Test with ef_search=200, subset of 3 books
    EF_SEARCH=200 BOOK_SUBSET=0,7,15 uv run python scripts/cnpg_planner_tipping_point_all_books.py
"""

import os
import sys
import csv
import time
from pathlib import Path
from dataclasses import dataclass, field

import numpy as np
import psycopg
from pgvector.psycopg import register_vector
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
sys.path.append(str(Path(__file__).parent.parent / "src"))
from shared.logger_structlog import setup_structlog

logger = setup_structlog()


# ---------------------------------------------------------------------------
# Connection parameters — sourced from .env.azure
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("CNPG_PG_HOST", "51.104.162.145")
PG_PORT = int(os.getenv("CNPG_PG_PORT", "5432"))
PG_DATABASE = os.getenv("VECTOR_PG_DATABASE", "benchmark_vectors")
PG_USER = os.getenv("CNPG_PG_USER", "benchmark_user")
PG_PASSWORD = os.getenv("CNPG_PG_PASSWORD", "")
TABLE_NAME = os.getenv("TABLE_NAME", "wot_chunks_2_5m")
SSLMODE = os.getenv("PGSSLMODE", "require")

# ---------------------------------------------------------------------------
# HNSW ef_search configuration
# ---------------------------------------------------------------------------
EF_SEARCH = int(os.getenv("EF_SEARCH", "40"))

# Book subset: indices into the sorted-by-count book list
BOOK_SUBSET_RAW = os.getenv("BOOK_SUBSET", "")
BOOK_SUBSET_INDICES = (
    [int(i) for i in BOOK_SUBSET_RAW.split(",") if i.strip()]
    if BOOK_SUBSET_RAW.strip() else None
)

# ---------------------------------------------------------------------------
# Output paths — filenames include ef_search for comparison across runs
# ---------------------------------------------------------------------------
OUTPUT_DIR = Path(__file__).parent.parent / "doc" / "plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_OUTPUT = OUTPUT_DIR / f"tipping_point_ef{EF_SEARCH}.csv"
PLOT_OUTPUT = OUTPUT_DIR / f"tipping_point_latency_ef{EF_SEARCH}.png"


# ---------------------------------------------------------------------------
# top_k sweep values — designed to capture the cliff precisely
# ---------------------------------------------------------------------------
# Coarse points for the "flat HNSW region" at low top_k
COARSE_TOP_K = [1, 5, 10, 15, 20, 25]

# Fine sweep: every integer from 30 to 50 to pinpoint the cliff
FINE_TOP_K = list(range(30, 51))

# Full sweep list
ALL_TOP_K = sorted(set(COARSE_TOP_K + FINE_TOP_K))

# Early-stop: after N consecutive bitmap detections per book,
# skip remaining top_k values (they'll all be bitmap too).
# We keep 2 measurements so the cliff is visible in the plot.
BITMAP_EARLY_STOP = 2


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class BookInfo:
    """Metadata about a single book filter value."""

    book_name: str
    row_count: int
    pct_of_total: float


@dataclass
class ProbeResult:
    """Result of a single EXPLAIN ANALYZE run."""

    book_name: str
    top_k: int
    uses_hnsw: bool
    latency_ms: float
    plan_type: str


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_connection() -> psycopg.Connection:
    """
    Create a psycopg3 connection, register pgvector types,
    and set ef_search if non-default.

    Returns:
        psycopg.Connection: Active database connection.
    """
    conn = psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode=SSLMODE,
        autocommit=True,
    )
    register_vector(conn)

    # Set ef_search to the configured value
    conn.execute(f"SET hnsw.ef_search = {EF_SEARCH}")

    # Verify the setting took effect
    current_ef = conn.execute("SHOW hnsw.ef_search").fetchone()[0]
    logger.info("connection_ready", ef_search=current_ef, host=PG_HOST, database=PG_DATABASE)

    return conn


def fetch_all_books(conn: psycopg.Connection) -> list[BookInfo]:
    """
    Retrieve all distinct book_name values with row counts.
    Sorted by row_count ascending (smallest filter first).

    Args:
        conn: Active database connection.

    Returns:
        List of BookInfo objects sorted by row_count ascending.
    """
    log = logger.bind(task="fetch_all_books")

    total_count = conn.execute(f"SELECT count(*) FROM {TABLE_NAME}").fetchone()[0]
    log.info("total_rows", count=total_count)

    rows = conn.execute(
        f"""
        SELECT metadata->>'book_name' AS book_name, count(*) AS cnt
        FROM {TABLE_NAME}
        WHERE metadata->>'book_name' IS NOT NULL
        GROUP BY metadata->>'book_name'
        ORDER BY cnt ASC
        """
    ).fetchall()

    books = []
    for row in rows:
        pct = (row[1] / total_count) * 100
        books.append(BookInfo(book_name=row[0], row_count=row[1], pct_of_total=round(pct, 2)))

    for b in books:
        log.info("book_filter", book=b.book_name, rows=b.row_count, pct=b.pct_of_total)

    return books


def fetch_sample_embedding(conn: psycopg.Connection) -> np.ndarray:
    """
    Grab one real embedding to use as the query vector for all runs.
    Using the same vector ensures latency differences are attributable
    to the planner's plan choice, not the query vector's position.

    Args:
        conn: Active database connection.

    Returns:
        np.ndarray: A 1536-dimensional embedding.
    """
    row = conn.execute(f"SELECT embedding FROM {TABLE_NAME} LIMIT 1").fetchone()
    embedding = np.array(row[0], dtype=np.float32)
    logger.info("sample_embedding_fetched", dim=len(embedding))
    return embedding


def probe_plan(
    conn: psycopg.Connection,
    embedding: np.ndarray,
    book_name: str,
    top_k: int,
) -> ProbeResult:
    """
    Run EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) for a filtered vector search
    and return the plan type and actual execution latency.

    Args:
        conn: Active database connection.
        embedding: Query vector.
        book_name: Filter value.
        top_k: LIMIT value.

    Returns:
        ProbeResult with plan detection and latency.
    """
    query = f"""
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT id, 1 - (embedding <=> %s::vector) AS similarity
        FROM {TABLE_NAME}
        WHERE metadata->>'book_name' = %s
        ORDER BY embedding <=> %s::vector
        LIMIT {top_k}
    """

    rows = conn.execute(query, (embedding, book_name, embedding)).fetchall()
    plan_text = "\n".join(r[0] for r in rows)

    # Detect plan type from EXPLAIN output
    uses_hnsw = "wot_chunks_2_5m_embedding_idx" in plan_text

    if uses_hnsw:
        plan_type = "HNSW"
    elif "Bitmap Heap Scan" in plan_text:
        plan_type = "Bitmap"
    elif "Sort" in plan_text:
        plan_type = "Sort"
    else:
        plan_type = "Other"

    # Parse execution time from EXPLAIN output
    latency_ms = 0.0
    for line in [r[0] for r in rows]:
        if "Execution Time:" in line:
            try:
                latency_ms = float(line.split("Execution Time:")[1].strip().replace(" ms", ""))
            except (ValueError, IndexError):
                pass

    return ProbeResult(
        book_name=book_name,
        top_k=top_k,
        uses_hnsw=uses_hnsw,
        latency_ms=round(latency_ms, 3),
        plan_type=plan_type,
    )


# ---------------------------------------------------------------------------
# Sweep logic
# ---------------------------------------------------------------------------
def sweep_all_books(
    conn: psycopg.Connection,
    books: list[BookInfo],
    embedding: np.ndarray,
) -> list[ProbeResult]:
    """
    For each book, sweep across all top_k values and record the plan and latency.

    Uses early-stop after BITMAP_EARLY_STOP consecutive bitmap detections
    to avoid unnecessary 100+ second queries at high top_k values.

    Args:
        conn: Active database connection.
        books: List of BookInfo objects.
        embedding: Query vector.

    Returns:
        List of ProbeResult for all actually-measured (book, top_k) combinations.
    """
    all_results: list[ProbeResult] = []

    for i, book in enumerate(books):
        log = logger.bind(
            task="sweep",
            book_idx=f"{i + 1}/{len(books)}",
            book=book.book_name,
            pct=book.pct_of_total,
        )
        log.info("starting_book_sweep")

        consecutive_bitmap = 0

        for top_k in ALL_TOP_K:
            # Early-stop: skip if we've already seen enough bitmap plans
            if consecutive_bitmap >= BITMAP_EARLY_STOP:
                log.info("early_stop", skipped_top_k=top_k)
                continue

            result = probe_plan(conn, embedding, book.book_name, top_k)
            all_results.append(result)

            log.info(
                "probe_result",
                top_k=top_k,
                plan=result.plan_type,
                latency_ms=result.latency_ms,
                uses_hnsw=result.uses_hnsw,
            )

            # Track consecutive bitmap hits
            if result.uses_hnsw:
                consecutive_bitmap = 0
            else:
                consecutive_bitmap += 1

        log.info("book_sweep_complete")

    return all_results


def find_tipping_points(
    results: list[ProbeResult],
    books: list[BookInfo],
) -> dict[str, int | None]:
    """
    For each book, find the first top_k value where HNSW is abandoned.

    Args:
        results: All ProbeResult objects.
        books: BookInfo objects.

    Returns:
        Dict mapping book_name -> first top_k where bitmap was used (or None).
    """
    tipping = {}
    for book in books:
        book_results = sorted(
            [r for r in results if r.book_name == book.book_name],
            key=lambda r: r.top_k,
        )

        first_bitmap = None
        for r in book_results:
            if not r.uses_hnsw:
                first_bitmap = r.top_k
                break

        tipping[book.book_name] = first_bitmap
        logger.info(
            "tipping_point",
            book=book.book_name,
            switches_at=first_bitmap,
            pct=book.pct_of_total,
        )

    return tipping


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------
def save_csv(results: list[ProbeResult]) -> None:
    """Save all probe results to CSV for reproducibility."""
    with open(CSV_OUTPUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["book_name", "top_k", "uses_hnsw", "latency_ms", "plan_type"])
        for r in results:
            writer.writerow([r.book_name, r.top_k, r.uses_hnsw, r.latency_ms, r.plan_type])

    logger.info("csv_saved", path=str(CSV_OUTPUT), rows=len(results))


# ---------------------------------------------------------------------------
# Plotting — latency vs top_k, one line per book
# ---------------------------------------------------------------------------
def plot_latency_vs_topk(
    results: list[ProbeResult],
    books: list[BookInfo],
    tipping_points: dict[str, int | None],
) -> None:
    """
    Generate a publication-quality plot showing query latency vs top_k
    for every book filter, with the planner transition clearly visible.

    Layout:
        Single panel, dark theme.
        X-axis: top_k value (linear)
        Y-axis: query latency in ms (log scale)
        One line per book, coloured by a gradient from light (small book) to
        dark (large book).
        A vertical dashed line marks the most common tipping point.

    Args:
        results: All ProbeResult objects.
        books: BookInfo objects for metadata.
        tipping_points: Dict from find_tipping_points().
    """
    log = logger.bind(task="plot")

    # --- Styling constants ---
    bg_color = "#0f172b"
    paper_color = "#1d293d"
    text_color = "#e2e8f0"
    grid_color = "#314158"
    hnsw_marker = "o"
    bitmap_marker = "x"

    # Sort books by percentage for consistent colour ordering
    sorted_books = sorted(books, key=lambda b: b.pct_of_total)

    # Generate a colour palette: gradient from cool (small) to warm (large)
    # Using a colormap that looks good on dark backgrounds
    cmap = plt.cm.get_cmap("plasma", len(sorted_books))
    book_colors = {b.book_name: cmap(i / max(1, len(sorted_books) - 1)) for i, b in enumerate(sorted_books)}

    # --- Create figure ---
    fig, ax = plt.subplots(figsize=(18, 10))
    fig.patch.set_facecolor(paper_color)
    ax.set_facecolor(bg_color)

    # --- Plot each book as a separate line ---
    for book in sorted_books:
        # Get results for this book, sorted by top_k
        book_results = sorted(
            [r for r in results if r.book_name == book.book_name],
            key=lambda r: r.top_k,
        )

        if not book_results:
            continue

        top_k_vals = [r.top_k for r in book_results]
        latency_vals = [r.latency_ms for r in book_results]
        color = book_colors[book.book_name]

        # Clean display label: remove leading number prefix
        clean_name = book.book_name
        if len(clean_name) > 3 and clean_name[2] == ".":
            clean_name = clean_name[4:].strip()
        label = f"{clean_name} ({book.pct_of_total}%)"

        # Plot the line
        ax.plot(
            top_k_vals, latency_vals,
            color=color, linewidth=1.8, alpha=0.9,
            label=label, zorder=3,
        )

        # Mark HNSW points with circles, bitmap with X
        hnsw_x = [r.top_k for r in book_results if r.uses_hnsw]
        hnsw_y = [r.latency_ms for r in book_results if r.uses_hnsw]
        bitmap_x = [r.top_k for r in book_results if not r.uses_hnsw]
        bitmap_y = [r.latency_ms for r in book_results if not r.uses_hnsw]

        ax.scatter(hnsw_x, hnsw_y, color=color, marker=hnsw_marker, s=25, zorder=4, alpha=0.8)
        ax.scatter(bitmap_x, bitmap_y, color=color, marker=bitmap_marker, s=40, zorder=4, alpha=0.9)

    # --- Tipping point reference line ---
    # Find the most common tipping point across all books
    valid_tips = [v for v in tipping_points.values() if v is not None]
    if valid_tips:
        from collections import Counter
        most_common_tip = Counter(valid_tips).most_common(1)[0][0]

        ax.axvline(
            x=most_common_tip, color="#ef4444", linewidth=2, linestyle="--",
            alpha=0.8, zorder=5, label=f"Tipping point: top_k={most_common_tip}",
        )

        # Add annotation
        ax.annotate(
            f"Planner abandons HNSW\nat top_k = {most_common_tip}",
            xy=(most_common_tip, ax.get_ylim()[1] * 0.3 if ax.get_ylim()[1] > 100 else 100),
            xytext=(most_common_tip + 2, ax.get_ylim()[1] * 0.3 if ax.get_ylim()[1] > 100 else 100),
            fontsize=10, color="#ef4444", fontweight="bold",
            arrowprops=dict(arrowstyle="->", color="#ef4444", lw=1.5),
            zorder=6,
        )

    # --- ef_search reference line ---
    ax.axvline(
        x=EF_SEARCH, color="#f59e0b", linewidth=1.5, linestyle=":",
        alpha=0.6, zorder=2, label=f"ef_search = {EF_SEARCH}",
    )

    # --- Axis formatting ---
    ax.set_xlabel("top_k (LIMIT value)", fontsize=13, color=text_color, fontweight="bold")
    ax.set_ylabel("Query Latency (ms)", fontsize=13, color=text_color, fontweight="bold")
    ax.set_yscale("log")
    ax.set_title(
        f"Filtered Vector Search Latency vs top_k — All Book Filters\n"
        f"hnsw.ef_search = {EF_SEARCH}  |  2.5M Vectors, 1536 Dimensions, CNPG pgvector",
        fontsize=15, color=text_color, fontweight="bold", pad=20,
    )

    # Format y-axis ticks as plain numbers (not scientific notation)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}" if x >= 1 else f"{x:.1f}"))

    # --- Grid and spines ---
    ax.grid(True, which="major", color=grid_color, alpha=0.4, linewidth=0.5)
    ax.grid(True, which="minor", color=grid_color, alpha=0.2, linewidth=0.3)
    ax.tick_params(colors=text_color, labelsize=10)
    for spine in ax.spines.values():
        spine.set_color(grid_color)

    # --- Legend ---
    legend = ax.legend(
        loc="upper left",
        fontsize=8,
        facecolor=bg_color,
        edgecolor=grid_color,
        labelcolor=text_color,
        framealpha=0.95,
        ncol=2,
    )

    # --- Marker legend (separate, bottom-right) ---
    ax.scatter([], [], marker=hnsw_marker, color=text_color, s=30, label="○ = HNSW Index Scan")
    ax.scatter([], [], marker=bitmap_marker, color=text_color, s=40, label="✕ = Bitmap Brute-Force")

    # --- Horizontal reference lines for context ---
    # 10ms threshold (typical SLA for fast queries)
    ax.axhline(y=10, color="#22c55e", linewidth=0.8, linestyle="-.", alpha=0.3)
    ax.text(ALL_TOP_K[0], 12, "10ms", fontsize=8, color="#22c55e", alpha=0.5)

    # 1000ms threshold (1 second — clearly broken)
    ax.axhline(y=1000, color="#ef4444", linewidth=0.8, linestyle="-.", alpha=0.3)
    ax.text(ALL_TOP_K[0], 1200, "1,000ms", fontsize=8, color="#ef4444", alpha=0.5)

    # --- Save ---
    fig.savefig(PLOT_OUTPUT, dpi=200, bbox_inches="tight", facecolor=paper_color)
    plt.close(fig)
    log.info("plot_saved", path=str(PLOT_OUTPUT))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """
    Main workflow:
    1. Connect to CNPG, set ef_search.
    2. Discover all book filters.
    3. Sweep top_k for each book, running EXPLAIN ANALYZE.
    4. Identify tipping points.
    5. Save CSV and generate the latency plot.
    """
    logger.info("=" * 70)
    logger.info(
        "analysis_start",
        ef_search=EF_SEARCH,
        top_k_range=f"{ALL_TOP_K[0]}-{ALL_TOP_K[-1]}",
        total_top_k_values=len(ALL_TOP_K),
    )
    logger.info("=" * 70)

    if not PG_PASSWORD:
        logger.error("missing_pg_password", hint="Load .env.azure")
        sys.exit(1)

    # Step 1: Connect
    conn = get_connection()

    # Step 2: Discover books
    all_books = fetch_all_books(conn)

    # Apply subset if configured
    if BOOK_SUBSET_INDICES:
        books = [all_books[i] for i in BOOK_SUBSET_INDICES if i < len(all_books)]
        logger.info(
            "book_subset_applied",
            total=len(all_books),
            selected=[b.book_name for b in books],
        )
    else:
        books = all_books

    logger.info(
        "sweep_config",
        ef_search=EF_SEARCH,
        num_books=len(books),
        top_k_values=ALL_TOP_K,
        total_queries=len(books) * len(ALL_TOP_K),
        early_stop_after=f"{BITMAP_EARLY_STOP} consecutive bitmap detections",
    )

    # Step 3: Fetch a sample embedding
    embedding = fetch_sample_embedding(conn)

    # Step 4: Run the sweep
    results = sweep_all_books(conn, books, embedding)

    # Step 5: Find tipping points
    tipping_points = find_tipping_points(results, books)

    # Step 6: Save CSV
    save_csv(results)

    # Step 7: Plot
    plot_latency_vs_topk(results, books, tipping_points)

    # Summary
    logger.info("=" * 70)
    logger.info("analysis_complete", ef_search=EF_SEARCH)
    for book_name, tip in sorted(tipping_points.items(), key=lambda x: x[1] or 999):
        logger.info("summary", book=book_name, switches_at=tip)
    logger.info("=" * 70)

    conn.close()
    logger.info("connection_closed")


if __name__ == "__main__":
    main()
