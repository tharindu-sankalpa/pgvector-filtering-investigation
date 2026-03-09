"""
Article-Quality Tipping Point Plots — White Theme

Generates two publication-ready plots from existing CSV data:

  Plot 1 — "The Cliff is Filter-Agnostic"
      ef_search=40 (default), all 16 books as separate lines.
      Proves: the planner switches at the same top_k regardless of
      which book filter is applied or its data distribution.

  Plot 2 — "Raising ef_search Makes It Worse"
      One representative book, 4 ef_search values (40, 100, 200, 400).
      Proves: higher ef_search pushes the tipping point *lower*, not higher.

Both plots:
    X-axis: top_k (LIMIT value)
    Y-axis: query latency in milliseconds (log scale)
    White/light theme suitable for Medium articles.

Usage:
    uv run python scripts/plot_tipping_point_article.py
"""

import sys
from pathlib import Path
from dataclasses import dataclass

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patheffects as pe

# ---------------------------------------------------------------------------
# Project imports
# ---------------------------------------------------------------------------
sys.path.append(str(Path(__file__).parent.parent / "src"))
from shared.logger_structlog import setup_structlog

logger = setup_structlog()


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent.parent / "doc" / "plots"

# CSV files from previous runs
CSV_FILES = {
    40: DATA_DIR / "tipping_point_ef40.csv",
    100: DATA_DIR / "tipping_point_ef100.csv",
    200: DATA_DIR / "tipping_point_ef200.csv",
    400: DATA_DIR / "tipping_point_ef400.csv",
}

# Output plots
PLOT_1_OUTPUT = DATA_DIR / "article_cliff_all_books.png"
PLOT_2_OUTPUT = DATA_DIR / "article_cliff_ef_search_impact.png"

# The representative book for Plot 2.
# "The Eye of the World" is the first book in the series, recognisable,
# and mid-range in data percentage (6.38%). Any book works since they
# all behave identically, but this one is most iconic.
REPRESENTATIVE_BOOK = "01. The Eye of the World"


# ---------------------------------------------------------------------------
# Styling constants — clean white theme
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Theme:
    """White theme palette for article-quality plots."""

    bg: str = "#ffffff"
    text: str = "#1a1a2e"
    grid: str = "#e0e0e0"
    spine: str = "#cccccc"
    accent_red: str = "#dc2626"
    accent_green: str = "#16a34a"
    accent_amber: str = "#d97706"
    annotation_bg: str = "#fef3c7"

T = Theme()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------
def load_csv(ef_search: int) -> pd.DataFrame:
    """
    Load a tipping point CSV file for a given ef_search value.

    Args:
        ef_search: The ef_search value (40, 100, 200, 400).

    Returns:
        DataFrame with columns: book_name, top_k, uses_hnsw, latency_ms, plan_type
    """
    path = CSV_FILES[ef_search]
    if not path.exists():
        logger.error("csv_not_found", path=str(path), ef_search=ef_search)
        sys.exit(1)

    df = pd.read_csv(path)
    logger.info(
        "csv_loaded",
        ef_search=ef_search,
        rows=len(df),
        books=df["book_name"].nunique(),
        path=str(path),
    )
    return df


def clean_book_name(name: str) -> str:
    """
    Remove the leading number prefix from book names for cleaner labels.

    '01. The Eye of the World' → 'The Eye of the World'
    'The Wheel of Time Companion ...' → 'WoT Companion' (shortened)
    """
    if name.startswith("The Wheel of Time Companion"):
        return "WoT Companion"
    if len(name) > 3 and name[2] == ".":
        return name[4:].strip()
    return name


# ---------------------------------------------------------------------------
# Plot 1: All books, single ef_search — proves cliff is filter-agnostic
# ---------------------------------------------------------------------------
def plot_all_books_single_ef(df: pd.DataFrame, ef_search: int = 40) -> None:
    """
    Generate Plot 1: all 16 books at ef_search=40 showing the uniform cliff.

    Each book is a separate line, coloured by a plasma gradient from
    smallest (lightest) to largest (darkest) filter.

    Args:
        df: DataFrame for ef_search=40.
        ef_search: The ef_search value used (for title/annotation).
    """
    log = logger.bind(task="plot_1_all_books")

    # Only plot actually-measured values (exclude early-stop inferred rows)
    df_measured = df[df["latency_ms"] > 0].copy()

    # Get books sorted by their row count proxy (data percentage)
    # The CSV order is already sorted by pct, but let's ensure consistency
    books = df_measured["book_name"].unique()

    # Determine the tipping point (first top_k where uses_hnsw is False)
    tipping_points = {}
    for book in books:
        book_df = df_measured[df_measured["book_name"] == book].sort_values("top_k")
        bitmap_rows = book_df[book_df["uses_hnsw"] == False]
        tipping_points[book] = bitmap_rows["top_k"].min() if len(bitmap_rows) > 0 else None

    # Sort books by tipping point then name (they should all be the same)
    books_sorted = sorted(books, key=lambda b: (tipping_points.get(b, 999), b))

    # Colour palette: gradient for books
    cmap = plt.colormaps.get_cmap("viridis")
    book_colors = {b: cmap(i / max(1, len(books_sorted) - 1)) for i, b in enumerate(books_sorted)}

    # =========================================================================
    # Figure setup
    # =========================================================================
    fig, ax = plt.subplots(figsize=(16, 9))
    fig.patch.set_facecolor(T.bg)
    ax.set_facecolor(T.bg)

    # =========================================================================
    # Plot each book
    # =========================================================================
    for book in books_sorted:
        book_df = df_measured[df_measured["book_name"] == book].sort_values("top_k")
        if book_df.empty:
            continue

        color = book_colors[book]
        label = clean_book_name(book)

        # Calculate data percentage from book name ordering
        # (books are sorted by size in CSV)
        book_idx = list(books_sorted).index(book)
        alpha = 0.6 + 0.4 * (book_idx / max(1, len(books_sorted) - 1))

        ax.plot(
            book_df["top_k"], book_df["latency_ms"],
            color=color, linewidth=1.6, alpha=alpha,
            label=label, zorder=3,
        )

        # Markers: circles for HNSW, X for bitmap
        hnsw = book_df[book_df["uses_hnsw"] == True]
        bitmap = book_df[book_df["uses_hnsw"] == False]

        ax.scatter(hnsw["top_k"], hnsw["latency_ms"], color=color, marker="o", s=18, zorder=4, alpha=alpha * 0.8)
        ax.scatter(bitmap["top_k"], bitmap["latency_ms"], color=color, marker="x", s=30, zorder=4, alpha=alpha)

    # =========================================================================
    # Tipping point reference
    # =========================================================================
    valid_tips = [v for v in tipping_points.values() if v is not None]
    if valid_tips:
        from collections import Counter
        most_common_tip = Counter(valid_tips).most_common(1)[0][0]

        # Vertical dashed line at the tipping point
        ax.axvline(x=most_common_tip, color=T.accent_red, linewidth=2.5, linestyle="--", alpha=0.85, zorder=5)

        # Annotation
        ax.annotate(
            f"All 16 filters switch\nat top_k = {most_common_tip}",
            xy=(most_common_tip, 500),
            xytext=(most_common_tip + 3, 300),
            fontsize=12, color=T.accent_red, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=T.accent_red, lw=2),
            bbox=dict(boxstyle="round,pad=0.4", facecolor=T.annotation_bg, edgecolor=T.accent_red, alpha=0.9),
            zorder=6,
        )

    # =========================================================================
    # Reference lines
    # =========================================================================
    ax.axhline(y=10, color=T.accent_green, linewidth=0.8, linestyle="-.", alpha=0.4)
    ax.text(1.5, 12, "10 ms", fontsize=9, color=T.accent_green, alpha=0.7, fontweight="bold")

    ax.axhline(y=1000, color=T.accent_red, linewidth=0.8, linestyle="-.", alpha=0.4)
    ax.text(1.5, 1200, "1,000 ms", fontsize=9, color=T.accent_red, alpha=0.7, fontweight="bold")

    # =========================================================================
    # Axis formatting
    # =========================================================================
    ax.set_xlabel("top_k (LIMIT value)", fontsize=14, color=T.text, fontweight="bold", labelpad=10)
    ax.set_ylabel("Query Latency (ms)", fontsize=14, color=T.text, fontweight="bold", labelpad=10)
    ax.set_yscale("log")

    ax.set_title(
        f"Filtered Vector Search Cliff — All 16 Book Filters (ef_search = {ef_search})\n"
        "The planner abandons HNSW at the same top_k regardless of filter selectivity",
        fontsize=15, color=T.text, fontweight="bold", pad=20,
    )

    # Y-axis formatting
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}" if x >= 1 else f"{x:.1f}"))

    # Grid
    ax.grid(True, which="major", color=T.grid, alpha=0.6, linewidth=0.6)
    ax.grid(True, which="minor", color=T.grid, alpha=0.3, linewidth=0.3)
    ax.tick_params(colors=T.text, labelsize=11)
    for spine in ax.spines.values():
        spine.set_color(T.spine)

    # Legend — right side, outside the plot
    legend = ax.legend(
        loc="upper left",
        fontsize=8.5,
        facecolor=T.bg,
        edgecolor=T.spine,
        labelcolor=T.text,
        framealpha=0.95,
        ncol=2,
        title="Book Filters (sorted by data %)",
        title_fontsize=9,
    )

    # =========================================================================
    # Save
    # =========================================================================
    fig.savefig(PLOT_1_OUTPUT, dpi=200, bbox_inches="tight", facecolor=T.bg)
    plt.close(fig)
    log.info("plot_1_saved", path=str(PLOT_1_OUTPUT))


# ---------------------------------------------------------------------------
# Plot 2: Single book, multiple ef_search — proves raising ef_search is worse
# ---------------------------------------------------------------------------
def plot_ef_search_impact(book_name: str = REPRESENTATIVE_BOOK) -> None:
    """
    Generate Plot 2: one book across all ef_search values showing that
    raising ef_search pushes the tipping point lower.

    Each ef_search value is a separate coloured line.

    Args:
        book_name: The book filter to use (same behaviour for all books).
    """
    log = logger.bind(task="plot_2_ef_impact", book=book_name)

    # Colour palette for ef_search values — from cool (low) to warm (high)
    ef_colors = {
        40: "#2563eb",    # Blue — default, best
        100: "#7c3aed",   # Purple
        200: "#db2777",   # Pink
        400: "#dc2626",   # Red — worst
    }

    ef_labels = {
        40: "ef_search = 40 (default)",
        100: "ef_search = 100",
        200: "ef_search = 200",
        400: "ef_search = 400",
    }

    # =========================================================================
    # Figure setup
    # =========================================================================
    fig, ax = plt.subplots(figsize=(16, 9))
    fig.patch.set_facecolor(T.bg)
    ax.set_facecolor(T.bg)

    # Track tipping points for annotation table
    tipping_summary = {}

    # =========================================================================
    # Plot each ef_search
    # =========================================================================
    for ef_search in sorted(CSV_FILES.keys()):
        df = load_csv(ef_search)

        # Filter to the representative book and only measured values
        book_df = df[(df["book_name"] == book_name) & (df["latency_ms"] > 0)].sort_values("top_k")

        if book_df.empty:
            log.warning("no_data", ef_search=ef_search, book=book_name)
            continue

        color = ef_colors[ef_search]
        label = ef_labels[ef_search]

        # Find tipping point
        bitmap_rows = book_df[book_df["uses_hnsw"] == False]
        tip = bitmap_rows["top_k"].min() if len(bitmap_rows) > 0 else None
        tipping_summary[ef_search] = tip

        # Plot the line
        ax.plot(
            book_df["top_k"], book_df["latency_ms"],
            color=color, linewidth=2.5, alpha=0.9,
            label=label, zorder=3 + ef_search,
        )

        # Markers
        hnsw = book_df[book_df["uses_hnsw"] == True]
        bitmap = book_df[book_df["uses_hnsw"] == False]

        ax.scatter(hnsw["top_k"], hnsw["latency_ms"], color=color, marker="o", s=30, zorder=4 + ef_search, alpha=0.8)
        ax.scatter(bitmap["top_k"], bitmap["latency_ms"], color=color, marker="x", s=50, zorder=4 + ef_search, alpha=0.9)

        # Mark the tipping point with a vertical dotted line
        if tip is not None:
            ax.axvline(
                x=tip, color=color, linewidth=1.2, linestyle=":",
                alpha=0.5, zorder=2,
            )

    # =========================================================================
    # Summary annotation box — shows tipping points for each ef_search
    # =========================================================================
    summary_text = "Tipping Points:\n"
    for ef, tip in sorted(tipping_summary.items()):
        arrow = "→" if ef == 40 else "↓"
        summary_text += f"  ef_search={ef:>3d}  {arrow}  top_k = {tip}\n"

    # Place the annotation in the upper-right area
    ax.text(
        0.97, 0.55, summary_text.strip(),
        transform=ax.transAxes,
        fontsize=11, fontfamily="monospace",
        color=T.text,
        verticalalignment="top", horizontalalignment="right",
        bbox=dict(
            boxstyle="round,pad=0.6",
            facecolor="#f8fafc",
            edgecolor=T.spine,
            alpha=0.95,
        ),
        zorder=10,
    )

    # =========================================================================
    # Directional annotation — "Higher ef_search = Earlier cliff"
    # =========================================================================
    # Arrow from the ef400 cliff to ef40 cliff to show the direction
    tips_sorted = sorted(tipping_summary.items())
    if len(tips_sorted) >= 2:
        first_ef, first_tip = tips_sorted[-1]   # ef_search=400, lowest tip
        last_ef, last_tip = tips_sorted[0]      # ef_search=40, highest tip

        ax.annotate(
            "",
            xy=(first_tip, 30), xytext=(last_tip, 30),
            arrowprops=dict(
                arrowstyle="<->", color=T.accent_amber,
                lw=2.5, connectionstyle="arc3,rad=0",
            ),
            zorder=7,
        )
        mid_tip = (first_tip + last_tip) / 2
        ax.text(
            mid_tip, 22,
            f"Higher ef_search\n← narrows usable range →",
            fontsize=10, color=T.accent_amber, fontweight="bold",
            ha="center", va="top",
            bbox=dict(boxstyle="round,pad=0.3", facecolor=T.annotation_bg, edgecolor=T.accent_amber, alpha=0.85),
            zorder=8,
        )

    # =========================================================================
    # Reference lines
    # =========================================================================
    ax.axhline(y=10, color=T.accent_green, linewidth=0.8, linestyle="-.", alpha=0.4)
    ax.text(1.5, 12, "10 ms", fontsize=9, color=T.accent_green, alpha=0.7, fontweight="bold")

    ax.axhline(y=1000, color=T.accent_red, linewidth=0.8, linestyle="-.", alpha=0.4)
    ax.text(1.5, 1200, "1,000 ms", fontsize=9, color=T.accent_red, alpha=0.7, fontweight="bold")

    # =========================================================================
    # Axis formatting
    # =========================================================================
    clean_name = clean_book_name(book_name)
    ax.set_xlabel("top_k (LIMIT value)", fontsize=14, color=T.text, fontweight="bold", labelpad=10)
    ax.set_ylabel("Query Latency (ms)", fontsize=14, color=T.text, fontweight="bold", labelpad=10)
    ax.set_yscale("log")

    ax.set_title(
        f"Impact of ef_search on the Filtered Search Cliff — \"{clean_name}\"\n"
        "Raising ef_search makes the planner abandon HNSW at a lower top_k",
        fontsize=15, color=T.text, fontweight="bold", pad=20,
    )

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}" if x >= 1 else f"{x:.1f}"))

    ax.grid(True, which="major", color=T.grid, alpha=0.6, linewidth=0.6)
    ax.grid(True, which="minor", color=T.grid, alpha=0.3, linewidth=0.3)
    ax.tick_params(colors=T.text, labelsize=11)
    for spine in ax.spines.values():
        spine.set_color(T.spine)

    # Legend
    legend = ax.legend(
        loc="upper left",
        fontsize=11,
        facecolor=T.bg,
        edgecolor=T.spine,
        labelcolor=T.text,
        framealpha=0.95,
    )

    # =========================================================================
    # Save
    # =========================================================================
    fig.savefig(PLOT_2_OUTPUT, dpi=200, bbox_inches="tight", facecolor=T.bg)
    plt.close(fig)
    log.info("plot_2_saved", path=str(PLOT_2_OUTPUT))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    """Generate both article-quality plots from existing CSV data."""
    logger.info("=" * 70)
    logger.info("article_plots_start")

    # Verify all CSVs exist
    for ef, path in CSV_FILES.items():
        if not path.exists():
            logger.error("missing_csv", ef_search=ef, path=str(path))
            sys.exit(1)
        logger.info("csv_found", ef_search=ef, path=str(path))

    # Plot 1: All books at ef_search=40
    logger.info("generating_plot_1", description="All books, ef_search=40")
    df_ef40 = load_csv(40)
    plot_all_books_single_ef(df_ef40, ef_search=40)

    # Plot 2: Single book across all ef_search values
    logger.info("generating_plot_2", description="Single book, all ef_search values", book=REPRESENTATIVE_BOOK)
    plot_ef_search_impact(REPRESENTATIVE_BOOK)

    logger.info("=" * 70)
    logger.info(
        "article_plots_complete",
        plot_1=str(PLOT_1_OUTPUT),
        plot_2=str(PLOT_2_OUTPUT),
    )


if __name__ == "__main__":
    main()
