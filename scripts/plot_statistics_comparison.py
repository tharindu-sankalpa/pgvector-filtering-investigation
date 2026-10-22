"""
Plot filtered search before and after ANALYZE, for the article's "The Fix" section.

The retrieval matrix was run twice on the same table, indexes and settings:
once without statistics for the metadata->>'book_name' expression (the state a
bulk load followed by CREATE INDEX leaves behind) and once after ANALYZE. This
script reads both runs from the results database and draws one figure with two
panels that share the article's x-axis notation "C:{concurrency} | K:{top_k}":

  - Top: average filtered-search latency (log scale) in both states, with pure
    vector search from the run with statistics as a reference line.
  - Bottom: the share of the 4,373 queries per scenario that completed within
    the benchmark's time budget, in both states.

The latency panel alone would hide the worst of the trap, because the stored
latency percentiles describe successful queries only; the bottom panel shows
how many queries that is.

Usage:
    # Port-forward the results database first (see doc/execution-guide.md, section 7)
    export RESULTS_PG_HOST=127.0.0.1 RESULTS_PG_PORT=15433 RESULTS_PG_USER=results_user
    export RESULTS_PG_PASSWORD=...   # from the benchmark-results-app Secret
    uv run python scripts/plot_statistics_comparison.py "<config without stats>" "<config with stats>"

Output:
    doc/plots/article_filtered_before_after.png
"""

import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg

sys.path.append(str(Path(__file__).parent.parent / "src"))
from shared.logger_structlog import setup_structlog

logger = setup_structlog()

# Output location shared with the other article plotting scripts.
OUTPUT_DIR = Path(__file__).parent.parent / "doc" / "plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = OUTPUT_DIR / "article_filtered_before_after.png"

# Connection settings for the results database, in the same order of
# precedence as plot_article_benchmarks.py.
RESULTS_PG_HOST = os.getenv("RESULTS_PG_HOST", os.getenv("PG_HOST", ""))
RESULTS_PG_PORT = os.getenv("RESULTS_PG_PORT", "5432")
RESULTS_PG_USER = os.getenv("RESULTS_PG_USER", os.getenv("PG_USER", ""))
RESULTS_PG_PASSWORD = os.getenv("RESULTS_PG_PASSWORD", os.getenv("PG_PASSWORD", ""))
RESULTS_PG_DATABASE = "benchmark_results"

# Every scenario submits this many queries; total_queries stores the successful ones.
QUERIES_PER_SCENARIO = 4373

# Colours from the article's diagram palette.
COLOUR_NO_STATS = "#B91C1C"
COLOUR_WITH_STATS = "#4F46E5"
COLOUR_VECTOR = "#94A3B8"


def fetch_run(conn: psycopg.Connection, database_config: str) -> pd.DataFrame:
    """Fetch every retrieval scenario summary recorded under one configuration label.

    Each run of the retrieval matrix writes one row per (test_type, concurrency,
    top_k) scenario, labelled with the run's DATABASE_CONFIG. When a scenario was
    recorded more than once under the same label, the latest row wins.

    Args:
        conn: Open connection to the benchmark_results database.
        database_config: Exact database_config value of the run.

    Returns:
        DataFrame with columns test_type, concurrency_level, top_k, total_queries
        and avg_ms (average latency of the successful queries, in milliseconds).

    Raises:
        ValueError: If no rows exist for the label, which usually means a typo.
    """
    log = logger.bind(task="fetch_run", database_config=database_config)

    # DISTINCT ON keeps the most recent row per scenario.
    query = """
        SELECT DISTINCT ON (test_type, concurrency_level, top_k)
               test_type, concurrency_level, top_k, total_queries,
               avg_latency_seconds * 1000.0 AS avg_ms
        FROM benchmark_retrieval_summary
        WHERE dataset_size = 2500000 AND database_config = %s
        ORDER BY test_type, concurrency_level, top_k, id DESC
    """
    df = pd.read_sql(query, conn, params=(database_config,))

    # A missing label would silently plot an empty panel; fail loudly instead.
    if df.empty:
        log.error("no_rows_for_config")
        raise ValueError(f"No retrieval summaries for database_config={database_config!r}")

    log.info("run_fetched",
             rows=len(df),
             test_types=sorted(df["test_type"].unique().tolist()),
             memory_kb=round(df.memory_usage(deep=True).sum() / 1024, 1))
    return df


def scenario_label(concurrency: int, top_k: int) -> str:
    """Format one scenario in the article's x-axis notation.

    Args:
        concurrency: Concurrency level of the scenario.
        top_k: Requested result count (the SQL LIMIT).

    Returns:
        The label, for example "C:50 | K:20".
    """
    return f"C:{concurrency} | K:{top_k}"


def build_comparison(no_stats: pd.DataFrame, with_stats: pd.DataFrame) -> pd.DataFrame:
    """Join the two runs on scenario, keeping filtered search plus the vector reference.

    Args:
        no_stats: Run without expression statistics, from fetch_run.
        with_stats: Run after ANALYZE, from fetch_run.

    Returns:
        One row per (concurrency, top_k) scenario, sorted by concurrency then
        top_k, with filtered latency and completion for both states and the
        vector-search latency from the run with statistics.
    """
    log = logger.bind(task="build_comparison")

    # Filtered search in each state, renamed so the two can sit side by side.
    keys = ["concurrency_level", "top_k"]
    trap = no_stats[no_stats["test_type"] == "Filtered Search"][keys + ["avg_ms", "total_queries"]]
    trap = trap.rename(columns={"avg_ms": "no_stats_ms", "total_queries": "no_stats_ok"})
    fixed = with_stats[with_stats["test_type"] == "Filtered Search"][keys + ["avg_ms", "total_queries"]]
    fixed = fixed.rename(columns={"avg_ms": "with_stats_ms", "total_queries": "with_stats_ok"})

    # Pure vector search after ANALYZE is the "no filter" reference line.
    vector = with_stats[with_stats["test_type"] == "Vector Search"][keys + ["avg_ms"]]
    vector = vector.rename(columns={"avg_ms": "vector_ms"})

    # Inner joins keep only scenarios present in both runs.
    merged = trap.merge(fixed, on=keys).merge(vector, on=keys)
    merged = merged.sort_values(keys).reset_index(drop=True)

    # Completion as a percentage of the queries each scenario submitted.
    merged["no_stats_pct"] = 100.0 * merged["no_stats_ok"] / QUERIES_PER_SCENARIO
    merged["with_stats_pct"] = 100.0 * merged["with_stats_ok"] / QUERIES_PER_SCENARIO
    merged["label"] = [scenario_label(c, k) for c, k in zip(merged["concurrency_level"], merged["top_k"])]

    log.info("comparison_built",
             scenarios=len(merged),
             columns=list(merged.columns),
             min_no_stats_pct=round(float(merged["no_stats_pct"].min()), 1),
             min_with_stats_pct=round(float(merged["with_stats_pct"].min()), 1))
    return merged


def plot(merged: pd.DataFrame, output_file: Path) -> None:
    """Draw the two-panel before/after figure and save it as a PNG.

    Args:
        merged: Output of build_comparison.
        output_file: Destination path for the PNG.
    """
    log = logger.bind(task="plot", output=str(output_file))

    x = list(range(len(merged)))
    fig, (ax_latency, ax_done) = plt.subplots(2, 1, figsize=(18, 9), sharex=True,
                                              gridspec_kw={"height_ratios": [3, 2]})

    # --- Top panel: latency of successful queries, log scale -----------------
    ax_latency.plot(x, merged["no_stats_ms"], color=COLOUR_NO_STATS, marker="o", linewidth=2,
                    label="Filtered search, no statistics")
    ax_latency.plot(x, merged["with_stats_ms"], color=COLOUR_WITH_STATS, marker="o", linewidth=2,
                    label="Filtered search, after ANALYZE")
    ax_latency.plot(x, merged["vector_ms"], color=COLOUR_VECTOR, linestyle="--", linewidth=1.5,
                    label="Vector search (no filter), reference")
    ax_latency.set_yscale("log")
    ax_latency.set_ylabel("Average latency of completed queries (ms, log)")
    ax_latency.grid(True, which="both", alpha=0.25)
    ax_latency.legend(loc="upper left", frameon=False)

    # --- Bottom panel: share of queries that completed -------------------------
    width = 0.4
    ax_done.bar([i - width / 2 for i in x], merged["no_stats_pct"], width=width,
                color=COLOUR_NO_STATS, label="No statistics")
    ax_done.bar([i + width / 2 for i in x], merged["with_stats_pct"], width=width,
                color=COLOUR_WITH_STATS, label="After ANALYZE")
    ax_done.set_ylim(0, 105)
    ax_done.set_ylabel(f"Completed of {QUERIES_PER_SCENARIO:,} queries (%)")
    ax_done.grid(True, axis="y", alpha=0.25)
    ax_done.legend(loc="lower left", frameon=False)

    # Shared x-axis in the article's notation, rotated to stay readable.
    ax_done.set_xticks(x)
    ax_done.set_xticklabels(merged["label"], rotation=90, fontsize=8)

    fig.suptitle("Filtered search, 2.5M vectors: without filter statistics vs after ANALYZE",
                 fontsize=14, fontweight="bold")
    fig.tight_layout()
    fig.savefig(output_file, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    log.info("plot_saved")


def main() -> None:
    """Read both runs, build the comparison and save the figure.

    Raises:
        SystemExit: If the two configuration labels are not supplied.
    """
    log = logger.bind(task="plot_statistics_comparison")

    # Both labels are required; there is no sensible default.
    if len(sys.argv) != 3:
        log.error("usage", expected='plot_statistics_comparison.py "<config without stats>" "<config with stats>"')
        raise SystemExit(2)
    no_stats_config, with_stats_config = sys.argv[1], sys.argv[2]

    try:
        conninfo = (f"host={RESULTS_PG_HOST} port={RESULTS_PG_PORT} user={RESULTS_PG_USER} "
                    f"password={RESULTS_PG_PASSWORD} dbname={RESULTS_PG_DATABASE} sslmode=require")
        with psycopg.connect(conninfo) as conn:
            no_stats = fetch_run(conn, no_stats_config)
            with_stats = fetch_run(conn, with_stats_config)
        merged = build_comparison(no_stats, with_stats)
        plot(merged, OUTPUT_FILE)
    except Exception:
        log.exception("plot_statistics_comparison_failed")
        raise


if __name__ == "__main__":
    main()
