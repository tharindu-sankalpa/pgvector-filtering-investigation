"""
Plot high-quality benchmark charts for the Medium article.

Replicates the Streamlit dashboard retrieval tab layout:
  - X-axis: "C:{concurrency} | K:{top_k}" (sorted by concurrency, then top_k)
  - Y-axis: QPS (top) and Avg Latency in ms (bottom)
  - One line per test type (Vector Search, Filtered Search, Hybrid Search)
  - Focused on PostgreSQL CNPG Self-Hosted, 2.5M dataset only

Usage:
    export $(grep -v '^#' .env.azure | xargs)
    uv run python scripts/plot_article_benchmarks.py

Output:
    doc/plots/article_cnpg_retrieval.png
"""

import os
import sys
from pathlib import Path

import psycopg
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

sys.path.append(str(Path(__file__).parent.parent / "src"))
from shared.logger_structlog import setup_structlog

logger = setup_structlog()

OUTPUT_DIR = Path(__file__).parent.parent / "doc" / "plots"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RESULTS_PG_HOST = os.getenv("RESULTS_PG_HOST", os.getenv("PGHOST", ""))
RESULTS_PG_USER = os.getenv("RESULTS_PG_USER", os.getenv("PGUSER", ""))
RESULTS_PG_PASSWORD = os.getenv("RESULTS_PG_PASSWORD", os.getenv("PGPASSWORD", ""))
RESULTS_PG_DATABASE = "benchmark_results"


def fetch_cnpg_results() -> pd.DataFrame:
    """Fetch CNPG 2.5M retrieval results, picking best QPS per scenario if duplicates exist."""
    conninfo = (
        f"host={RESULTS_PG_HOST} "
        f"user={RESULTS_PG_USER} "
        f"password={RESULTS_PG_PASSWORD} "
        f"dbname={RESULTS_PG_DATABASE} "
        f"sslmode=require"
    )
    conn = psycopg.connect(conninfo, autocommit=True)

    # Use DISTINCT ON to pick the row with the highest QPS per scenario
    # in case there are multiple runs for the same (test_type, top_k, concurrency_level)
    query = """
        SELECT DISTINCT ON (test_type, top_k, concurrency_level)
               test_type, top_k, concurrency_level,
               avg_latency_seconds * 1000 AS avg_latency_ms,
               p50_latency_seconds * 1000 AS p50_ms,
               p95_latency_seconds * 1000 AS p95_ms,
               p99_latency_seconds * 1000 AS p99_ms,
               qps
        FROM benchmark_retrieval_summary
        WHERE database_name LIKE '%CNPG%'
          AND dataset_size = 2500000
          AND test_type IN ('Vector Search', 'Filtered Search', 'Hybrid Search')
        ORDER BY test_type, top_k, concurrency_level, qps DESC
    """
    rows = conn.execute(query).fetchall()
    cols = [
        "test_type", "top_k", "concurrency_level",
        "avg_latency_ms", "p50_ms", "p95_ms", "p99_ms", "qps",
    ]
    conn.close()

    df = pd.DataFrame(rows, columns=cols)
    logger.info("cnpg_results_fetched", rows=len(df), test_types=df["test_type"].nunique())
    return df


# Colour palette per test type (matches dashboard aesthetic)
TEST_TYPE_STYLES = {
    "Vector Search":   {"color": "#3B82F6", "marker": "o"},
    "Filtered Search": {"color": "#EF4444", "marker": "s"},
    "Hybrid Search":   {"color": "#10B981", "marker": "D"},
}


def build_x_axis_signature(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create the composite X-axis label used by the Streamlit dashboard:
    "C:{concurrency} | K:{top_k}", sorted by concurrency first then top_k.
    """
    df = df.copy()
    df["sort_concurrency"] = df["concurrency_level"].astype(int)
    df["sort_top_k"] = df["top_k"].astype(int)
    df = df.sort_values(["sort_concurrency", "sort_top_k"])
    df["x_label"] = df.apply(
        lambda r: f"C:{int(r['concurrency_level'])} | K:{int(r['top_k'])}",
        axis=1,
    )
    return df


def plot_dashboard_style(df: pd.DataFrame) -> None:
    """
    Create a two-panel figure (QPS top, Latency bottom) replicating the
    Streamlit dashboard retrieval chart layout.
    """
    fig, (ax_qps, ax_lat) = plt.subplots(
        2, 1, figsize=(22, 14), sharex=True,
        gridspec_kw={"hspace": 0.08},
    )

    # Ordered unique x labels
    x_labels = df["x_label"].unique().tolist()
    x_positions = np.arange(len(x_labels))
    x_label_to_pos = {lbl: pos for pos, lbl in enumerate(x_labels)}

    for test_type, style in TEST_TYPE_STYLES.items():
        subset = df[df["test_type"] == test_type].copy()
        if subset.empty:
            continue

        x_pos = [x_label_to_pos[lbl] for lbl in subset["x_label"]]

        ax_qps.plot(
            x_pos, subset["qps"].values,
            label=f"PostgreSQL CNPG | {test_type} | 2.5M",
            color=style["color"], marker=style["marker"],
            linewidth=2.2, markersize=6, alpha=0.9,
        )
        ax_lat.plot(
            x_pos, subset["avg_latency_ms"].values,
            label=f"PostgreSQL CNPG | {test_type} | 2.5M",
            color=style["color"], marker=style["marker"],
            linewidth=2.2, markersize=6, alpha=0.9,
        )

    # Styling: clean white theme for article
    bg_color = "#ffffff"
    paper_color = "#ffffff"
    text_color = "#1a1a2e"
    grid_color = "#e0e0e0"
    spine_color = "#cccccc"

    for ax, ylabel, title in [
        (ax_qps, "Queries Per Second (QPS)", "QPS"),
        (ax_lat, "Avg Latency (ms)", "Avg Latency (ms)"),
    ]:
        ax.set_facecolor(bg_color)
        ax.set_ylabel(ylabel, fontsize=12, color=text_color, fontweight="bold")
        ax.tick_params(colors=text_color, labelsize=9)
        ax.grid(True, color=grid_color, alpha=0.6, linewidth=0.6)
        for spine in ax.spines.values():
            spine.set_color(spine_color)

        ax.legend(
            fontsize=10, loc="upper left",
            facecolor=bg_color, edgecolor=spine_color,
            labelcolor=text_color, framealpha=0.95,
        )

    # Use log scale on latency to make both low and high values visible
    ax_lat.set_yscale("log")

    # X-axis labels on the bottom panel only
    ax_lat.set_xticks(x_positions)
    ax_lat.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8, color=text_color)
    ax_lat.set_xlabel("Concurrency | Top K", fontsize=12, color=text_color, fontweight="bold")

    fig.patch.set_facecolor(paper_color)

    fig.suptitle(
        "PostgreSQL CNPG Self-Hosted Retrieval Performance (2.5M Vectors, D16s_v3 16vCPU 64GB)",
        fontsize=15, fontweight="bold", color=text_color, y=0.98,
    )

    out_path = OUTPUT_DIR / "article_cnpg_retrieval.png"
    fig.savefig(out_path, dpi=200, bbox_inches="tight", facecolor=paper_color)
    plt.close(fig)
    logger.info("plot_saved", path=str(out_path))


def main() -> None:
    """Fetch data and generate the dashboard-style article plot."""
    logger.info("plot_generation_start")

    df = fetch_cnpg_results()
    df = build_x_axis_signature(df)

    logger.info(
        "data_profile",
        test_types=list(df["test_type"].unique()),
        top_k_values=sorted(df["top_k"].unique().tolist()),
        concurrency_levels=sorted(df["concurrency_level"].unique().tolist()),
        total_rows=len(df),
    )

    plot_dashboard_style(df)

    logger.info("plot_generation_complete", output_dir=str(OUTPUT_DIR))


if __name__ == "__main__":
    main()
