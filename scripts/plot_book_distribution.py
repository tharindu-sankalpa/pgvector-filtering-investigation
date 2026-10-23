"""
Plot the per-book row-count distribution of the 2.5M-row benchmark table.

This is the figure embedded as doc/diagrams/book-filter-distribution.png. It is
drawn with matplotlib rather than an image model because the bars must be exact
and because the image generator rejected a prompt listing all sixteen titles.

Input, one of:
    --csv PATH      CSV with columns book_name,rows (any order; sorted here)
    (no --csv)      Query the vector database with the same GROUP BY used in
                    scripts/perf_audit_pgvector_2.5m_wot.sql

Usage:
    export $(grep -v '^#' .env.aws | xargs)
    uv run python scripts/plot_book_distribution.py
    uv run python scripts/plot_book_distribution.py --csv doc/results/book-row-counts.csv
    uv run python scripts/plot_book_distribution.py --csv ... --output /tmp/x.png

Environment (database mode): VECTOR_PG_HOST / VECTOR_PG_PORT / VECTOR_PG_DATABASE /
VECTOR_PG_USER / VECTOR_PG_PASSWORD, falling back to PG_HOST / PG_PORT / PG_USER /
PG_PASSWORD and database "vector_data", as in src/shared/results_db.py.

Output:
    doc/diagrams/book-filter-distribution.png (default), 3:2 at 200 dpi.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = REPO_ROOT / "doc" / "diagrams" / "book-filter-distribution.png"
TABLE_NAME = os.getenv("TABLE_NAME", "wot_chunks_2_5m")

# Shared palette from doc/diagrams/prompts.md: slate for neutral bars, red for the
# observed trap filter (smallest book), indigo for the largest book.
COLOR_NEUTRAL = "#94A3B8"
COLOR_SMALLEST = "#B91C1C"
COLOR_LARGEST = "#4F46E5"
INK = "#0F172A"
INK_MUTED = "#475569"
RULE = "#CBD5E1"


def load_csv(path: Path) -> list[tuple[str, int]]:
    rows: list[tuple[str, int]] = []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames or not {"book_name", "rows"} <= set(reader.fieldnames):
            sys.exit(f"{path}: expected columns book_name,rows; got {reader.fieldnames}")
        for record in reader:
            rows.append((record["book_name"].strip(), int(record["rows"].replace(",", ""))))
    return rows


def load_database() -> list[tuple[str, int]]:
    import psycopg  # imported lazily so --csv mode needs no driver

    conninfo = (
        f"host={os.getenv('VECTOR_PG_HOST', os.getenv('PG_HOST', ''))} "
        f"port={os.getenv('VECTOR_PG_PORT', os.getenv('PG_PORT', '5432'))} "
        f"dbname={os.getenv('VECTOR_PG_DATABASE', 'vector_data')} "
        f"user={os.getenv('VECTOR_PG_USER', os.getenv('PG_USER', ''))} "
        f"password={os.getenv('VECTOR_PG_PASSWORD', os.getenv('PG_PASSWORD', ''))}"
    )
    query = f"""
        SELECT metadata->>'book_name' AS book_name, count(*) AS rows
        FROM {TABLE_NAME}
        GROUP BY metadata->>'book_name'
        ORDER BY 1
    """
    with psycopg.connect(conninfo) as conn:
        return [(name, int(count)) for name, count in conn.execute(query)]


def plot(rows: list[tuple[str, int]], output: Path) -> None:
    rows = sorted(rows, key=lambda r: r[0])  # book order: the names carry a numeric prefix
    names = [r[0] for r in rows]
    counts = [r[1] for r in rows]
    total = sum(counts)
    i_min = counts.index(min(counts))
    i_max = counts.index(max(counts))

    colors = [COLOR_NEUTRAL] * len(rows)
    colors[i_min] = COLOR_SMALLEST
    colors[i_max] = COLOR_LARGEST

    from matplotlib import font_manager

    installed = {f.name for f in font_manager.fontManager.ttflist}
    family = ["Inter"] if "Inter" in installed else ["DejaVu Sans", "sans-serif"]
    plt.rcParams.update({
        "font.family": family,
        "axes.edgecolor": RULE,
        "xtick.color": INK_MUTED,
        "ytick.color": INK,
    })

    fig, ax = plt.subplots(figsize=(12, 8), dpi=200)
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    y = list(range(len(rows)))[::-1]  # first book at the top
    ax.barh(y, counts, color=colors, height=0.62, zorder=3)

    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=11, color=INK)
    ax.tick_params(axis="y", length=0)
    ax.tick_params(axis="x", labelsize=10)

    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v/1000)}k" if v else "0"))
    ax.set_xlim(0, max(counts) * 1.28)
    ax.set_xlabel("Rows per book_name value", fontsize=11, color=INK_MUTED, labelpad=10)
    ax.grid(axis="x", color=RULE, linewidth=0.6, zorder=0)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)

    pad = max(counts) * 0.012
    for yi, count, idx in zip(y, counts, range(len(rows))):
        share = count / total * 100
        if idx == i_min:
            label, color, weight = f"{count:,} rows · {share:.1f}% · most selective", COLOR_SMALLEST, "semibold"
        elif idx == i_max:
            label, color, weight = f"{count:,} rows · {share:.1f}% · least selective", COLOR_LARGEST, "semibold"
        else:
            label, color, weight = f"{count:,}", INK_MUTED, "normal"
        ax.text(count + pad, yi, label, va="center", ha="left", fontsize=10, color=color, fontweight=weight)

    fig.text(
        0.01, 0.005,
        f"{len(rows)} distinct values · {total:,} rows total · uneven by design",
        fontsize=10, color=INK_MUTED, ha="left", va="bottom",
    )

    fig.tight_layout(rect=(0, 0.03, 1, 1))
    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, dpi=200, facecolor="white")
    plt.close(fig)
    print(f"wrote {output} ({len(rows)} books, {total:,} rows)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--csv", type=Path, help="CSV with columns book_name,rows instead of querying the database")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help=f"output PNG (default: {DEFAULT_OUTPUT})")
    args = parser.parse_args()

    rows = load_csv(args.csv) if args.csv else load_database()
    if not rows:
        sys.exit("no rows returned")
    plot(rows, args.output)


if __name__ == "__main__":
    main()
