"""
ef_search / ef Parameter Verification Experiment

Verifies the behavior of the HNSW ef_search parameter in both pgvector (CNPG)
and Milvus self-hosted distributed to determine whether result clipping occurs
when top_k exceeds the ef_search / ef value.

Hypothesis:
    pgvector defaults to ef_search=40. When LIMIT > 40, only 40 rows are
    returned regardless of LIMIT.  Milvus uses ef=max(64, top_k), which
    ensures ef >= top_k, so results are never clipped.

Experiment design:
    Part A — pgvector:
        For each ef_search in [40, 64, 100, 200, 400]:
            For each top_k in [1, 10, 20, 40, 50, 80, 100, 200]:
                SET hnsw.ef_search = <ef_search>
                Run vector search with LIMIT <top_k>
                Record: rows_returned, latency_ms

    Part B — Milvus:
        For each ef in [40, 64, 100, 200, 400]:
            For each top_k in [1, 10, 20, 40, 50, 80, 100, 200]:
                search with ef=<ef>, limit=<top_k>
                Record: rows_returned, latency_ms
                (Milvus rejects ef < top_k, so those combos will error)

    Part C — Summary table and CSV export

Usage:
    export $(grep -v '^#' .env.azure | xargs)
    uv run python scripts/ef_search_verification_experiment.py

    # pgvector only
    uv run python scripts/ef_search_verification_experiment.py --pgvector-only

    # Milvus only
    uv run python scripts/ef_search_verification_experiment.py --milvus-only
"""

import argparse
import csv
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import numpy as np

sys.path.append(str(Path(__file__).parent.parent / "src"))
from shared.logger_structlog import setup_structlog

logger = setup_structlog()


# =============================================================================
# Experiment Configuration
# =============================================================================

EF_SEARCH_VALUES = [40, 64, 100, 200, 400]
TOP_K_VALUES = [1, 10, 20, 40, 50, 80, 100, 200]
REPEAT_COUNT = 3  # repeat each combo this many times and average


@dataclass
class ExperimentResult:
    """Single experiment measurement."""

    system: str
    ef_value: int
    top_k: int
    rows_returned: int
    latency_ms: float
    clipped: bool
    error: Optional[str] = None


# =============================================================================
# Part A — pgvector (CNPG)
# =============================================================================

PG_HOST = os.getenv("CNPG_PG_HOST", os.getenv("PG_HOST", ""))
PG_PORT = int(os.getenv("CNPG_PG_PORT", os.getenv("PG_PORT", "5432")))
PG_DATABASE = os.getenv("VECTOR_PG_DATABASE", "benchmark_vectors")
PG_USER = os.getenv("CNPG_PG_USER", os.getenv("PG_USER", ""))
PG_PASSWORD = os.getenv("CNPG_PG_PASSWORD", os.getenv("PG_PASSWORD", ""))
PG_TABLE = os.getenv("TABLE_NAME", "wot_chunks_2_5m")
PG_SSLMODE = os.getenv("PGSSLMODE", "require")


def pgvector_get_connection():
    """
    Create a psycopg3 connection to the CNPG cluster with vector type
    registered, ready for pgvector queries.

    Returns:
        psycopg.Connection with vector type registered.
    """
    import psycopg
    from pgvector.psycopg import register_vector

    conn = psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD,
        sslmode=PG_SSLMODE,
        autocommit=True,
    )
    register_vector(conn)
    return conn


def pgvector_fetch_sample_embedding(conn) -> np.ndarray:
    """
    Fetch a single real embedding from the table to use as the query vector.

    Returns:
        np.ndarray: 1536-dimensional embedding vector.
    """
    row = conn.execute(
        f"SELECT embedding FROM {PG_TABLE} LIMIT 1"
    ).fetchone()
    if row is None:
        raise RuntimeError(f"No rows in {PG_TABLE}")
    return np.array(row[0])


def pgvector_run_experiment(conn, embedding: np.ndarray) -> list[ExperimentResult]:
    """
    Run the full ef_search × top_k matrix on pgvector.

    For each combination, issues SET hnsw.ef_search, then a vector similarity
    query, counts the rows actually returned, and measures wall-clock latency.

    Args:
        conn: psycopg connection with vector type registered.
        embedding: Query vector to search with.

    Returns:
        list[ExperimentResult]: One result per (ef_search, top_k, repeat).
    """
    results: list[ExperimentResult] = []
    log = logger.bind(system="pgvector")

    # Warmup: run a few queries to load index pages into shared_buffers
    log.info("warmup_start", queries=10)
    for _ in range(10):
        conn.execute(
            f"SELECT id FROM {PG_TABLE} ORDER BY embedding <=> %s LIMIT 10",
            (embedding,),
        )
    log.info("warmup_complete")

    for ef_search in EF_SEARCH_VALUES:
        conn.execute(f"SET hnsw.ef_search = {ef_search}")
        log.info("ef_search_set", ef_search=ef_search)

        for top_k in TOP_K_VALUES:
            latencies = []
            rows_counts = []

            for rep in range(REPEAT_COUNT):
                t0 = time.perf_counter()
                rows = conn.execute(
                    f"""
                    SELECT id, 1 - (embedding <=> %s) AS similarity
                    FROM {PG_TABLE}
                    ORDER BY embedding <=> %s
                    LIMIT {top_k}
                    """,
                    (embedding, embedding),
                ).fetchall()
                elapsed_ms = (time.perf_counter() - t0) * 1000

                latencies.append(elapsed_ms)
                rows_counts.append(len(rows))

            avg_latency = sum(latencies) / len(latencies)
            avg_rows = int(round(sum(rows_counts) / len(rows_counts)))
            is_clipped = avg_rows < top_k

            result = ExperimentResult(
                system="pgvector",
                ef_value=ef_search,
                top_k=top_k,
                rows_returned=avg_rows,
                latency_ms=round(avg_latency, 2),
                clipped=is_clipped,
            )
            results.append(result)

            log.info(
                "pgvector_measurement",
                ef_search=ef_search,
                top_k=top_k,
                rows_returned=avg_rows,
                latency_ms=round(avg_latency, 2),
                clipped=is_clipped,
            )

    return results


# =============================================================================
# Part B — Milvus Self-Hosted Distributed
# =============================================================================

MILVUS_HOST = os.getenv("MILVUS_AKS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_AKS_PORT", "19530"))
MILVUS_COLLECTION = os.getenv("COLLECTION_NAME", "wot_chunks_2_5m_dist")


def milvus_get_client():
    """
    Create a synchronous MilvusClient connected to the self-hosted cluster.

    Returns:
        MilvusClient instance.
    """
    from pymilvus import MilvusClient

    uri = f"http://{MILVUS_HOST}:{MILVUS_PORT}"
    client = MilvusClient(uri=uri)
    logger.info("milvus_client_created", host=MILVUS_HOST, port=MILVUS_PORT)
    return client


def milvus_fetch_sample_embedding(client) -> list[float]:
    """
    Fetch a single real embedding from the collection to use as query vector.

    Args:
        client: MilvusClient instance.

    Returns:
        list[float]: 1536-dimensional embedding.
    """
    rows = client.query(
        collection_name=MILVUS_COLLECTION,
        filter="id > 0",
        output_fields=["embedding"],
        limit=1,
    )
    if not rows:
        raise RuntimeError(f"No rows in collection {MILVUS_COLLECTION}")
    return rows[0]["embedding"]


def milvus_run_experiment(client, embedding: list[float]) -> list[ExperimentResult]:
    """
    Run the full ef × top_k matrix on Milvus.

    Milvus enforces ef >= top_k for HNSW. Combinations where ef < top_k will
    error — we capture those as clipped=True with rows_returned=0.

    Args:
        client: MilvusClient instance.
        embedding: Query vector to search with.

    Returns:
        list[ExperimentResult]: One result per (ef, top_k, repeat).
    """
    results: list[ExperimentResult] = []
    log = logger.bind(system="milvus")

    # Warmup
    log.info("warmup_start", queries=10)
    for _ in range(10):
        client.search(
            collection_name=MILVUS_COLLECTION,
            data=[embedding],
            anns_field="embedding",
            search_params={"metric_type": "COSINE", "params": {"ef": 64}},
            limit=10,
            output_fields=["id"],
        )
    log.info("warmup_complete")

    for ef in EF_SEARCH_VALUES:
        log.info("ef_set", ef=ef)

        for top_k in TOP_K_VALUES:
            latencies = []
            rows_counts = []
            error_msg = None

            for rep in range(REPEAT_COUNT):
                try:
                    t0 = time.perf_counter()
                    hits = client.search(
                        collection_name=MILVUS_COLLECTION,
                        data=[embedding],
                        anns_field="embedding",
                        search_params={
                            "metric_type": "COSINE",
                            "params": {"ef": ef},
                        },
                        limit=top_k,
                        output_fields=["id"],
                    )
                    elapsed_ms = (time.perf_counter() - t0) * 1000

                    # hits is a list of list of dicts (one per query vector)
                    num_results = len(hits[0]) if hits else 0
                    latencies.append(elapsed_ms)
                    rows_counts.append(num_results)
                except Exception as e:
                    error_msg = str(e)
                    log.warning(
                        "milvus_search_error",
                        ef=ef,
                        top_k=top_k,
                        error=error_msg,
                    )
                    latencies.append(0.0)
                    rows_counts.append(0)

            avg_latency = sum(latencies) / len(latencies) if latencies else 0
            avg_rows = int(round(sum(rows_counts) / len(rows_counts))) if rows_counts else 0
            is_clipped = avg_rows < top_k

            result = ExperimentResult(
                system="milvus",
                ef_value=ef,
                top_k=top_k,
                rows_returned=avg_rows,
                latency_ms=round(avg_latency, 2),
                clipped=is_clipped,
                error=error_msg,
            )
            results.append(result)

            log.info(
                "milvus_measurement",
                ef=ef,
                top_k=top_k,
                rows_returned=avg_rows,
                latency_ms=round(avg_latency, 2),
                clipped=is_clipped,
                error=error_msg,
            )

    return results


# =============================================================================
# Part C — Summary & Export
# =============================================================================

def print_summary_table(results: list[ExperimentResult]) -> None:
    """
    Print a formatted summary table to the log, grouped by system.

    Highlights clipped rows with a [CLIPPED] or [ERROR] marker.

    Args:
        results: All experiment results to summarise.
    """
    header = f"{'System':<10} {'ef':>5} {'top_k':>6} {'rows':>6} {'lat_ms':>10} {'status':<12}"
    separator = "-" * len(header)

    logger.info("experiment_summary_start")
    logger.info("summary_header", header=header)
    logger.info("summary_separator", line=separator)

    for r in results:
        if r.error:
            status = "[ERROR]"
        elif r.clipped:
            status = "[CLIPPED]"
        else:
            status = "OK"

        line = f"{r.system:<10} {r.ef_value:>5} {r.top_k:>6} {r.rows_returned:>6} {r.latency_ms:>10.2f} {status:<12}"
        logger.info("summary_row", line=line)


def export_csv(results: list[ExperimentResult], path: str) -> None:
    """
    Export results to a CSV file for further analysis or plotting.

    Args:
        results: All experiment results.
        path: Output file path.
    """
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "system", "ef_value", "top_k", "rows_returned",
            "latency_ms", "clipped", "error",
        ])
        for r in results:
            writer.writerow([
                r.system, r.ef_value, r.top_k, r.rows_returned,
                r.latency_ms, r.clipped, r.error or "",
            ])

    logger.info("csv_exported", path=path, total_rows=len(results))


def print_clipping_analysis(results: list[ExperimentResult]) -> None:
    """
    Print a focused analysis showing only the cases where clipping occurs.

    This directly answers the question: "At what ef/top_k combinations do
    we lose results?"

    Args:
        results: All experiment results.
    """
    logger.info("clipping_analysis_start")

    for system in ["pgvector", "milvus"]:
        system_results = [r for r in results if r.system == system]
        clipped = [r for r in system_results if r.clipped]

        logger.info(
            "clipping_summary",
            system=system,
            total_combos=len(system_results),
            clipped_combos=len(clipped),
            clipping_rate_pct=round(100 * len(clipped) / max(len(system_results), 1), 1),
        )

        if clipped:
            for r in clipped:
                logger.warning(
                    "clipped_result",
                    system=system,
                    ef=r.ef_value,
                    top_k=r.top_k,
                    rows_returned=r.rows_returned,
                    rows_lost=r.top_k - r.rows_returned,
                    error=r.error,
                )


def print_latency_impact_analysis(results: list[ExperimentResult]) -> None:
    """
    Analyse how increasing ef impacts latency for a fixed top_k.

    For each system and a selection of top_k values, shows the latency at
    each ef level.  This answers: "What is the cost of raising ef?"

    Args:
        results: All experiment results.
    """
    logger.info("latency_impact_analysis_start")

    focus_top_ks = [10, 50, 100, 200]

    for system in ["pgvector", "milvus"]:
        for top_k in focus_top_ks:
            entries = [
                r for r in results
                if r.system == system and r.top_k == top_k and r.error is None
            ]
            if not entries:
                continue

            baseline = entries[0].latency_ms if entries else 0
            for r in entries:
                speedup = r.latency_ms / baseline if baseline > 0 else 0
                logger.info(
                    "latency_vs_ef",
                    system=system,
                    top_k=top_k,
                    ef=r.ef_value,
                    latency_ms=r.latency_ms,
                    rows=r.rows_returned,
                    relative_to_baseline=round(speedup, 2),
                )


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    """
    Entry point.  Runs pgvector and/or Milvus experiments based on CLI flags,
    prints summary tables, clipping analysis, latency impact analysis, and
    exports results to CSV.
    """
    parser = argparse.ArgumentParser(description="ef_search verification experiment")
    parser.add_argument("--pgvector-only", action="store_true", help="Run pgvector experiments only")
    parser.add_argument("--milvus-only", action="store_true", help="Run Milvus experiments only")
    args = parser.parse_args()

    run_pgvector = not args.milvus_only
    run_milvus = not args.pgvector_only

    all_results: list[ExperimentResult] = []

    logger.info(
        "experiment_config",
        ef_values=EF_SEARCH_VALUES,
        top_k_values=TOP_K_VALUES,
        repeats=REPEAT_COUNT,
        run_pgvector=run_pgvector,
        run_milvus=run_milvus,
    )

    # ── Part A: pgvector ──
    if run_pgvector:
        logger.info("pgvector_experiment_start")
        try:
            conn = pgvector_get_connection()
            embedding = pgvector_fetch_sample_embedding(conn)
            logger.info(
                "pgvector_connected",
                host=PG_HOST,
                database=PG_DATABASE,
                table=PG_TABLE,
                embedding_dim=len(embedding),
            )

            # Show current default ef_search
            current_ef = conn.execute("SHOW hnsw.ef_search").fetchone()
            logger.info("pgvector_current_default_ef_search", value=current_ef[0] if current_ef else "unknown")

            pg_results = pgvector_run_experiment(conn, embedding)
            all_results.extend(pg_results)
            conn.close()
            logger.info("pgvector_experiment_complete", measurements=len(pg_results))
        except Exception:
            logger.exception("pgvector_experiment_failed")

    # ── Part B: Milvus ──
    if run_milvus:
        logger.info("milvus_experiment_start")
        try:
            client = milvus_get_client()
            embedding = milvus_fetch_sample_embedding(client)
            logger.info(
                "milvus_connected",
                host=MILVUS_HOST,
                collection=MILVUS_COLLECTION,
                embedding_dim=len(embedding),
            )

            mv_results = milvus_run_experiment(client, embedding)
            all_results.extend(mv_results)
            client.close()
            logger.info("milvus_experiment_complete", measurements=len(mv_results))
        except Exception:
            logger.exception("milvus_experiment_failed")

    # ── Part C: Analysis ──
    if all_results:
        print_summary_table(all_results)
        print_clipping_analysis(all_results)
        print_latency_impact_analysis(all_results)

        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)
        csv_path = str(output_dir / "ef_search_verification_results.csv")
        export_csv(all_results, csv_path)

    logger.info("experiment_finished", total_measurements=len(all_results))


if __name__ == "__main__":
    main()
