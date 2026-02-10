#!/usr/bin/env python3
"""
Build canonical experiment assignments (Silver).

Reads:
- data/raw/fact_assignment/dt=YYYY-MM-DD

Writes:
- data/silver/fact_assignment_canonical/dt=YYYY-MM-DD
- data/silver/metrics_assignment_quality/dt=YYYY-MM-DD

Key guarantees:
- exactly one row per (experiment_id, user_id)
- deterministic choice when duplicates exist (earliest assignment_time_utc wins)

Why this job exists:
- In real systems, assignment logs are often duplicated due to retries, fan-out, or bugs.
- Canonicalization makes downstream analysis safe and reproducible.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dt", required=True, help="Partition date, e.g. 2026-02-01")
    p.add_argument("--in", dest="in_path", default="data/raw", help="Base input path (default: data/raw)")
    p.add_argument("--out", dest="out_path", default="data/silver", help="Base output path (default: data/silver)")
    return p.parse_args()


def write_parquet(df, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def main() -> None:
    args = parse_args()
    dt = args.dt
    in_base = args.in_path.rstrip("/")
    out_base = args.out_path.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("experimentation-analytics-platform-build-assignments")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    in_path = f"{in_base}/fact_assignment/dt={dt}"
    raw = spark.read.parquet(in_path)

    # -----------------------------
    # 1) Basic validation of expected columns
    # -----------------------------
    expected_cols = {
        "experiment_id",
        "user_id",
        "variant_id",
        "assignment_time_utc",
        "assignment_source",
        "dt",
    }
    missing = sorted(list(expected_cols - set(raw.columns)))
    if missing:
        raise ValueError(f"Missing required columns in raw assignments: {missing}")

    # -----------------------------
    # 2) Compute duplicate diagnostics BEFORE canonicalization
    # -----------------------------
    # duplicates = count(*) - 1 for each (experiment_id, user_id) where count(*) > 1
    per_key = (
        raw.groupBy("experiment_id", "user_id")
        .agg(F.count(F.lit(1)).alias("rows_per_key"))
    )

    dup_keys = per_key.filter(F.col("rows_per_key") > F.lit(1))

    total_rows = raw.count()
    total_keys = per_key.count()
    duplicate_keys_count = dup_keys.count()

    duplicate_rows_excess = (
        dup_keys.select(F.sum(F.col("rows_per_key") - F.lit(1)).alias("excess")).collect()[0]["excess"]
    )
    if duplicate_rows_excess is None:
        duplicate_rows_excess = 0

    # -----------------------------
    # 3) Canonicalization rule (deterministic)
    # -----------------------------
    # Keep the earliest assignment_time_utc per (experiment_id, user_id).
    # If tie exists, use variant_id as a deterministic tie-breaker.
    w = (
        Window.partitionBy("experiment_id", "user_id")
        .orderBy(F.col("assignment_time_utc").asc(), F.col("variant_id").asc())
    )

    canonical = (
        raw
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == F.lit(1))
        .drop("rn")
    )

    # Sanity check: canonical must have exactly one row per key
    canonical_keys = canonical.groupBy("experiment_id", "user_id").agg(F.count(F.lit(1)).alias("c"))
    bad = canonical_keys.filter(F.col("c") != F.lit(1)).count()
    if bad != 0:
        raise RuntimeError("Canonicalization failed: found keys with count != 1")

    # -----------------------------
    # 4) Write outputs
    # -----------------------------
    out_canonical = f"{out_base}/fact_assignment_canonical/dt={dt}"
    write_parquet(canonical, out_canonical)

    # A small metrics table that later feeds rpt_experiment_quality
    metrics = spark.createDataFrame(
        [
            (
                dt,
                total_rows,
                total_keys,
                int(duplicate_keys_count),
                int(duplicate_rows_excess),
            )
        ],
        [
            "dt",
            "raw_rows",
            "unique_keys",
            "duplicate_keys_count",
            "duplicate_rows_excess",
        ],
    ).withColumn("generated_at_utc", F.current_timestamp())

    out_metrics = f"{out_base}/metrics_assignment_quality/dt={dt}"
    write_parquet(metrics, out_metrics)

    # -----------------------------
    # 5) Print a short operator-friendly summary
    # -----------------------------
    print("✅ Built canonical assignments (Silver)")
    print(f"dt: {dt}")
    print(f"input:  {in_path}")
    print(f"output: {out_canonical}")
    print(f"raw_rows={total_rows} unique_keys={total_keys} dup_keys={duplicate_keys_count} dup_rows_excess={duplicate_rows_excess}")

    spark.stop()


if __name__ == "__main__":
    main()
