#!/usr/bin/env python3
"""
Build exposure validation + quality metrics (Silver/Gold).

Reads:
- data/silver/fact_assignment_canonical/dt=YYYY-MM-DD
- data/raw/fact_exposure/dt=YYYY-MM-DD

Writes:
- data/silver/int_experiment_exposures_deduped/dt=YYYY-MM-DD
- data/silver/int_experiment_exposure_validation/dt=YYYY-MM-DD
- data/gold/fct_experiment_quality_metrics_daily/dt=YYYY-MM-DD

Key guarantees:
- exactly one row per (experiment_id, user_id) in exposure validation
- deterministic dedupe of exposure events
- exposure timing and variant integrity are auditable
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dt", required=True, help="Partition date, e.g. 2026-02-01")
    p.add_argument("--in", dest="in_path", default="data/raw", help="Base input path (default: data/raw)")
    p.add_argument("--silver", dest="silver_path", default="data/silver", help="Base silver path (default: data/silver)")
    p.add_argument("--gold", dest="gold_path", default="data/gold", help="Base gold path (default: data/gold)")
    p.add_argument("--max_days_after_assignment", type=int, default=7, help="Max exposure window after assignment")
    p.add_argument("--pre_assignment_grace_minutes", type=int, default=5, help="Grace window before assignment")
    return p.parse_args()


def write_parquet(df, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def require_columns(df, expected_cols: set[str], df_name: str) -> None:
    missing = sorted(list(expected_cols - set(df.columns)))
    if missing:
        raise ValueError(f"Missing required columns in {df_name}: {missing}")


def main() -> None:
    args = parse_args()
    dt = args.dt
    in_base = args.in_path.rstrip("/")
    silver_base = args.silver_path.rstrip("/")
    gold_base = args.gold_path.rstrip("/")

    spark = (
        SparkSession.builder
        .appName("experimentation-analytics-platform-exposure-validation")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    assignments_path = f"{silver_base}/fact_assignment_canonical/dt={dt}"
    exposures_path = f"{in_base}/fact_exposure/dt={dt}"

    assignments = spark.read.parquet(assignments_path)
    exposures_raw = spark.read.parquet(exposures_path)

    require_columns(
        assignments,
        {"experiment_id", "user_id", "variant_id", "assignment_time_utc", "dt"},
        "canonical assignments",
    )
    require_columns(
        exposures_raw,
        {"experiment_id", "user_id", "variant_id", "exposure_time_utc", "exposure_event_type", "dt"},
        "raw exposures",
    )

    # -----------------------------
    # 1) Staging: standardize exposure events
    # -----------------------------
    exposures = (
        exposures_raw
        .select(
            "experiment_id",
            "user_id",
            "variant_id",
            "exposure_time_utc",
            "exposure_event_type",
            "dt",
        )
        .withColumn(
            "exposure_event_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("experiment_id"),
                    F.col("user_id"),
                    F.col("variant_id"),
                    F.col("exposure_time_utc").cast("string"),
                    F.col("exposure_event_type"),
                ),
                256,
            ),
        )
        .filter(F.col("experiment_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("exposure_time_utc").isNotNull())
        .filter(F.col("variant_id").isNotNull())
    )

    # -----------------------------
    # 2) Dedupe exposures deterministically
    # -----------------------------
    w_dedupe = (
        Window.partitionBy("experiment_id", "user_id", "variant_id")
        .orderBy(F.col("exposure_time_utc").asc(), F.col("exposure_event_id").asc())
    )
    exposures_deduped = (
        exposures
        .withColumn("rn", F.row_number().over(w_dedupe))
        .filter(F.col("rn") == F.lit(1))
        .drop("rn")
    )

    out_exposures = f"{silver_base}/int_experiment_exposures_deduped/dt={dt}"
    write_parquet(exposures_deduped, out_exposures)

    # -----------------------------
    # 3) Exposure validation table
    # -----------------------------
    exposure_stats = (
        exposures_deduped
        .groupBy("experiment_id", "user_id")
        .agg(
            F.count(F.lit(1)).alias("exposure_event_count"),
            F.countDistinct("variant_id").alias("exposure_variation_count"),
        )
    )

    joined = (
        assignments.alias("a")
        .join(exposures_deduped.alias("e"), ["experiment_id", "user_id"], "left")
        .join(exposure_stats.alias("s"), ["experiment_id", "user_id"], "left")
        .select(
            F.col("a.experiment_id").alias("experiment_id"),
            F.col("a.user_id").alias("user_id"),
            F.col("a.variant_id").alias("assigned_variant_id"),
            F.col("a.assignment_time_utc").alias("assigned_at"),
            F.col("e.exposure_time_utc").alias("exposure_at"),
            F.col("e.variant_id").alias("exposure_variant_id"),
            F.col("e.exposure_event_id").alias("exposure_event_id"),
            F.col("s.exposure_event_count").alias("exposure_event_count"),
            F.col("s.exposure_variation_count").alias("exposure_variation_count"),
            F.col("a.dt").alias("dt"),
        )
    )

    w_first = (
        Window.partitionBy("experiment_id", "user_id")
        .orderBy(F.col("exposure_at").asc(), F.col("exposure_event_id").asc())
    )

    first_exposure = (
        joined
        .withColumn("rn_first_exposure_any", F.row_number().over(w_first))
        .filter(F.col("rn_first_exposure_any") == F.lit(1))
        .drop("rn_first_exposure_any")
        .withColumn("has_any_exposure", F.col("exposure_at").isNotNull())
        .withColumn("first_exposure_at", F.col("exposure_at"))
        .withColumn("first_exposure_variant_id", F.col("exposure_variant_id"))
        .withColumn("first_exposure_event_id", F.col("exposure_event_id"))
        .drop("exposure_at", "exposure_variant_id", "exposure_event_id")
    )

    grace_cutoff = F.expr(f"assigned_at - INTERVAL {args.pre_assignment_grace_minutes} MINUTES")
    window_end = F.expr(f"assigned_at + INTERVAL {args.max_days_after_assignment} DAYS")

    validation = (
        first_exposure
        .withColumn(
            "exposure_delay_seconds",
            F.when(
                F.col("has_any_exposure"),
                F.col("first_exposure_at").cast("long") - F.col("assigned_at").cast("long"),
            ).otherwise(F.lit(None).cast("long")),
        )
        .withColumn(
            "is_pre_assignment_exposure",
            F.when(
                F.col("has_any_exposure") & (F.col("first_exposure_at") < grace_cutoff),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "exposure_outside_window",
            F.when(
                F.col("has_any_exposure") & (F.col("first_exposure_at") > window_end),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "is_variant_mismatch",
            F.when(
                F.col("has_any_exposure") & (F.col("first_exposure_variant_id") != F.col("assigned_variant_id")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "has_multiple_variations_exposed",
            F.when(F.col("exposure_variation_count") > F.lit(1), F.lit(True)).otherwise(F.lit(False)),
        )
        .withColumn(
            "has_valid_exposure",
            F.when(
                F.col("has_any_exposure")
                & (~F.col("is_pre_assignment_exposure"))
                & (~F.col("exposure_outside_window"))
                & (~F.col("is_variant_mismatch"))
                & (~F.col("has_multiple_variations_exposed")),
                F.lit(True),
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "validation_status",
            F.when(F.col("has_any_exposure") == F.lit(False), F.lit("no_exposure"))
            .when(F.col("has_multiple_variations_exposed") == F.lit(True), F.lit("multi_variation_exposure"))
            .when(F.col("is_pre_assignment_exposure") == F.lit(True), F.lit("pre_assignment_exposure"))
            .when(F.col("exposure_outside_window") == F.lit(True), F.lit("exposure_outside_window"))
            .when(F.col("is_variant_mismatch") == F.lit(True), F.lit("variant_mismatch"))
            .when(F.col("has_valid_exposure") == F.lit(True), F.lit("valid"))
            .otherwise(F.lit("invalid_other")),
        )
    )

    out_validation = f"{silver_base}/int_experiment_exposure_validation/dt={dt}"
    write_parquet(validation, out_validation)

    # -----------------------------
    # 4) Daily quality metrics
    # -----------------------------
    v = validation.withColumn("date_day", F.to_date("assigned_at"))

    daily = (
        v.groupBy("experiment_id", "date_day")
        .agg(
            F.count(F.lit(1)).alias("assigned_units"),
            F.sum(F.when(F.col("has_any_exposure"), F.lit(1)).otherwise(F.lit(0))).alias("exposed_units"),
            F.sum(F.when(F.col("has_valid_exposure"), F.lit(1)).otherwise(F.lit(0))).alias("valid_exposed_units"),
            F.sum(F.when(~F.col("has_any_exposure"), F.lit(1)).otherwise(F.lit(0))).alias("missing_exposure_units"),
            F.sum(F.when(F.col("is_variant_mismatch"), F.lit(1)).otherwise(F.lit(0))).alias("variant_mismatch_units"),
            F.sum(F.when(F.col("has_multiple_variations_exposed"), F.lit(1)).otherwise(F.lit(0))).alias("multi_variation_units"),
            F.sum(F.when(F.col("is_pre_assignment_exposure"), F.lit(1)).otherwise(F.lit(0))).alias("pre_assignment_units"),
            F.sum(F.when(F.col("exposure_outside_window"), F.lit(1)).otherwise(F.lit(0))).alias("outside_window_units"),
            F.avg(F.when(F.col("has_any_exposure"), F.col("exposure_delay_seconds"))).alias(
                "avg_exposure_delay_seconds"
            ),
        )
    )

    daily = (
        daily
        .withColumn(
            "exposure_rate",
            F.when(F.col("assigned_units") == 0, F.lit(None).cast("double"))
            .otherwise(F.col("exposed_units").cast("double") / F.col("assigned_units").cast("double")),
        )
        .withColumn(
            "valid_exposure_rate",
            F.when(F.col("assigned_units") == 0, F.lit(None).cast("double"))
            .otherwise(F.col("valid_exposed_units").cast("double") / F.col("assigned_units").cast("double")),
        )
        .withColumn(
            "mismatch_rate",
            F.when(F.col("exposed_units") == 0, F.lit(None).cast("double"))
            .otherwise(F.col("variant_mismatch_units").cast("double") / F.col("exposed_units").cast("double")),
        )
        .withColumn(
            "percent_missing_exposure",
            F.when(F.col("assigned_units") == 0, F.lit(None).cast("double"))
            .otherwise(F.col("missing_exposure_units").cast("double") / F.col("assigned_units").cast("double")),
        )
        .withColumn(
            "percent_multiple_exposures",
            F.when(F.col("assigned_units") == 0, F.lit(None).cast("double"))
            .otherwise(F.col("multi_variation_units").cast("double") / F.col("assigned_units").cast("double")),
        )
        .withColumn(
            "pre_assignment_rate",
            F.when(F.col("assigned_units") == 0, F.lit(None).cast("double"))
            .otherwise(F.col("pre_assignment_units").cast("double") / F.col("assigned_units").cast("double")),
        )
        .withColumn(
            "outside_window_rate",
            F.when(F.col("assigned_units") == 0, F.lit(None).cast("double"))
            .otherwise(F.col("outside_window_units").cast("double") / F.col("assigned_units").cast("double")),
        )
        .withColumn("generated_at_utc", F.current_timestamp())
        .withColumn("dt", F.lit(dt))
    )

    out_quality = f"{gold_base}/fct_experiment_quality_metrics_daily/dt={dt}"
    write_parquet(daily, out_quality)

    print("✅ Built exposure validation + quality metrics")
    print(f"dt: {dt}")
    print(f"assignments: {assignments_path}")
    print(f"exposures: {exposures_path}")
    print(f"validation: {out_validation}")
    print(f"quality: {out_quality}")

    spark.stop()


if __name__ == "__main__":
    main()
