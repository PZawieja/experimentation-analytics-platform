#!/usr/bin/env python3
"""
Generate synthetic experimentation data (assignments, exposures, events) for a given dt.

Outputs (Parquet):
- data/raw/dim_experiment/dt=YYYY-MM-DD/
- data/raw/dim_experiment_variant/dt=YYYY-MM-DD/
- data/raw/fact_assignment/dt=YYYY-MM-DD/
- data/raw/fact_exposure/dt=YYYY-MM-DD/
- data/raw/fact_event/dt=YYYY-MM-DD/

Design goals:
- deterministic (seeded) generation
- user-level experiments
- explicit assignment vs exposure separation
- ability to intentionally "break" SRM for demo purposes
"""

import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dt", required=True, help="Partition date, e.g. 2026-02-01")
    p.add_argument("--out", default="data/raw", help="Base output path (default: data/raw)")

    p.add_argument("--users", type=int, default=50_000, help="Number of users to simulate")
    p.add_argument("--experiments", type=int, default=3, help="Number of experiments to simulate")

    # membership & behavior rates
    p.add_argument("--assignment_rate", type=float, default=0.30, help="Share of users assigned per experiment (0..1)")
    p.add_argument("--exposure_rate", type=float, default=0.80, help="Share of assigned users who get exposed (0..1)")
    p.add_argument("--conversion_rate", type=float, default=0.03, help="Base conversion probability (0..1)")
    p.add_argument("--treatment_lift", type=float, default=0.15, help="Relative lift for treatment (e.g. 0.15 = +15%)")

    # SRM demo
    p.add_argument(
        "--srm_break",
        action="store_true",
        help="If set, generate biased allocations (e.g. ~60/40 instead of 50/50) to demonstrate SRM failure",
    )

    p.add_argument("--seed", type=int, default=42, help="Random seed for deterministic runs")
    return p.parse_args()


def dt_to_ts(dt_str: str) -> datetime:
    # treat dt as local day boundary (UTC for simplicity)
    return datetime.strptime(dt_str, "%Y-%m-%d")


def write_parquet(df, path: str) -> None:
    (
        df.write.mode("overwrite")
        .parquet(path)
    )


def main() -> None:
    args = parse_args()
    dt_str = args.dt
    base_out = args.out.rstrip("/")

    dt_start = dt_to_ts(dt_str)
    dt_end = dt_start + timedelta(days=1)

    spark = (
        SparkSession.builder
        .appName("experimentation-analytics-platform-generate-data")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------
    # 1) Users
    # -----------------------------
    # user_id as stable string keys
    users = (
        spark.range(0, args.users)
        .select(F.concat(F.lit("u_"), F.col("id").cast("string")).alias("user_id"))
    )

    # -----------------------------
    # 2) Experiments + variants
    # -----------------------------
    dim_experiment = (
        spark.range(0, args.experiments)
        .select(
            F.concat(F.lit("exp_"), F.col("id").cast("string")).alias("experiment_id"),
            F.concat(F.lit("Experiment "), F.col("id").cast("string")).alias("experiment_name"),
            F.lit("growth_analytics").alias("owner_team"),
            F.lit("user").alias("unit_type"),
            F.lit(dt_start.isoformat()).alias("start_time_utc"),
            F.lit(None).cast("string").alias("end_time_utc"),
            F.lit("running").alias("status"),
            F.current_timestamp().alias("created_at_utc"),
            F.lit(dt_str).alias("dt"),
        )
    )

    # Two-variant experiments: control/treatment
    # expected allocation always 50/50; SRM check compares to observed
    dim_experiment_variant = (
        dim_experiment.select("experiment_id", "dt")
        .crossJoin(
            spark.createDataFrame(
                [("control", "Control", 50.0, True), ("treatment", "Treatment", 50.0, False)],
                ["variant_id", "variant_name", "allocation_pct", "is_control"],
            )
        )
        .select(
            "experiment_id",
            "variant_id",
            "variant_name",
            "allocation_pct",
            "is_control",
            "dt",
        )
    )

    # -----------------------------
    # 3) Assignments
    # -----------------------------
    # Decide experiment membership: each user has probability assignment_rate per experiment
    # Then assign variant. If --srm_break, bias allocation ~60/40 for demo.
    exp_ids = [f"exp_{i}" for i in range(args.experiments)]
    exp_df = spark.createDataFrame([(e,) for e in exp_ids], ["experiment_id"])

    # Candidate rows: user x experiment
    candidates = users.crossJoin(exp_df)

    # Membership
    candidates = candidates.withColumn(
        "is_assigned",
        (F.rand(args.seed) < F.lit(float(args.assignment_rate))).cast("boolean")
    ).filter(F.col("is_assigned") == F.lit(True)).drop("is_assigned")

    # Variant assignment
    if args.srm_break:
        # ~60/40 split: control 60%, treatment 40% (intentionally wrong vs expected 50/50)
        candidates = candidates.withColumn(
            "variant_id",
            F.when(F.rand(args.seed + 1) < F.lit(0.60), F.lit("control")).otherwise(F.lit("treatment"))
        )
    else:
        # 50/50 split
        candidates = candidates.withColumn(
            "variant_id",
            F.when(F.rand(args.seed + 1) < F.lit(0.50), F.lit("control")).otherwise(F.lit("treatment"))
        )

    # Assignment time within the day
    # Note: use seconds offset into the day for realism
    candidates = candidates.withColumn(
        "assignment_time_utc",
        F.expr(f"timestampadd(SECOND, cast(rand({args.seed + 2}) * 86399 as int), timestamp'{dt_start.isoformat()}')")
    )

    fact_assignment = (
        candidates
        .select(
            "experiment_id",
            "user_id",
            "variant_id",
            "assignment_time_utc",
        )
        .withColumn("assignment_source", F.lit("feature_flag"))
        .withColumn("dt", F.lit(dt_str))
    )

    # Enforce uniqueness (safety): if duplicates ever appear, keep earliest assignment
    w = Window.partitionBy("experiment_id", "user_id").orderBy(F.col("assignment_time_utc").asc())
    fact_assignment = (
        fact_assignment
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == F.lit(1))
        .drop("rn")
    )

    # -----------------------------
    # 4) Exposures
    # -----------------------------
    # Only some assigned users get exposed
    fact_exposure = (
        fact_assignment
        .withColumn("is_exposed", (F.rand(args.seed + 3) < F.lit(float(args.exposure_rate))).cast("boolean"))
        .filter(F.col("is_exposed") == F.lit(True))
        .drop("is_exposed")
        # Exposure occurs after assignment, up to +6 hours
        .withColumn(
            "exposure_time_utc",
            F.expr(f"timestampadd(SECOND, cast(rand({args.seed + 4}) * 21600 as int), assignment_time_utc)")
        )
        .withColumn("exposure_event_type", F.lit("feature_rendered"))
        .withColumn("dt", F.lit(dt_str))
        .select(
            "experiment_id",
            "user_id",
            "variant_id",
            "exposure_time_utc",
            "exposure_event_type",
            "dt",
        )
    )

    # -----------------------------
    # 5) Events (behavior + conversions + revenue)
    # -----------------------------
    # We generate events primarily for exposed users (common real-world pattern).
    # Outcomes are measured after exposure. Treatment gets a lift in conversion probability.
    base = (
        fact_exposure
        .select("experiment_id", "user_id", "variant_id", "exposure_time_utc", "dt")
        .withColumn("analysis_start_time_utc", F.col("exposure_time_utc"))
    )

    # Conversion probability: base * (1 + lift) for treatment
    base = base.withColumn(
        "p_convert",
        F.when(
            F.col("variant_id") == F.lit("treatment"),
            F.lit(float(args.conversion_rate) * (1.0 + float(args.treatment_lift)))
        ).otherwise(F.lit(float(args.conversion_rate)))
    )

    # One conversion per user max (for clarity)
    base = base.withColumn(
        "did_convert",
        (F.rand(args.seed + 5) < F.col("p_convert")).cast("boolean")
    )

    # Create some generic engagement events (e.g., click/page_view)
    # 1..5 events per exposed user
    engagement = (
        base
        .withColumn("n_events", (F.floor(F.rand(args.seed + 6) * 5) + F.lit(1)).cast("int"))
        .select("experiment_id", "user_id", "variant_id", "analysis_start_time_utc", "n_events", "dt")
        .withColumn("seq", F.explode(F.sequence(F.lit(1), F.col("n_events"))))
        .withColumn(
            "event_time_utc",
            F.expr(f"timestampadd(SECOND, cast(rand({args.seed + 7}) * 14400 as int), analysis_start_time_utc)")
        )
        .withColumn(
            "event_type",
            F.when(F.col("seq") % 2 == 0, F.lit("page_view")).otherwise(F.lit("click"))
        )
        .withColumn("revenue", F.lit(None).cast("double"))
        .drop("n_events", "seq")
    )

    # Conversion events (optional, only if did_convert)
    conversions = (
        base
        .filter(F.col("did_convert") == F.lit(True))
        .withColumn(
            "event_time_utc",
            F.expr(f"timestampadd(SECOND, cast(rand({args.seed + 8}) * 14400 as int), analysis_start_time_utc)")
        )
        .withColumn("event_type", F.lit("purchase"))
        # revenue: lognormal-ish approximation via exp(randn)
        .withColumn("revenue", F.round(F.exp(F.randn(args.seed + 9)) * F.lit(30.0), 2))
        .select("experiment_id", "user_id", "variant_id", "event_time_utc", "event_type", "revenue", "dt")
    )

    fact_event = (
        engagement
        .unionByName(conversions, allowMissingColumns=True)
        .withColumn("event_id", F.expr("uuid()"))
        # ingest_time slightly after event_time
        .withColumn(
            "ingest_time_utc",
            F.expr(f"timestampadd(SECOND, cast(rand({args.seed + 10}) * 600 as int), event_time_utc)")
        )
        .select(
            "event_id",
            "experiment_id",
            "user_id",
            "variant_id",
            "event_time_utc",
            "ingest_time_utc",
            "event_type",
            "revenue",
            "dt",
        )
    )

    # -----------------------------
    # 6) Write outputs
    # -----------------------------
    write_parquet(dim_experiment, f"{base_out}/dim_experiment/dt={dt_str}")
    write_parquet(dim_experiment_variant, f"{base_out}/dim_experiment_variant/dt={dt_str}")
    write_parquet(fact_assignment, f"{base_out}/fact_assignment/dt={dt_str}")
    write_parquet(fact_exposure, f"{base_out}/fact_exposure/dt={dt_str}")
    write_parquet(fact_event, f"{base_out}/fact_event/dt={dt_str}")

    print("✅ Generated synthetic experimentation data")
    print(f"dt: {dt_str}")
    print(f"out: {base_out}")
    print(f"users: {args.users}")
    print(f"experiments: {args.experiments}")
    print(f"srm_break: {args.srm_break}")

    spark.stop()


if __name__ == "__main__":
    main()
