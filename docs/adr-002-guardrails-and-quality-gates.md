# ADR-002: Experiment guardrails and quality gates

## Status
Accepted

## Context

Experiment analysis is often invalidated by data issues rather than statistical methods.
Common failure modes include:

- broken randomization (Sample Ratio Mismatch / SRM)
- duplicated assignments
- outcomes recorded before exposure (time leakage)
- metrics computed at incorrect grain (double counting)
- low sample sizes leading to misleading results

A principal goal of this platform is to make invalid experiments easy to detect
and hard to analyze incorrectly.

## Decision

We introduce an explicit experiment quality report that runs as a first-class pipeline artifact.
Analysis and reporting should depend on an `is_valid_for_analysis` flag computed from guardrails.

The canonical output is:

- `rpt_experiment_quality`

This report is produced per experiment per day (or per run) and includes:
- guardrail checks
- pass/fail status
- diagnostic metrics for debugging

### Guardrails (v1)

1) **Assignment uniqueness**
- Rule: exactly one row per `experiment_id × user_id` in `fact_assignment`
- Failure metric: `assignment_duplicates_count`
- Gate: fail if count > 0

2) **Sample Ratio Mismatch (SRM)**
- Rule: observed allocation approximately matches expected allocation from `dim_experiment_variant`
- Metrics:
  - `variant_allocation_deviation_pct`
  - optional `chi_square_p_value`
- Gate: fail if deviation exceeds threshold (default: 2–3%) or p-value < 0.01

3) **Exposure correctness**
- Rule: `exposure_time_utc >= assignment_time_utc`
- Failure metric: `exposure_before_assignment_count`
- Gate: fail if count > 0

4) **Outcome time correctness**
- Rule: outcomes used for metrics must occur after analysis start time
  (preferably first exposure time, otherwise assignment time)
- Failure metric: `outcomes_before_analysis_start_count`
- Gate: fail if count > 0

5) **Metric grain enforcement**
- Rule: exactly one row per `experiment_id × user_id` in `fct_experiment_unit_outcomes`
- Failure metric: `duplicate_outcome_rows_count`
- Gate: fail if count > 0

6) **Minimum sample size (soft gate)**
- Rule: avoid interpretation below minimum N per variant
- Metric: `users_per_variant`
- Gate: flagged (warning), not hard-fail by default

### Output contract

`rpt_experiment_quality` must include:
- `experiment_id`
- `dt`
- `is_valid_for_analysis`
- `failed_guardrails` (string list or encoded string)
- key diagnostic counts and ratios
- optional notes

## Alternatives considered

### No quality gates (trust analysts to be careful)
Rejected because:
- invalid experiments are frequently analyzed in practice
- quality issues are hard to discover downstream
- logic becomes duplicated across teams and dashboards

### Only SRM check
Rejected because:
- SRM is necessary but insufficient
- many critical issues occur even with correct randomization
  (e.g., double counting, leakage, assignment duplicates)

### Always reprocess history to include late data and corrections
Rejected for v1 because:
- increases operational complexity significantly
- makes results less predictable without explicit governance
A future extension can include backfills and correction workflows.

## Consequences

### Positive
- Prevents common causes of invalid experiment conclusions
- Creates an explicit contract: "only analyze valid experiments"
- Improves trust and debuggability
- Centralizes experimentation correctness logic

### Negative / trade-offs
- Adds extra pipeline steps and computation
- Requires clear ownership of quality thresholds
- May block analysis for experiments that would otherwise be explored informally

## Notes

Guardrails are intentionally explicit and conservative.
Thresholds (e.g., SRM deviation) should be tuned based on real production behavior.
The presence of the quality report is as important as the metric outputs themselves.
