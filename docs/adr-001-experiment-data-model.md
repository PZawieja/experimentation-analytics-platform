# ADR-001: Experimentation data model (assignment → exposure → outcomes)

## Status
Accepted

## Context

Product experimentation requires consistent, reproducible measurement.
In practice, experimentation data is often unreliable because:

- assignment is logged inconsistently across services
- exposure is not clearly separated from assignment
- outcomes are counted at the wrong grain (double counting)
- outcomes may occur before exposure due to timestamp or join errors

To enable trustworthy experimentation analysis, the platform needs a canonical model
that separates concerns and enforces a consistent grain for metric computation.

## Decision

We implement a canonical experimentation model with explicit separation between:

- **assignment**: the randomization decision (who is eligible for which variant)
- **exposure**: whether the user actually saw the treatment
- **outcomes**: post-exposure behavior aggregated at the experiment unit grain

The unit of randomization for v1 is **user_id**.

### Canonical tables

1) `dim_experiment`  
Metadata and governance per experiment.

2) `dim_experiment_variant`  
Variant definitions including expected allocation percentages for SRM validation.

3) `fact_assignment` (canonical)  
Exactly one row per `experiment_id × user_id` containing:
- `variant_id`
- `assignment_time_utc`

4) `fact_exposure` (events)  
0..n rows per `experiment_id × user_id` representing exposure events.
Exposures must occur at or after assignment time.

5) `fct_experiment_unit_outcomes` (analysis-ready)  
Exactly one row per `experiment_id × user_id`, containing standardized outcomes
computed in a well-defined window starting at first exposure (preferred) or assignment.

### Grain enforcement

All experiment metrics are computed from `fct_experiment_unit_outcomes`,
which is strictly 1 row per `experiment_id × user_id`.
This prevents double counting and makes aggregations stable.

## Alternatives considered

### Single table combining assignment/exposure/outcomes
Rejected because:
- conflates responsibilities
- makes data quality issues hard to detect
- leads to inconsistent logic across experiments

### Assignment-only analysis without exposure
Rejected because:
- assigned users may never see treatment
- estimates can be biased if exposure differs by variant or segment
- makes it difficult to reason about adoption versus impact

### Account-level unit from the start
Considered, but not chosen for v1 because:
- fewer units makes simulation and validation harder
- additional complexity around multi-user accounts
User-level is implemented first; account-level can be added as an extension.

## Consequences

### Positive
- Clear separation of responsibilities improves correctness and observability
- Standardized metrics become reusable across experiments
- Prevents common errors (leakage, double counting, inconsistent windows)
- Enables guardrails (SRM, exposure validity) to be implemented cleanly

### Negative / trade-offs
- Requires additional modeling steps and explicit joins
- Exposure tracking is not always available in real systems
- Sessionization or identity stitching may be needed in real-world deployments

## Notes

This model intentionally prioritizes correctness and reproducibility.
Downstream dashboards and ad-hoc analyses should use the outcomes table as the source of truth,
and should consult the quality report before interpreting results.
