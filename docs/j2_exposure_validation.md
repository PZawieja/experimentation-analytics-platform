# J2: Exposure Validation + Quality Metrics

## Purpose and guarantees
J2 creates the auditable bridge between assignment and exposure. It guarantees:
- assignments remain the canonical grain (one row per experiment_id x unit_id)
- exposures are deduped deterministically
- timing and variant integrity are explicitly validated
- quality metrics expose tracking regressions early

## Definitions
- **Assignment**: the randomization decision for a unit in an experiment.
- **Exposure**: an event indicating the unit saw the experiment treatment.
- **Valid exposure**: a post-assignment exposure that is on time, within the window, and matches the assigned variation.
- **Mismatch**: exposure variation differs from assigned variation.
- **Multi-variation exposure**: a unit is exposed to more than one variation in the same experiment.

## Data flow
```
raw events / exposure logs
        |
        v
stg_experiment_exposures
        |
        v
int_experiment_exposures_deduped
        |
assignments (fct_experiment_assignments)
        |          \
        |           v
        +----> int_experiment_exposure_validation
                        |
                        v
        fct_experiment_exposure_quality_daily
```

## Model descriptions
- `stg_experiment_exposures`: standardizes exposure events from `fct_events` and extracts experiment/variation IDs.
- `int_experiment_exposures_deduped`: deterministic dedupe (earliest event per experiment/unit/variation).
- `int_experiment_exposure_validation`: joins assignments to exposures and computes validation flags per unit, including first exposure any vs post-assignment.
- `fct_experiment_exposure_quality_daily`: daily quality metrics per experiment.

## Validation rules
- **Grace window**: pre-assignment exposures are allowed up to `exp_allow_pre_assignment_exposure_grace_minutes` minutes before assignment for post-assignment matching.
- **Max window**: exposure must occur within `exp_exposure_max_days_after_assignment` days after assignment.
- **Variant match**: exposure variation must equal assigned variation.
- **Single-variation exposure**: units exposed to multiple variations are invalid.

## Debugging: exposure_rate drops
1) Verify the exposure event name (`exp_exposure_event_name`) matches production.
2) Check ingestion lag or missing partitions in `fct_events`.
3) Inspect `int_experiment_exposure_validation` for spikes in:
   - `no_exposure`
   - `pre_assignment_exposure`
   - `exposure_outside_window`
4) Compare exposure counts by experiment in the raw event stream.
5) Validate assignment volumes did not shift (J1 quality metrics).

## Extensions
- **By-variation breakdown**: add `assigned_variation_id` to daily aggregates.
- **Anomaly alerts**: flag exposure_rate or mismatch_rate changes beyond thresholds.
