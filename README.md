## Experimentation Analytics Platform

Spark-based analytics pipeline for experimentation data with an explicit
assignment -> exposure -> outcomes model. The goal is to make experiments
auditable, guardrail-ready, and analysis-safe by default.

### Core data products
- `data/silver/fact_assignment_canonical`: canonical assignments (J1)
- `data/silver/int_experiment_exposure_validation`: assignment-to-exposure validation (J2)
- `data/gold/fct_experiment_quality_metrics_daily`: daily exposure quality metrics (J2)

### Key analysis modes
- **ITT (intent-to-treat)**: analyze all assigned users, regardless of exposure
- **Exposure-based**: analyze only users with a valid exposure

The platform is designed to support both, and requires analysts to choose
the appropriate mode explicitly.

### Jobs
Generate synthetic data:
`python jobs/00_generate_data.py --dt 2026-02-01`

Build canonical assignments (J1):
`python jobs/10_build_assignments.py --dt 2026-02-01`

Build exposure validation + quality metrics (J2):
`python jobs/20_build_exposure_validation.py --dt 2026-02-01`

### dbt quality tests (portfolio)
A minimal dbt project is included to demonstrate data tests on the
Spark-produced tables. See `dbt/models/experiments/sources.yml`.

## Demo This Repo

### dbt commands
Run the full project:
`dbt deps && dbt run`

Run only experiment models:
`dbt run --select models/experiments/*`

### Core tables to open
- `int_experiment_exposure_validation`
- `fct_experiment_exposure_quality_daily`
- `fct_experiment_cohort`

### Failure modes + detection
- **Broken exposure logging** → exposure_rate drops in quality daily.
- **Pre-assignment exposure** → `pre_assignment_exposure` spikes in validation.
- **Variant mismatch** → `variant_mismatch` spikes in validation.
- **Multi-variation exposure** → `multi_variation_exposure` spikes.
- **Late exposure** → `exposure_outside_window` increases.

### Next steps (J4–J8)
- J4: metric spine (eligibility + attribution windows)
- J5: metric definitions layer (metrics contract)
- J6: experiment results engine (uplift + stats)
- J7: statistical validation (SRM, power, sanity checks)
- J8: guardrail monitoring (safety + automatic gating)
