# J6: Experiment Results Engine

## What J6 adds
J6 produces automated experiment results tables at the metric level. It
standardizes how results are computed, making analysis reproducible and
auditable without bespoke notebooks.

## Conversion definition (default)
Conversion is defined as an event with name `exp_conversion_event_name`
within `exp_conversion_window_days` days after the exposure anchor date
(typically first post-assignment exposure). These defaults are configurable
in `dbt_project.yml`.

## Z-test and confidence interval (v1)
For a binary metric (conversion), J6 uses a two-sample z-test:
- conversion rate per variation: p = conversions / users
- standard error: sqrt( p_c*(1-p_c)/n_c + p_t*(1-p_t)/n_t )
- z = (p_t - p_c) / standard_error
- 95% CI for uplift: (p_t - p_c) ± 1.96 * standard_error

## Assumptions
- Independent units and stable randomization.
- Large-sample approximation is valid for z-test.
- Single post-exposure window anchored at first exposure.

## Known limitations
- Binary metric only (conversion).
- No multiple-testing correction.
- No time-to-event modeling or censoring.
- No covariate adjustment (e.g., CUPED).

## Extending to continuous metrics
- Add per-user metric values to the spine (e.g., revenue_7d).
- Aggregate mean and variance by variation.
- Use a two-sample t-test (Welch) and report CI on mean difference.
