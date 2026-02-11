with agg as (

    select *
    from {{ ref('agg_experiment_metric_by_variant') }}

)

, control as (

    select *
    from agg
    where variation_id = '{{ var("exp_control_variation_id") }}'

)

, treatment as (

    select *
    from agg
    where variation_id != '{{ var("exp_control_variation_id") }}'

)

, joined as (

    select
        t.experiment_id

        , c.n_users as n_control
        , c.n_converted as conv_control
        , c.conversion_rate as cr_control

        , t.n_users as n_treatment
        , t.n_converted as conv_treatment
        , t.conversion_rate as cr_treatment

        , (t.conversion_rate - c.conversion_rate) as uplift_abs

    from treatment t
    join control c
        on t.experiment_id = c.experiment_id

)

, stats as (

    select
        *

        -- pooled proportion
        , (conv_control + conv_treatment) * 1.0
          / (n_control + n_treatment) as p_pool

    from joined

)

, final as (

    select
        *
        -- standard error
        , sqrt(
            p_pool * (1 - p_pool)
            * (1.0 / n_control + 1.0 / n_treatment)
          ) as se

        , uplift_abs
          / nullif(
                sqrt(
                    p_pool * (1 - p_pool)
                    * (1.0 / n_control + 1.0 / n_treatment)
                ),
                0
            ) as z_score

        , abs(
            uplift_abs
            / nullif(
                sqrt(
                    p_pool * (1 - p_pool)
                    * (1.0 / n_control + 1.0 / n_treatment)
                ),
                0
            )
          ) as z_abs

        , (1.0 / (1.0 + 0.2316419 * abs(
            uplift_abs
            / nullif(
                sqrt(
                    p_pool * (1 - p_pool)
                    * (1.0 / n_control + 1.0 / n_treatment)
                ),
                0
            )
          ))) as t

    from stats

)

select
    experiment_id

    , n_control
    , cr_control

    , n_treatment
    , cr_treatment

    , uplift_abs

    , z_score

    -- two-sided p-value (normal approx; DuckDB-safe, no erf())
    , 2
      * (exp(-0.5 * z_abs * z_abs) / sqrt(2 * pi()))
      * (t * (
            0.319381530
            + t * (
                -0.356563782
                + t * (
                    1.781477937
                    + t * (
                        -1.821255978
                        + t * 1.330274429
                    )
                )
            )
        )) as p_value_two_sided

    -- 95% CI for absolute uplift
    , uplift_abs - 1.96 * se as ci_low
    , uplift_abs + 1.96 * se as ci_high

from final
