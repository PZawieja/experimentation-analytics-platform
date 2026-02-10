{{ config(materialized='table') }}

with base as (
    select
        experiment_id
        , metric_id
        , variation_id
        , n_users
        , conversions
        , conversion_rate
    from {{ ref('agg_experiment_metric_by_variant') }}
)

, control as (
    select
        experiment_id
        , metric_id
        , n_users as control_n_users
        , conversions as control_conversions
        , conversion_rate as control_conversion_rate
    from base
    where variation_id = '{{ var("exp_control_variation_id", "control") }}'
)

select
    b.experiment_id
    , b.metric_id
    , b.variation_id
    , b.n_users
    , b.conversions
    , b.conversion_rate

    , c.control_n_users
    , c.control_conversions
    , c.control_conversion_rate

    , (b.conversion_rate - c.control_conversion_rate) as uplift_abs
    , (b.conversion_rate / nullif(c.control_conversion_rate, 0)) - 1 as uplift_rel

    -- pooled proportion z-test
    , (
        (b.conversions + c.control_conversions)
        / nullif(b.n_users + c.control_n_users, 0)
    ) as pooled_p
    , sqrt(
        (
            (
                (b.conversions + c.control_conversions)
                / nullif(b.n_users + c.control_n_users, 0)
            )
            * (1 - (
                (b.conversions + c.control_conversions)
                / nullif(b.n_users + c.control_n_users, 0)
            ))
        )
        * (1 / nullif(b.n_users, 0) + 1 / nullif(c.control_n_users, 0))
    ) as se
    , (b.conversion_rate - c.control_conversion_rate)
        / nullif(
            sqrt(
                (
                    (
                        (b.conversions + c.control_conversions)
                        / nullif(b.n_users + c.control_n_users, 0)
                    )
                    * (1 - (
                        (b.conversions + c.control_conversions)
                        / nullif(b.n_users + c.control_n_users, 0)
                    ))
                )
                * (1 / nullif(b.n_users, 0) + 1 / nullif(c.control_n_users, 0))
            ),
            0
        ) as z

    -- two-sided p-value using normal CDF via erf
    , 2 * (
        1 - (
            0.5 * (
                1 + erf(
                    abs(
                        (b.conversion_rate - c.control_conversion_rate)
                        / nullif(
                            sqrt(
                                (
                                    (
                                        (b.conversions + c.control_conversions)
                                        / nullif(b.n_users + c.control_n_users, 0)
                                    )
                                    * (1 - (
                                        (b.conversions + c.control_conversions)
                                        / nullif(b.n_users + c.control_n_users, 0)
                                    ))
                                )
                                * (1 / nullif(b.n_users, 0) + 1 / nullif(c.control_n_users, 0))
                            ),
                            0
                        )
                    ) / sqrt(2)
                )
            )
        )
    ) as p_value_two_sided

    -- 95% CI using unpooled standard error
    , sqrt(
        (b.conversion_rate * (1 - b.conversion_rate) / nullif(b.n_users, 0))
        + (c.control_conversion_rate * (1 - c.control_conversion_rate) / nullif(c.control_n_users, 0))
    ) as se_unpooled
    , (b.conversion_rate - c.control_conversion_rate)
        - 1.96 * sqrt(
            (b.conversion_rate * (1 - b.conversion_rate) / nullif(b.n_users, 0))
            + (c.control_conversion_rate * (1 - c.control_conversion_rate) / nullif(c.control_n_users, 0))
        ) as ci_low
    , (b.conversion_rate - c.control_conversion_rate)
        + 1.96 * sqrt(
            (b.conversion_rate * (1 - b.conversion_rate) / nullif(b.n_users, 0))
            + (c.control_conversion_rate * (1 - c.control_conversion_rate) / nullif(c.control_n_users, 0))
        ) as ci_high

    , (2 * (
        1 - (
            0.5 * (
                1 + erf(
                    abs(
                        (b.conversion_rate - c.control_conversion_rate)
                        / nullif(
                            sqrt(
                                (
                                    (
                                        (b.conversions + c.control_conversions)
                                        / nullif(b.n_users + c.control_n_users, 0)
                                    )
                                    * (1 - (
                                        (b.conversions + c.control_conversions)
                                        / nullif(b.n_users + c.control_n_users, 0)
                                    ))
                                )
                                * (1 / nullif(b.n_users, 0) + 1 / nullif(c.control_n_users, 0))
                            ),
                            0
                        )
                    ) / sqrt(2)
                )
            )
        )
    )) < 0.05 as is_statistically_significant_95

    , current_timestamp() as computed_at

from base b
left join control c
    on b.experiment_id = c.experiment_id
   and b.metric_id = c.metric_id
