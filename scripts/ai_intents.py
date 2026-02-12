SQL_TEMPLATES = {
    "did_treatment_win": """
        select
            experiment_id
          , n_control
          , cr_control
          , n_treatment
          , cr_treatment
          , uplift_abs
          , z_score
          , p_value
          , ci_low
          , ci_high
          , case
                when p_value < {alpha}
                 and uplift_abs > 0
                    then true
                else false
            end as did_treatment_win
        from ai_fct_experiment_results
        where experiment_id = '{experiment_id}'
    """
}
