{% macro experiment_validation_status(
    has_any_exposure,
    has_multiple_variations_exposed,
    is_pre_assignment_exposure,
    exposure_outside_window,
    is_variant_mismatch,
    has_valid_exposure
) %}
case
    when {{ has_any_exposure }} = false then 'no_exposure'
    when {{ has_multiple_variations_exposed }} = true then 'multi_variation_exposure'
    when {{ is_pre_assignment_exposure }} = true then 'pre_assignment_exposure'
    when {{ exposure_outside_window }} = true then 'exposure_outside_window'
    when {{ is_variant_mismatch }} = true then 'variant_mismatch'
    when {{ has_valid_exposure }} = true then 'valid'
    else 'invalid_other'
end
{% endmacro %}
