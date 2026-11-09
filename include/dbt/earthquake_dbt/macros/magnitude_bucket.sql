{% macro magnitude_bucket(magnitude_column) %}
    case
        when {{ magnitude_column }} is null then 'unknown'
        when {{ magnitude_column }} < 2 then 'micro (<2)'
        when {{ magnitude_column }} < 4 then 'minor (2-4)'
        when {{ magnitude_column }} < 6 then 'light-moderate (4-6)'
        when {{ magnitude_column }} < 7 then 'strong (6-7)'
        else 'major (7+)'
    end
{% endmacro %}
