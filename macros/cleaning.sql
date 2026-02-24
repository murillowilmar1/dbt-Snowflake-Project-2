{% macro clean_id(col, default_value) %}
    coalesce(
        nullif(upper(trim(cast({{ col }} as varchar))), ''),
        '{{ default_value }}'
    )
{% endmacro %}

{% macro cast_user_id(col) %}
    cast({{ col }} as number(38,0))
{% endmacro %}

{% macro cast_amount(col) %}
    cast({{ col }} as number(10,2))
{% endmacro %}

{% macro cast_date(col) %}
    cast({{ col }} as date)
{% endmacro %}

{% macro cast_timestamp(col) %}
    cast({{ col }} as timestamp_ntz)
{% endmacro %}