{% macro clean_organismes(organismes) %}
REGEXP_REPLACE(NORMALIZE(TRIM(LOWER({{organismes}})), NFD), r'\pM', '')
{% endmacro %}