{% macro clean_types_controles(types_c) %}
CASE 
    WHEN LOWER({{types_c}}) = 'vidéo' OR LOWER({{types_c}}) = 'videoprotection' THEN 'Video'
    WHEN LOWER({{types_c}}) = 'loi 78' OR LOWER({{types_c}}) = 'loi 1978' THEN 'Loi 1978 / RGPD'
    WHEN LOWER({{types_c}}) LIKE '%contrôle%' THEN 'Loi 1978 / RGPD'
    WHEN LOWER({{types_c}}) LIKE '%directive%' THEN 'Directive Police/Justice'
    WHEN LOWER({{types_c}}) = 'rgpd' THEN 'RGPD'
ELSE LOWER({{types_c}})
END
{% endmacro %}