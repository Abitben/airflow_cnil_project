{% macro clean_modalites_controles(types_c) %}
CASE 
    WHEN LOWER({{types_c}}) = 'vidéo' OR LOWER({{types_c}}) = 'videoprotection' THEN 'Sur place/ sur pièces/ sur audition'
    WHEN LOWER({{types_c}}) = 'loi 78' OR LOWER({{types_c}}) = 'loi 1978' THEN 'Sur place/ sur pièces/ sur audition'
    WHEN LOWER({{types_c}}) LIKE '%contrôle%' THEN 'en ligne'
    WHEN LOWER({{types_c}}) LIKE '%directive%' THEN 'Sur place/ sur pièces/ sur audition'
    WHEN LOWER({{types_c}}) = 'rgpd' THEN 'Sur place/ sur pièces/ sur audition'
ELSE 'Non renseigné'
END
{% endmacro %}