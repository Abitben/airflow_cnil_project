SELECT
  *
FROM
  {{ref('base_staging_opencnil_nombre_controles_depuis_1990_maj_juin_2023_csv')}}

UNION ALL

SELECT
  *
FROM
  {{ref('base_to_complete_sets_controles_2023_manual')}}