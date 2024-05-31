SELECT 
  *
FROM
  {{ref('base_staging_opencnil_volumes_plaintes_depuis_1981_maj_juin_2023_csv')}}

UNION ALL

SELECT 
  *
FROM
  {{ref('base_to_complete_sets_plaintes_2023')}}