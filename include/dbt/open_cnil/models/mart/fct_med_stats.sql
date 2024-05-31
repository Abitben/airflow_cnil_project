SELECT 
  *
FROM
 {{ref('base_staging_open_cnil_volumes_med_depuis_2014_maj_aout_2023_csv')}}

UNION ALL

SELECT 
  *
FROM
  {{ref('base_to_complete_sets_med_2023_manual')}}
  