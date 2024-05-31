WITH sectors AS (
  SELECT
  DISTINCT(organisme_type),
  secteur,
  FROM {{ref('base_to_complete_sets_organismes_secteur')}}
)

SELECT id,
  sec.secteur
FROM {{ ref('stg_sanc_gdpr_fr') }} as list
LEFT JOIN sectors AS sec
ON list.organisme_type = sec.organisme_type