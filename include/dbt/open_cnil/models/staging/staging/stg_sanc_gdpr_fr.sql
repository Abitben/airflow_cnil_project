WITH OrganismesNumbered AS (
SELECT 
  ROW_NUMBER() OVER(ORDER BY date_date ASC) as id,
  ROW_NUMBER() OVER (PARTITION BY organisme_type, date_date, decision, theme, manquements ORDER BY organisme_type) AS row_num,
  date_date,
  organisme_type,
  LOWER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(TRIM(manquements), r'\s+([A-Z][a-zéèêëàâäôöùûüç])|,', r';\1')
        , ',', ';')
      ) AS manquements,
  decision,
  theme,
  link
FROM 
  {{ ref('base_staging_gdpr_fr_sanctions') }}
ORDER BY 
  date_date ASC),

filter_bad_duplicate as (
SELECT
  id,
  date_date,
  row_num,
  organisme_type,
  manquements,
  SPLIT(manquements, ";") as manquements_array,
  decision,
  SPLIT(decision, 'et') as decision_array,
  theme,
  link
FROM
    OrganismesNumbered
WHERE 
  id NOT IN (9, 26, 39, 67, 68)
)

SELECT
  id,
  date_date,
  CASE
      WHEN COUNT(*) OVER (PARTITION BY organisme_type, date_date, decision, theme, manquements) > 1 THEN organisme_type || '_' || CAST(row_num AS STRING)
      ELSE organisme_type
  END AS organisme_type,
  manquements,
  manquements_array,
  decision,
  decision_array,
  IF(organisme_type LIKE '%simplifiée%', TRUE, FALSE) AS is_simplified,
  theme,
  link
FROM 
  filter_bad_duplicate
ORDER BY 
  date_date ASC
