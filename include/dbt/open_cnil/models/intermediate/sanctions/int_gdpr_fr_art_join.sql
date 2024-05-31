WITH gdpr_table AS (
    SELECT
        manquement,
        IFNULL(article_principe, 'Non défini') as article_principe,
        IFNULL(article_nb, "'Non défini'") as article_nb,
        IFNULL(article_gdpr,"'Non défini'") as article_gdpr
    FROM
        {{ref('base_to_complete_sets_manquements_gdpr')}}
),

unnest_list AS(
SELECT 
    sanc.id,
    TRIM(manquement) as manquement
FROM 
  {{ref('stg_sanc_gdpr_fr')}} as sanc, 
  UNNEST(sanc.manquements_array) as manquement
WHERE
  manquement != ""
)

SELECT
  sanc.id,
  ARRAY_AGG(DISTINCT gdpr.article_principe) as article_principe,
  ARRAY_AGG(DISTINCT gdpr.article_nb) as article_nb,
  ARRAY_AGG(DISTINCT gdpr.article_gdpr) as article_gdpr
FROM 
  unnest_list as sanc 
LEFT JOIN gdpr_table as gdpr
  ON sanc.manquement = gdpr.manquement
GROUP BY sanc.id
