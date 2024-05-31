WITH joined_art AS (
SELECT 
  etid_clean, 
  LEFT(quoted_art, 7) as quoted_art, 
  article_nb,
  article_gdpr
FROM 
  {{ref('stg_sanc_gdpr_eu')}} as list, 
  UNNEST(quoted_art_array) as quoted_art
LEFT JOIN 
  {{ref('base_to_complete_sets_gdpr_list_art_eng')}} as gdpr 
  ON TRIM(LEFT(list.quoted_art, 7)) = gdpr.article_nb)

SELECT 
  etid_clean,
  ARRAY_AGG(article_nb) as article_nb,
  ARRAY_AGG(article_gdpr) as article_gdpr
FROM joined_art
WHERE article_nb IS NOT NULL
GROUP BY etid_clean
