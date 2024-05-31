SELECT 
  list.etid_clean as id,
  list.country,
  list.date_of_decision,
  list.fine_eur,
  list.controller_processor,
  quoted_art_array,
  gdpr.article_gdpr,
  reason,
  authority,
  sector,
  summary,
  source_url,
FROM
  {{ref('stg_sanc_gdpr_eu')}} as list
LEFT JOIN {{ref('int_gdpr_eu_join_art')}} as gdpr
  ON list.etid_clean = gdpr.etid_clean