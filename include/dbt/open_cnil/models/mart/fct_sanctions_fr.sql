SELECT 
  sanc.id,
  sanc.date_date,
  sanc.organisme_type,
  sectors.secteur,
  sanc.decision,
  dec_type.decision_type_array,
  amounts.amount,
  gdpr.article_principe,
  gdpr.article_nb,
  gdpr.article_gdpr,
  sanc.is_simplified,
  sanc.link
FROM
  {{ref('stg_sanc_gdpr_fr')}} as sanc
LEFT JOIN {{ref('int_gdpr_fr_art_join')}} as gdpr
  ON sanc.id = gdpr.id
LEFT JOIN {{ref('int_gdpr_fr_decision_type_array')}} as dec_type
  ON sanc.id = dec_type.id
LEFT JOIN {{ref('int_gdpr_fr_sec_join')}} as sectors
  ON sanc.id = sectors.id
LEFT JOIN {{ref('int_gdpr_fr_amount_join')}} as amounts
  ON sanc.id =  amounts.id
ORDER BY
  sanc.date_date DESC
