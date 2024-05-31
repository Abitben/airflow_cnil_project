SELECT 
  etid,
  CAST(REPLACE(etid, 'ETid-', '') AS INT64) as etid_clean,
  country,
  CASE
    WHEN REGEXP_CONTAINS(date_of_decision,'Unknown') THEN NULL
    WHEN REGEXP_CONTAINS(date_of_decision, '^[0-9]{4}$') THEN PARSE_DATE('%Y', date_of_decision)
    WHEN REGEXP_CONTAINS(date_of_decision, '^[0-9]{4}-[0-9]{2}$') THEN PARSE_DATE('%Y-%m', date_of_decision)
    WHEN REGEXP_CONTAINS(date_of_decision, '^[0-9]{4}-[0-9]{2}-[0-9]{4}$') THEN PARSE_DATE('%Y-%m', LEFT(date_of_decision, 7))
    WHEN REGEXP_CONTAINS(date_of_decision, '^[0-9]{4}-[0-9]{2}-[0-9]{3}$') THEN PARSE_DATE('%Y-%m-0%d', date_of_decision)
    ELSE CAST(date_of_decision AS DATE)
  END
  AS date_of_decision,
  fine,
  CASE 
    WHEN REGEXP_CONTAINS(fine, r'^[0-9,]+$') THEN CAST(REPLACE(fine, ",","") AS INT64)
    WHEN fine = 'Fine in three-digit amount' THEN 500
    WHEN fine = 'Fine in four-digit amount' THEN 5000
    WHEN fine = 'Fine in five-digit amount' THEN 50000
    WHEN fine = 'Fine in six-digit amount' THEN 500000
    WHEN fine = 'Fine amount between EUR 50 and EUR 100' THEN 75
    WHEN fine = 'Fine amount between EUR 300 and EUR 400' THEN 350
    WHEN fine = 'Fine amount between EUR 50 and EUR 800' THEN 425
    WHEN fine = 'Fine amount between EUR 400 and EUR 600' THEN 500
    WHEN fine = 'Fine amount between EUR 100 and EUR 1,000' THEN 500
    WHEN fine = 'Fine amount between EUR 200 and EUR 1000' THEN 600
    WHEN fine = 'Fine amount between EUR 350 and EUR 1000' THEN 675
    WHEN fine = 'Only intention to issue fine' THEN NULL
    WHEN fine = 'Unknown' THEN NULL
    ELSE NULL
  END as fine_eur,
  controller_processor,
  quoted_art,
  SPLIT(REGEXP_REPLACE(quoted_art, r',\s*A', r';A'), ";") AS quoted_art_array,
  `type` as reason,
  authority,
  sector,
  summary,
  source.source as source_url,
FROM {{ref('base_staging_gdpr_eu_sanctions')}}