WITH dec_seg AS (
SELECT
  id,
  decision_array,
  CASE
    WHEN LOWER(TRIM(decision)) LIKE '%non-lieu%' OR LOWER(TRIM(decision)) LIKE '%non lieu%' THEN 'Non-lieu'
    WHEN LOWER(TRIM(decision)) LIKE '%sanction%' THEN 'Amende'
    WHEN LOWER(TRIM(decision)) LIKE '%injonction%' AND date_date >= '2023-01-01'  THEN 'Injonction avec astreinte'
    WHEN LOWER(TRIM(decision)) LIKE '%injonction%' AND LOWER(TRIM(decision)) LIKE '%astreinte%'  THEN 'Injonction avec astreinte'
    WHEN LOWER(TRIM(decision)) LIKE '%injonction%' AND LOWER(TRIM(decision)) NOT LIKE '%astreinte%' THEN 'Injonction simple' 
    WHEN LOWER(TRIM(decision)) LIKE '%amende%' THEN 'Amende'
    WHEN LOWER(TRIM(decision)) LIKE '%sanction%' AND LOWER(TRIM(decision)) LIKE '%non publ%'  THEN 'Amende non publique'
    WHEN LOWER(TRIM(decision)) LIKE '%sanction pécuniaire%' THEN 'Amende'
    WHEN LOWER(TRIM(decision)) = 'avertissement non public' THEN 'Avertissement non public'
    WHEN LOWER(TRIM(decision)) = 'avertissement public' OR LOWER(TRIM(decision)) LIKE '%avertissement%' THEN 'Avertissement public'
    WHEN LOWER(TRIM(decision)) = "rappel à l'ordre" THEN "Rappel à l'ordre"
    WHEN LOWER(TRIM(decision)) LIKE '%liquidation%' THEN "Liquidation d'astreinte"
    WHEN LOWER(TRIM(decision)) LIKE '%abandon%' THEN 'Abandon des poursuites'
    WHEN LOWER(TRIM(decision)) LIKE '%non-lieu%' OR LOWER(TRIM(decision)) LIKE '%non lieu%' THEN 'Non-lieu'
    WHEN LOWER(TRIM(decision)) = 'relaxe' THEN 'Relaxe'
    ELSE 'other'
  END AS decision_type,
FROM
  {{ ref('stg_sanc_gdpr_fr') }}, UNNEST(decision_array) as decision)

SELECT
    id,  
    ARRAY_AGG(decision_type) as decision_type_array
  FROM 
    dec_seg
  GROUP BY
    id