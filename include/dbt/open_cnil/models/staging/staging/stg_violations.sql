SELECT 
  PARSE_DATE('%Y-%m', date_de_reception_de_la_notification) as date_date,
  SPLIT(secteur_dactivite_de_lorganisme_concerne, ";") AS secteur_dactivite_de_lorganisme_concerne,
  SPLIT(natures_de_la_violation, ",") AS natures_de_la_violation,
  nombre_de_personnes_impactees,
  SPLIT(REGEXP_REPLACE(typologie_des_donnees_impactees, r',([[:upper:]])', r'|\1'), '|') AS typologie_des_donnees_impactees,
  CASE
      WHEN donnees_sensibles = 'Oui' THEN TRUE
      WHEN donnees_sensibles IS NULL THEN FALSE
      WHEN donnees_sensibles = 'Données sensibles' THEN TRUE
      ELSE FALSE
    END AS donnees_sensibles,
  SPLIT(REGEXP_REPLACE(origines_de_lincident, r',([[:upper:]])', r'|\1'), '|') AS origines_de_lincident,
  SPLIT(causes_de_lincident, ",") AS causes_de_lincident,
  information_des_personnes
FROM 
  {{ ref('base_raw_data_opencnil_violationsdcpnotifiees_20240331_csv') }}
WHERE 
  date_de_reception_de_la_notification NOT LIKE '%Date de réception de la notification%'