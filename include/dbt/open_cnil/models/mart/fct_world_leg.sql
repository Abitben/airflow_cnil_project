SELECT
  *,
  CASE 
    WHEN niveau_de_protection = 'Pas de loi ' THEN 1
    WHEN niveau_de_protection = 'Avec législation' THEN 2
    WHEN niveau_de_protection = 'Autorité indépendante et loi(s)' THEN 3
    WHEN niveau_de_protection = 'Pays en adéquation partielle' THEN 4
    WHEN niveau_de_protection = 'Pays adéquat' THEN 5
    WHEN niveau_de_protection = "Pays membre de l'UE ou de l'EEE" THEN 6
    ELSE 0
    END as heat
FROM
  {{ref('base_staging_opencnil_autorites_de_protection_vd_20231010_csv')}}