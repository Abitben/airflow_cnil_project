WITH code_naf_rebase AS (
  SELECT
    *,
    CONCAT(LEFT(code_naf_organisme_designant, 2), ".", RIGHT(code_naf_organisme_designant, 3)) as code_naf_rebase_designant,
    CONCAT(LEFT(code_naf_organisme_designe, 2), ".", RIGHT(code_naf_organisme_designe, 3)) as code_naf_rebase_designe,
    IF(nom_organisme_designe IS NULL AND siren_organisme_designe IS NULL, 'DPO interne', nom_organisme_designe) as nom_organisme_designe_clean,
  FROM
  {{ref('stg_dpo')}}
)
SELECT 
  dpo.siren_organisme_designant,
  dpo.nom_organisme_designant,
  sec_designant.secteur as secteur_designant,
  dpo.code_naf_rebase_designant,
  naf_designant.`intitulé` as naf_intitule_designant,
  dpo.adresse_postale_organisme_designant,
  dpo.num_dep,
  dpo.code_iso_dep_designant,
  dpo.zipcode_designant,
  dpo.ville_organisme_designant,
  dpo.pays_organisme_designant,
  dpo.type_de_dpo,
  IF(nom_organisme_designe IS NULL AND siren_organisme_designe IS NULL, 'DPO interne', 'DPO externe') as interne_externe,
  dpo.date_de_la_designation,
  dpo.siren_organisme_designe,
  nom_organisme_designe_clean as nom_organisme_designe,
  dpo.code_naf_organisme_designe,
  naf_designe.`intitulé` as naf_intitule_designe,
  dpo.adresse_postale_organisme_designe,
  dpo.code_postal_organisme_designe,
  dpo.pays_organisme_designe,
  dpo.code_iso_region_designe,
  dpo.code_iso_dep_designe,
  dpo.zipcode_designant_designe,
  dpo.ville_organisme_designe,
  dpo.moyen_contact_dpo_email,
  dpo.moyen_contact_dpo_telephone,
  dpo.moyen_contact_dpo_url,
  dpo.moyen_contact_dpo_adresse_postale,
  dpo.moyen_contact_dpo_code_postal,
  dpo.moyen_contact_dpo_ville,
  dpo.moyen_contact_dpo_pays,
  dpo.code_iso_region_designant,
  sec_designe.secteur as secteur_designe,
  COUNT(DISTINCT dpo.nom_organisme_designant) OVER(PARTITION BY dpo.nom_organisme_designe) as nb_designation,
FROM code_naf_rebase as dpo
LEFT JOIN cnil-392113.to_complete_sets.code_naf as naf_designant
  ON naf_designant.code = dpo.code_naf_rebase_designant
LEFT JOIN cnil-392113.to_complete_sets.code_naf as naf_designe
  ON naf_designe.code = dpo.code_naf_rebase_designe
LEFT JOIN cnil-392113.to_complete_sets.code_secteur as sec_designant
  ON sec_designant.code = dpo.secteur_activite_organisme_designant
LEFT JOIN cnil-392113.to_complete_sets.code_secteur as sec_designe
  ON sec_designe.code = dpo.secteur_activite_organisme_designe