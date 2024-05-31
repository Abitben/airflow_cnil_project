SELECT controles.annee,
  controles.type_de_controle,
  controles.organismes,
  grouped_company.grouped_company,
  controles.modalite_de_controle,
  controles.region_iso,
  controles.departement_iso,
  controles.ville,
  controles.pays,
  controles.secteur_dactivite,
FROM {{ref('int_controles_union')}} as controles
LEFT JOIN {{ref('base_to_complete_sets_controles_org_grouped')}} as grouped_company
  ON controles.organismes = grouped_company.organismes