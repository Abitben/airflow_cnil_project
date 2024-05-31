WITH union_all AS(
{{ dbt_utils.union_relations(
    relations=[ref('base_staging_liste_des_controles_realises_par_la_cnil_en_2014'), 
              ref('base_staging_liste_des_controles_realises_par_la_cnil_en_2015'), 
              ref('base_staging_liste_des_controles_realises_par_la_cnil_en_2016'),
              ref('base_staging_liste_des_controles_realises_par_la_cnil_en_2017'), 
              ref('base_staging_opencnil_liste_controles_2018_csv'),
              ref('base_staging_opencnil_liste_controles_2019_csv'),
              ref('base_staging_open_data_controles_2020_vd_20210603_csv'),
              ref('base_staging_open_data_controles_2021_v20220921_csv'),
              ref('base_staging_open_data_controles_cnil_2022_v20231003_csv')
      ],
) }})

SELECT _dbt_source_relation,
    annee,
    IF(type_de_controle = 'RGPD', 'Loi 1978 / RGPD', type_de_controle) as type_de_controle,
    organismes,
    CONCAT('FR','-',departement) as departement_iso,
    {{code_iso_region('departement')}} AS region_iso,
    CASE
        WHEN LOWER(lieu_de_controle) LIKE '%paris%' THEN 'Paris'
        ELSE INITCAP(lieu_de_controle)
    END as ville,
    INITCAP(LOWER(TRIM(lieu_de_controle))) as lieu_de_controle,
    secteur_dactivite as secteur,
    {{clean_sect_controles('secteur_dactivite')}} as secteur_dactivite,
    IFNULL(modalite_de_controle, 'Non renseigné') as modalite_de_controle,
    IFNULL(pays, 'France') as pays
FROM union_all