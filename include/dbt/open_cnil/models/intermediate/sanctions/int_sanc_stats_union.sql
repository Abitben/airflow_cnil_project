WITH union_all AS(
{{ dbt_utils.union_relations(
    relations=[ref('base_staging_open_cnil_ventilation_sanctions_depuis_2014_vd_csv'), 
              ref('base_staging_opencnil_sanctions_depuis_2019_maj_nov_2023_csv'), 
      ],
) }})

SELECT 
    annee,
    amd_non_pub,
    amd_non_pub_avec_inj,
    amd_non_pub_sans_inj,
    amd_pub,
    amd_pub_avec_inj,
    amd_pub_sans_inj,
    inj_non_pub,
    inj_pub,
    rappel_a_lordre_non_pub,
    rappel_a_lordre_pub,
    autres_mesures_non_pub,
    autres_mesures_pub,
    amd_non_pub_simplifiee,
    amd_non_pub_simplifiee_avec_inj,
    amd_non_pub_simplifiee_sans_inj,
    autres_mesures_total,
    total_proc_non_simplifiee,
    total_proc_simplifiee,
    relaxes_non_lieu,
    amd_total,
FROM union_all
ORDER BY annee
