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
  amd_non_pub_simplifiee as total_amd_non_simplifiee,
  amd_non_pub + amd_pub as total_amd_simplifiee,
  amd_total,
  autres_mesures_total,	
  relaxes_non_lieu,	
  total_proc_non_simplifiee,
  total_proc_simplifiee,
  total_proc_non_simplifiee + total_proc_simplifiee  as total_procedures
FROM
  {{ref('int_sanc_stats_union')}}
ORDER BY
  annee

