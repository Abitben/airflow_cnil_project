with source as (
      select * from {{ source('raw_data', 'opencnil_sanctions_depuis_2019_maj_nov_2023_csv') }}
),
renamed as (
    select
        {{ adapter.quote("annee") }},
        {{ adapter.quote("amende_administrative_non_publique") }} as amd_non_pub,
        {{ adapter.quote("avec_injonction") }} as amd_non_pub_avec_inj,
        {{ adapter.quote("sans_injonction") }} as amd_non_pub_sans_inj,
        {{ adapter.quote("amende_administrative_publique") }} as amd_pub,
        {{ adapter.quote("avec_injonction_2") }} as amd_pub_avec_inj,
        {{ adapter.quote("sans_injonction_2") }} as amd_pub_sans_inj,
        {{ adapter.quote("injonction_seule_non_publique") }} as inj_non_pub,
        {{ adapter.quote("injonction_seule_publique") }} as inj_pub,
        {{ adapter.quote("rappel_a_lordre_non_public") }} as rappel_a_lordre_non_pub,
        {{ adapter.quote("rappel_a_lordre_public") }} as rappel_a_lordre_pub,
        {{ adapter.quote("autres_mesures_non_publiques_prevues_par_larticle_20_lil") }} as autres_mesures_non_pub,
        {{ adapter.quote("autres_mesures_publiques_prevues_par_larticle_20_lil") }} as autres_mesures_pub,
        {{ adapter.quote("relaxesnonlieu") }} as relaxes_non_lieu,
        {{ adapter.quote("amende_administrative_non_publique_dans_le_cadre_de_la_procedure_simplifiee") }} as amd_non_pub_simplifiee,
        {{ adapter.quote("avec_injonction_3") }} as amd_non_pub_simplifiee_avec_inj,
        {{ adapter.quote("sans_injonction_3") }} as amd_non_pub_simplifiee_sans_inj,
        {{ adapter.quote("total") }} as total_proc_non_simplifiee,
        {{ adapter.quote("total_2") }} as total_proc_simplifiee,


    from source
)
select *,
        rappel_a_lordre_non_pub + rappel_a_lordre_pub + autres_mesures_non_pub + autres_mesures_pub + inj_pub + inj_non_pub as autres_mesures_total,
        amd_non_pub_avec_inj + amd_pub_avec_inj + inj_non_pub + inj_pub + amd_non_pub_simplifiee_avec_inj as inj_total,
        amd_non_pub + amd_pub + amd_non_pub_simplifiee  as amd_total, 
from renamed