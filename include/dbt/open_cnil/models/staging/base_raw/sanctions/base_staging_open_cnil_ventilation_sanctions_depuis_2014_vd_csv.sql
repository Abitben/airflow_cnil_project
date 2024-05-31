{% set all_columns = adapter.get_columns_in_relation(source('raw_data', 'open_cnil_ventilation_sanctions_depuis_2014_vd_csv')) %}

with source as (
      select 
        {% for col in all_columns %}
            CAST({{col.name}} AS INT64) as {{col.name}} 
            {% if not loop.last %},
            {% endif %}
        {% endfor %}
      from {{ source('raw_data', 'open_cnil_ventilation_sanctions_depuis_2014_vd_csv') }}
),
renamed as (
    select
        {{ adapter.quote("annee") }},
        {{ adapter.quote("sanctions_pecuniaires_non_publiques") }} as amd_non_pub,
        {{ adapter.quote("sanctions_pecuniaires_publiques") }} as amd_pub,
        {{ adapter.quote("avertissements_non_publics") }} as rappel_a_lordre_non_pub,
        {{ adapter.quote("avertissements_publics") }} as rappel_a_lordre_pub,
        {{ adapter.quote("relaxesnonlieu") }} as relaxes_non_lieu,
        {{ adapter.quote("total") }} as total_proc_non_simplifiee,

    from source
)
select
    annee,
    amd_non_pub, 
    0 as amd_non_pub_avec_inj,
    0 as amd_non_pub_sans_inj,
    amd_pub,
    0 as amd_pub_avec_inj,
    0 as amd_pub_sans_inj,
    0 as inj_non_pub,
    0 as inj_pub,
    rappel_a_lordre_non_pub,
    rappel_a_lordre_pub,
    0 as autres_mesures_non_pub,
    0 as autres_mesures_pub,
    relaxes_non_lieu,
    0 as amd_non_pub_simplifiee,
    0 as amd_non_pub_simplifiee_avec_inj,
    0 as amd_non_pub_simplifiee_sans_inj,
    rappel_a_lordre_non_pub + rappel_a_lordre_pub as autres_mesures_total, 
    0 as inj_total, 
    amd_non_pub + amd_pub as amd_total,
    total_proc_non_simplifiee,
    0 as total_proc_simplifiee,
from renamed