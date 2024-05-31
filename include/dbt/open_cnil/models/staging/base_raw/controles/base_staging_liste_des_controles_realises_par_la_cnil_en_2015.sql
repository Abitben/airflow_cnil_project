with source as (
      select * from {{ source('raw_data', 'liste_des_controles_realises_par_la_cnil_en_2015') }}
),
renamed as (
    select
        DATE({{ adapter.quote("annee") }}, 01, 01) as annee,
        {{ clean_types_controles(adapter.quote("type_de_controle"))}} as type_de_controle,
        {{ clean_organismes(adapter.quote("organismes")) }} as organismes,
        {{ adapter.quote("lieu_de_controle") }},
        CAST({{ adapter.quote("departement") }} AS INT64) as departement,
        {{ adapter.quote("secteur_dactivite__de_lorganisme") }} as secteur_dactivite,
        {{clean_modalites_controles('type_de_controle')}} as modalite_de_controle,
        'France' as pays 
    from source
)
select * from renamed
  