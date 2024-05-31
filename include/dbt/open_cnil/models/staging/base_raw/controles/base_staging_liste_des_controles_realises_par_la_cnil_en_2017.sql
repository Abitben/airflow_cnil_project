with source as (
      select * from {{ source('raw_data', 'liste_des_controles_realises_par_la_cnil_en_2017') }}
),
renamed as (
    select
        DATE(2017, 01, 01) as annee,
        {{ clean_types_controles(adapter.quote("type_de_controle"))}} as type_de_controle,
        {{ adapter.quote("modalite_de_controle") }},
        {{ clean_organismes(adapter.quote("organismes")) }} as organismes,
        CAST({{ adapter.quote("departement") }} as INT64) as departement,
        {{ adapter.quote("lieu") }} as lieu_de_controle,
        {{ adapter.quote("secteur_dactivite") }},
        'France' as pays
    from source
    LIMIT 1000
    OFFSET 2
)
select * from renamed
  